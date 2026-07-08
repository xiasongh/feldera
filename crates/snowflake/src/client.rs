use crate::arrow::{decode_base64_ipc_stream, normalize_snowflake_batch};
use anyhow::{anyhow, bail, Context, Result as AnyResult};
use arrow::{buffer::Buffer, ipc::reader::StreamDecoder, record_batch::RecordBatch};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::try_stream;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bytes::BytesMut;
use feldera_types::transport::snowflake::{SnowflakeAuthenticator, SnowflakeReaderConfig};
use futures_util::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use openssl::{
    pkey::{PKey, Private},
    sha::sha256,
};
use reqwest::{
    header::{
        HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_ENCODING, USER_AGENT,
    },
    Client, StatusCode, Url,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{
    collections::HashMap,
    env, fs, io,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader as AsyncStreamReader;
use uuid::Uuid;

// Match Snowflake's C/C++ connector, which defines its client id as "C API":
// https://github.com/snowflakedb/libsnowflakeclient/blob/master/include/snowflake/client.h
const CLIENT_APP_ID: &str = "C API";
const CLIENT_APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const JWT_LIFETIME_SECONDS: u64 = 60;
const QUERY_RESULT_FORMAT: &str = "ARROW_FORCE";
const QUERY_IN_PROGRESS_CODE: &str = "333333";
const QUERY_IN_PROGRESS_ASYNC_CODE: &str = "333334";
const CHUNK_DOWNLOAD_MAX_ATTEMPTS: u32 = 6;
const ARROW_DECODE_BUFFER_SIZE: usize = 256 * 1024;

pub(crate) struct SnowflakeClient {
    http: Client,
    base_url: Url,
    session_token: String,
    sequence_id: AtomicU64,
}

pub(crate) struct SnowflakeArrowQueryMetadata {
    pub(crate) query_id: Option<String>,
    pub(crate) total_rows: Option<u64>,
}

pub(crate) struct SnowflakeArrowBatchStream<'a> {
    pub(crate) metadata: SnowflakeArrowQueryMetadata,
    pub(crate) batches: BoxStream<'a, AnyResult<RecordBatch>>,
}

#[derive(Deserialize)]
struct SnowflakeApiResponse<T> {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    data: Option<T>,
}

#[derive(Deserialize)]
struct LoginData {
    #[serde(default)]
    token: Option<String>,
    #[serde(default, rename = "sessionToken")]
    session_token: Option<String>,
}

#[derive(Deserialize)]
struct QueryData {
    #[serde(default, rename = "queryId")]
    query_id: Option<String>,
    #[serde(default, rename = "queryResultFormat")]
    query_result_format: Option<String>,
    #[serde(default, rename = "rowsetBase64")]
    rowset_base64: Option<String>,
    #[serde(default)]
    chunks: Vec<SnowflakeChunk>,
    #[serde(default, rename = "chunkHeaders")]
    chunk_headers: HashMap<String, String>,
    #[serde(default)]
    qrmk: Option<String>,
    #[serde(default)]
    total: Option<u64>,
    #[serde(default, rename = "getResultUrl")]
    get_result_url: Option<String>,
}

#[derive(Deserialize)]
struct SnowflakeChunk {
    url: String,
}

#[derive(Debug, Serialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

impl SnowflakeClient {
    pub(crate) async fn from_reader_config(config: &SnowflakeReaderConfig) -> AnyResult<Self> {
        config.validate().map_err(|error| anyhow!(error))?;
        let http = Client::builder()
            .user_agent(user_agent())
            .connect_timeout(Duration::from_secs(30))
            .no_gzip()
            .build()
            .context("error building Snowflake HTTP client")?;
        let base_url = account_base_url(&config.account)?;
        let jwt = jwt_token(config)?;

        let login_url = login_url(&base_url, config)?;
        let body = login_body(config, &jwt);
        let response = http
            .post(login_url)
            .header(ACCEPT, "application/snowflake")
            .json(&body)
            .send()
            .await
            .context("error sending Snowflake login request")?
            .error_for_status()
            .context("Snowflake login request failed")?
            .json::<SnowflakeApiResponse<LoginData>>()
            .await
            .context("error parsing Snowflake login response")?;

        let data = response_data(response, "Snowflake login")?;

        let client = Self {
            http,
            base_url,
            session_token: data.token.or(data.session_token).ok_or_else(|| {
                anyhow!("Snowflake login response did not include a session token")
            })?,
            sequence_id: AtomicU64::new(0),
        };

        // Snowflake's C/C++ connector enables Arrow with this session parameter:
        // https://github.com/snowflakedb/libsnowflakeclient/blob/master/cpp/lib/ArrowChunkIterator.hpp
        client
            .execute_statement(&format!(
                "alter session set C_API_QUERY_RESULT_FORMAT={QUERY_RESULT_FORMAT}"
            ))
            .await
            .context("error enabling Snowflake Arrow result format")?;

        Ok(client)
    }

    pub(crate) async fn query_arrow_batch_stream(
        &self,
        sql: &str,
        max_concurrent_readers: usize,
    ) -> AnyResult<SnowflakeArrowBatchStream<'_>> {
        let data = self.execute_statement(sql).await?;
        let format = data.query_result_format.as_deref().unwrap_or_default();
        if !format.eq_ignore_ascii_case("arrow") && !format.eq_ignore_ascii_case("arrow_force") {
            bail!("Snowflake returned queryResultFormat='{format}', expected Arrow");
        }

        let inline_batches = data
            .rowset_base64
            .as_deref()
            .map(decode_base64_ipc_stream)
            .transpose()?;
        let chunk_headers = chunk_headers(&data)?;
        let metadata = SnowflakeArrowQueryMetadata {
            query_id: data.query_id,
            total_rows: data.total,
        };

        let inline_stream = stream::iter(
            inline_batches
                .into_iter()
                .flatten()
                .map(Ok::<_, anyhow::Error>),
        );
        let downloaded_batches = stream::iter(
            data.chunks
                .into_iter()
                .map(move |chunk| self.stream_chunk(chunk, chunk_headers.clone())),
        )
        .flatten_unordered(max_concurrent_readers.max(1));

        Ok(SnowflakeArrowBatchStream {
            metadata,
            batches: inline_stream.chain(downloaded_batches).boxed(),
        })
    }

    fn stream_chunk<'a>(
        &'a self,
        chunk: SnowflakeChunk,
        headers: HeaderMap,
    ) -> BoxStream<'a, AnyResult<RecordBatch>> {
        try_stream! {
            let chunk_url = redact_url(&chunk.url);
            let response = self
                .open_chunk_response(&chunk.url, &headers)
                .await
                .with_context(|| format!("error opening Snowflake result chunk {chunk_url}"))?;
            let gzip_header = response_is_gzip_encoded(&response, &chunk.url);
            let body_url = chunk_url.clone();
            let body = response.bytes_stream().map_err(move |error| {
                io::Error::other(format!(
                    "error reading Snowflake result chunk {body_url}: {error}"
                ))
            });
            let mut body = BufReader::new(AsyncStreamReader::new(body));
            let gzip_magic = body
                .fill_buf()
                .await
                .with_context(|| format!("error reading Snowflake result chunk {chunk_url}"))?
                .starts_with(&[0x1f, 0x8b]);

            let reader: Pin<Box<dyn AsyncRead + Send>> = if gzip_header || gzip_magic {
                Box::pin(GzipDecoder::new(body))
            } else {
                Box::pin(body)
            };
            let mut batches = decode_arrow_reader(reader, chunk_url);
            while let Some(batch) = batches.next().await {
                yield batch?;
            }
        }
        .boxed()
    }

    async fn execute_statement(&self, sql: &str) -> AnyResult<QueryData> {
        let sql = sql.trim();
        if sql.is_empty() {
            bail!("Snowflake query must not be empty");
        }

        let mut url = self.endpoint("/queries/v1/query-request")?;
        url.query_pairs_mut()
            .append_pair("requestId", &Uuid::new_v4().to_string());

        let body = json!({
            "sqlText": sql,
            "asyncExec": false,
            "sequenceId": self.sequence_id.fetch_add(1, Ordering::Relaxed) + 1,
            "querySubmissionTime": epoch_millis(),
        });

        let mut response = self
            .http
            .post(url)
            .headers(self.session_headers()?)
            .json(&body)
            .send()
            .await
            .context("error sending Snowflake query request")?
            .error_for_status()
            .context("Snowflake query request failed")?
            .json::<SnowflakeApiResponse<QueryData>>()
            .await
            .context("error parsing Snowflake query response")?;

        while response.code.as_deref().is_some_and(|code| {
            code == QUERY_IN_PROGRESS_CODE || code == QUERY_IN_PROGRESS_ASYNC_CODE
        }) {
            let result_url = response
                .data
                .as_ref()
                .and_then(|data| data.get_result_url.as_deref())
                .ok_or_else(|| {
                    anyhow!("Snowflake query-in-progress response did not include getResultUrl")
                })?;
            let result_url = self
                .base_url
                .join(result_url.trim_start_matches('/'))
                .with_context(|| format!("invalid Snowflake query result URL '{result_url}'"))?;
            response = self
                .http
                .get(result_url)
                .headers(self.session_headers()?)
                .send()
                .await
                .context("error polling Snowflake query result")?
                .error_for_status()
                .context("Snowflake query result poll failed")?
                .json::<SnowflakeApiResponse<QueryData>>()
                .await
                .context("error parsing Snowflake query result response")?;
        }

        response_data(response, "Snowflake query")
    }

    async fn open_chunk_response(
        &self,
        url: &str,
        headers: &HeaderMap,
    ) -> AnyResult<reqwest::Response> {
        for attempt in 1..=CHUNK_DOWNLOAD_MAX_ATTEMPTS {
            let response = match self.http.get(url).headers(headers.clone()).send().await {
                Ok(response) => response,
                Err(_) if attempt < CHUNK_DOWNLOAD_MAX_ATTEMPTS => {
                    tokio::time::sleep(chunk_retry_backoff(attempt)).await;
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("error sending Snowflake chunk request, attempt {attempt}")
                    });
                }
            };

            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }

            if !is_retryable_chunk_status(status) || attempt == CHUNK_DOWNLOAD_MAX_ATTEMPTS {
                response.error_for_status().with_context(|| {
                    format!("Snowflake chunk request failed with status {status}")
                })?;
                unreachable!("error_for_status returned Ok for unsuccessful status {status}");
            }

            tokio::time::sleep(chunk_retry_backoff(attempt)).await;
        }

        unreachable!("chunk retry loop should return or error");
    }

    fn endpoint(&self, path: &str) -> AnyResult<Url> {
        self.base_url
            .join(path.trim_start_matches('/'))
            .with_context(|| format!("invalid Snowflake endpoint path: {path}"))
    }

    fn session_headers(&self) -> AnyResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/snowflake"));
        headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent())?);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Snowflake Token=\"{}\"", self.session_token))
                .context("invalid Snowflake session token header")?,
        );
        Ok(headers)
    }
}

/// Incrementally decode Arrow IPC batches from an asynchronous byte source.
fn decode_arrow_reader<R>(
    mut reader: R,
    source: String,
) -> BoxStream<'static, AnyResult<RecordBatch>>
where
    R: AsyncRead + Send + Unpin + 'static,
{
    try_stream! {
        let mut decoder = StreamDecoder::new();

        loop {
            let mut bytes = BytesMut::with_capacity(ARROW_DECODE_BUFFER_SIZE);
            let count = reader
                .read_buf(&mut bytes)
                .await
                .with_context(|| format!("error reading Arrow IPC data from {source}"))?;
            if count == 0 {
                break;
            }

            let mut buffer = Buffer::from(bytes.freeze());
            while !buffer.is_empty() {
                if let Some(batch) = decoder
                    .decode(&mut buffer)
                    .with_context(|| format!("error decoding Arrow IPC data from {source}"))?
                {
                    yield normalize_snowflake_batch(batch).with_context(|| {
                        format!("error normalizing Arrow data from {source}")
                    })?;
                }
            }
        }

        decoder
            .finish()
            .with_context(|| format!("incomplete Arrow IPC stream from {source}"))?;
    }
    .boxed()
}

fn chunk_retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(250 * 2_u64.pow(attempt - 1))
}

fn account_base_url(account: &str) -> AnyResult<Url> {
    let account = account.trim();
    validate_account_identifier(account)?;
    let top_level_domain = if account
        .split('.')
        .nth(1)
        .is_some_and(|region| region.to_ascii_lowercase().starts_with("cn-"))
    {
        "cn"
    } else {
        "com"
    };
    let url = format!("https://{account}.snowflakecomputing.{top_level_domain}");
    Url::parse(&url)
        .with_context(|| format!("invalid Snowflake account URL derived from '{account}'"))
}

fn validate_account_identifier(account: &str) -> AnyResult<()> {
    if account.is_empty()
        || account
            .to_ascii_lowercase()
            .contains(".snowflakecomputing.")
        || account.split('.').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        bail!(
            "invalid Snowflake account identifier '{account}'; expected dot-separated letters, digits, underscores, or hyphens"
        );
    }
    Ok(())
}

fn login_account_name(account: &str) -> &str {
    if account.to_ascii_lowercase().contains(".global") {
        let account = account.split('.').next().unwrap_or(account);
        account
            .rsplit_once('-')
            .map_or(account, |(account, _)| account)
    } else {
        account.split('.').next().unwrap_or(account)
    }
}

fn login_url(base_url: &Url, config: &SnowflakeReaderConfig) -> AnyResult<Url> {
    let mut url = base_url.join("session/v1/login-request")?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("request_id", &Uuid::new_v4().to_string());
        if let Some(database) = nonempty(config.database.as_deref()) {
            query.append_pair("databaseName", database);
        }
        if let Some(schema) = nonempty(config.schema.as_deref()) {
            query.append_pair("schemaName", schema);
        }
        if let Some(warehouse) = nonempty(config.warehouse.as_deref()) {
            query.append_pair("warehouse", warehouse);
        }
        if let Some(role) = nonempty(config.role.as_deref()) {
            query.append_pair("roleName", role);
        }
    }
    Ok(url)
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then_some(value)
    })
}

fn login_body(config: &SnowflakeReaderConfig, jwt: &str) -> JsonValue {
    let mut session_parameters = serde_json::Map::new();
    session_parameters.insert(
        "AUTOCOMMIT".to_string(),
        JsonValue::String("true".to_string()),
    );
    session_parameters.insert("TIMEZONE".to_string(), JsonValue::String("UTC".to_string()));

    json!({
        "data": {
            "CLIENT_APP_ID": CLIENT_APP_ID,
            "CLIENT_APP_VERSION": CLIENT_APP_VERSION,
            "ACCOUNT_NAME": login_account_name(&config.account),
            "LOGIN_NAME": config.user,
            "AUTHENTICATOR": "SNOWFLAKE_JWT",
            "TOKEN": jwt,
            "CLIENT_ENVIRONMENT": {
                "APPLICATION": CLIENT_APP_ID,
                "OS": env::consts::OS,
                "ARCH": env::consts::ARCH,
            },
            "SESSION_PARAMETERS": session_parameters,
        }
    })
}

fn jwt_token(config: &SnowflakeReaderConfig) -> AnyResult<String> {
    match config.authenticator {
        SnowflakeAuthenticator::SnowflakeJwt => {}
    }

    let pem = fs::read(&config.private_key_file).with_context(|| {
        format!(
            "error reading Snowflake private key {}",
            config.private_key_file
        )
    })?;
    let key = parse_private_key(&pem, config.private_key_file_pwd.as_deref())?;
    let public_key_fingerprint = public_key_fingerprint(&key)?;

    let account = login_account_name(&config.account).to_ascii_uppercase();
    let user = config.user.to_ascii_uppercase();
    let subject = format!("{account}.{user}");
    let now = epoch_seconds();
    let claims = JwtClaims {
        iss: format!("{subject}.SHA256:{public_key_fingerprint}"),
        sub: subject,
        iat: now,
        exp: now + JWT_LIFETIME_SECONDS,
    };
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("JWT".to_string());
    let signing_key = key
        .private_key_to_pem_pkcs8()
        .context("error serializing Snowflake RSA private key")?;
    let signing_key = EncodingKey::from_rsa_pem(&signing_key)
        .context("error parsing Snowflake RSA private key")?;
    encode(&header, &claims, &signing_key).context("error signing Snowflake JWT")
}

fn parse_private_key(pem: &[u8], passphrase: Option<&str>) -> AnyResult<PKey<Private>> {
    PKey::private_key_from_pem_passphrase(pem, passphrase.unwrap_or_default().as_bytes()).context(
        "error parsing Snowflake RSA private key; provide 'private_key_file_pwd' if the key is encrypted",
    )
}

fn public_key_fingerprint(key: &PKey<Private>) -> AnyResult<String> {
    let public_key_der = key
        .public_key_to_der()
        .context("error extracting Snowflake public key DER")?;
    Ok(BASE64_STANDARD.encode(sha256(&public_key_der)))
}

fn response_data<T>(response: SnowflakeApiResponse<T>, operation: &str) -> AnyResult<T> {
    if response.success == Some(false) {
        bail!(
            "{operation} failed: code={} message={}",
            response.code.unwrap_or_default(),
            response.message.unwrap_or_default()
        );
    }

    response.data.ok_or_else(|| {
        anyhow!(
            "{operation} response did not include a data object: code={} message={}",
            response.code.unwrap_or_default(),
            response.message.unwrap_or_default()
        )
    })
}

fn chunk_headers(data: &QueryData) -> AnyResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in &data.chunk_headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("invalid Snowflake chunk header name: {name}"))?;
        let value = HeaderValue::from_str(value)
            .with_context(|| format!("invalid Snowflake chunk header value for {name}"))?;
        headers.insert(name, value);
    }

    if headers.is_empty() {
        if let Some(qrmk) = &data.qrmk {
            headers.insert(
                HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm"),
                HeaderValue::from_static("AES256"),
            );
            headers.insert(
                HeaderName::from_static("x-amz-server-side-encryption-customer-key"),
                HeaderValue::from_str(qrmk).context("invalid Snowflake qrmk chunk header")?,
            );
        }
    }

    Ok(headers)
}

fn response_is_gzip_encoded(response: &reqwest::Response, url: &str) -> bool {
    response
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("gzip"))
        || url.contains("response-content-encoding=gzip")
}

fn is_retryable_chunk_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::GATEWAY_TIMEOUT
}

fn redact_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => "<invalid-url>".to_string(),
    }
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn user_agent() -> String {
    format!("{CLIENT_APP_ID}/{CLIENT_APP_VERSION}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
    };
    use flate2::{write::GzEncoder, Compression};
    use openssl::{rsa::Rsa, symm::Cipher};
    use std::{io::Write, sync::Arc};
    use tokio::{
        io::{duplex, AsyncWriteExt},
        sync::oneshot,
        time::timeout,
    };

    #[test]
    fn derives_account_urls() {
        assert_eq!(
            account_base_url("org-account").unwrap().as_str(),
            "https://org-account.snowflakecomputing.com/"
        );
        assert_eq!(
            account_base_url("xy12345.us-east-1").unwrap().as_str(),
            "https://xy12345.us-east-1.snowflakecomputing.com/"
        );
        assert!(account_base_url("http://localhost:8080").is_err());
        assert!(account_base_url("xy12345.snowflakecomputing.com").is_err());
    }

    #[test]
    fn normalizes_account_name_for_login_and_jwt() {
        assert_eq!(login_account_name("org-account"), "org-account");
        assert_eq!(login_account_name("xy12345.us-east-1"), "xy12345");
        assert_eq!(login_account_name("xy12345-external.global"), "xy12345");
    }

    #[test]
    fn parses_encrypted_private_key() {
        let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let encrypted = key
            .private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), b"secret")
            .unwrap();

        assert!(parse_private_key(&encrypted, None).is_err());
        let decrypted = parse_private_key(&encrypted, Some("secret")).unwrap();
        assert_eq!(
            public_key_fingerprint(&decrypted).unwrap(),
            public_key_fingerprint(&key).unwrap()
        );
    }

    #[tokio::test]
    async fn streams_arrow_batch_before_gzip_eof() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let second =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3, 4]))])
                .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            writer.write(&first).unwrap();
            writer.write(&second).unwrap();
            writer.finish().unwrap();
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&ipc).unwrap();
        let gzip = encoder.finish().unwrap();
        let trailer_start = gzip.len() - 8;
        let (mut writer, reader) = duplex(gzip.len());
        let (release_sender, release_receiver) = oneshot::channel();
        tokio::spawn(async move {
            writer.write_all(&gzip[..trailer_start]).await.unwrap();
            release_receiver.await.unwrap();
            writer.write_all(&gzip[trailer_start..]).await.unwrap();
        });

        let reader = GzipDecoder::new(BufReader::new(reader));
        let mut batches = decode_arrow_reader(reader, "test stream".to_string());
        let first = timeout(Duration::from_secs(2), batches.next())
            .await
            .expect("first batch was not decoded before gzip EOF")
            .unwrap()
            .unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(
            first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );

        release_sender.send(()).unwrap();
        let second = batches.next().await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 2);
        assert_eq!(
            second
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[3, 4]
        );
        assert!(batches.next().await.is_none());
    }

    #[test]
    fn redacts_presigned_chunk_url() {
        assert_eq!(
            redact_url("https://example.s3.amazonaws.com/chunk?X-Amz-Signature=secret#frag"),
            "https://example.s3.amazonaws.com/chunk"
        );
        assert_eq!(redact_url("not a url"), "<invalid-url>");
    }

    #[ignore]
    #[tokio::test]
    async fn snowflake_arrow_smoke_test() -> AnyResult<()> {
        let config = SnowflakeReaderConfig {
            account: env::var("SNOWFLAKE_ACCOUNT").context("SNOWFLAKE_ACCOUNT is not set")?,
            user: env::var("SNOWFLAKE_USER").context("SNOWFLAKE_USER is not set")?,
            authenticator: SnowflakeAuthenticator::SnowflakeJwt,
            role: env::var("SNOWFLAKE_ROLE").ok(),
            warehouse: env::var("SNOWFLAKE_WAREHOUSE").ok(),
            database: env::var("SNOWFLAKE_DATABASE").ok(),
            schema: env::var("SNOWFLAKE_SCHEMA").ok(),
            private_key_file: env::var("SNOWFLAKE_PRIVATE_KEY_FILE")
                .context("SNOWFLAKE_PRIVATE_KEY_FILE is not set")?,
            private_key_file_pwd: env::var("SNOWFLAKE_PRIVATE_KEY_FILE_PWD").ok(),
            table: "unused".to_string(),
            mode: Default::default(),
            transaction_mode: Default::default(),
            snapshot_filter: None,
            skip_unused_columns: false,
            num_parsers: 4,
            max_concurrent_readers: None,
        };
        let client = SnowflakeClient::from_reader_config(&config).await?;
        let query =
            env::var("SNOWFLAKE_TEST_QUERY").unwrap_or_else(|_| "select 1::int as ID".to_string());
        let result = client.query_arrow_batch_stream(&query, 1).await?;
        let batches = result.batches.collect::<Vec<_>>().await;
        let batches = batches.into_iter().collect::<AnyResult<Vec<_>>>()?;
        let rows = batches
            .into_iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 1);
        Ok(())
    }
}
