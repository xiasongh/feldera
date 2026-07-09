#!/usr/bin/env bash

# Verify that an older pipeline manager can forward a transport implemented by a
# newer custom runtime.
#
# 1. Create a runtime-only Datagen alias in a local clone.
# 2. Start the unmodified pipeline manager.
# 3. Compile a pipeline against the cloned runtime revision.
# 4. Verify that the pipeline processes data successfully.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

SQL_COMPILER_PATH="sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar"
API_PORT="${FELDERA_TEST_API_PORT:-18080}"
COMPILER_PORT="${FELDERA_TEST_COMPILER_PORT:-18085}"
RUNNER_PORT="${FELDERA_TEST_RUNNER_PORT:-18089}"
TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/feldera-custom-runtime-test.XXXXXX")"
COMPILER_DIR="${TEST_DIR}/compiler"
RUNTIME_REPO="${COMPILER_DIR}/feldera-checkout"
MANAGER_LOG="${TEST_DIR}/pipeline-manager.log"
MANAGER_PID=""

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [[ -n "${MANAGER_PID}" ]]; then
        kill "${MANAGER_PID}" 2>/dev/null || true
        wait "${MANAGER_PID}" 2>/dev/null || true
    fi
    if [[ ${status} -ne 0 ]]; then
        tail -n 200 "${MANAGER_LOG}" 2>/dev/null || true
    fi
    rm -rf "${TEST_DIR}"
    exit "${status}"
}
trap cleanup EXIT INT TERM

if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home ]]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="${JAVA_HOME}/bin:${PATH}"
fi
if ! java -version >/dev/null 2>&1; then
    echo "A working Java installation is required to run the SQL compiler." >&2
    exit 1
fi

if [[ ! -f "${SQL_COMPILER_PATH}" ]]; then
    (cd sql-to-dbsp-compiler && ./build.sh)
fi

if pgrep -f '[p]ipeline-manager' >/dev/null; then
    echo "A pipeline-manager process is already running; stop it before this test." >&2
    exit 1
fi

mkdir -p "${COMPILER_DIR}"
git clone --quiet --shared . "${RUNTIME_REPO}"

uv run --project python --frozen python - "${RUNTIME_REPO}/crates/feldera-types/src/config.rs" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
source = path.read_text()
needle = "    Datagen(DatagenInputConfig),"
if source.count(needle) != 1:
    raise SystemExit(f"expected one Datagen transport variant in {path}")
path.write_text(source.replace(
    needle,
    '    #[serde(alias = "runtime_only_datagen")]\n' + needle,
))
PY

git -C "${RUNTIME_REPO}" config user.name "Feldera Test"
git -C "${RUNTIME_REPO}" config user.email "test@feldera.com"
git -C "${RUNTIME_REPO}" add crates/feldera-types/src/config.rs
git -C "${RUNTIME_REPO}" commit --quiet -m "Add runtime-only Datagen alias"
RUNTIME_VERSION="$(git -C "${RUNTIME_REPO}" rev-parse HEAD)"

mkdir -p "${TEST_DIR}/web-console"
printf '<!doctype html><title>test</title>\n' >"${TEST_DIR}/web-console/index.html"
WEBCONSOLE_BUILD_DIR="${TEST_DIR}/web-console" \
    cargo build -p pipeline-manager

FELDERA_UNSTABLE_FEATURES=runtime_version \
    target/debug/pipeline-manager \
    --api-port "${API_PORT}" \
    --compiler-port "${COMPILER_PORT}" \
    --runner-port "${RUNNER_PORT}" \
    --pg-embed-working-directory "${TEST_DIR}/postgres" \
    --compiler-working-directory "${COMPILER_DIR}" \
    --runner-working-directory "${TEST_DIR}/runner" \
    --sql-compiler-path "${SQL_COMPILER_PATH}" \
    --dev-mode \
    >"${MANAGER_LOG}" 2>&1 &
MANAGER_PID=$!

for _ in {1..120}; do
    if curl --fail --silent "http://127.0.0.1:${API_PORT}/healthz" >/dev/null; then
        break
    fi
    if ! kill -0 "${MANAGER_PID}" 2>/dev/null; then
        echo "pipeline-manager exited before becoming healthy" >&2
        exit 1
    fi
    sleep 1
done
curl --fail --silent "http://127.0.0.1:${API_PORT}/healthz" >/dev/null

cd python
FELDERA_HOST="http://127.0.0.1:${API_PORT}" \
FELDERA_CUSTOM_RUNTIME_VERSION="${RUNTIME_VERSION}" \
PYTHONPATH=. \
    uv run --frozen pytest tests/platform/test_custom_runtime_transport.py -vv
