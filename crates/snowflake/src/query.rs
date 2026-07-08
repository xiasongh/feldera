use anyhow::{bail, Result as AnyResult};
use feldera_types::program_schema::Relation;

pub(crate) fn build_snapshot_query(
    table: &str,
    snapshot_filter: Option<&str>,
    skip_unused_columns: bool,
    relation: &Relation,
) -> AnyResult<String> {
    let table = table.trim();
    if table.is_empty() {
        bail!("Snowflake table name must not be empty");
    }

    let skip_unused_columns =
        skip_unused_columns || relation.get_property("skip_unused_columns") == Some("true");
    let columns = relation
        .fields
        .iter()
        .filter(|field| {
            !skip_unused_columns
                || !field.unused
                || (!field.columntype.nullable && field.default.is_none())
        })
        .map(|field| field.name.sql_name())
        .collect::<Vec<_>>();

    if columns.is_empty() {
        bail!(
            "Snowflake snapshot query for input relation '{}' has no columns to read",
            relation.name
        );
    }

    let mut query = format!("SELECT {} FROM {table}", columns.join(", "));

    if let Some(filter) = snapshot_filter {
        let filter = filter.trim();
        if filter.is_empty() {
            bail!("Snowflake snapshot filter must not be empty when specified");
        }
        query.push_str(" WHERE ");
        query.push_str(filter);
    }

    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_types::program_schema::{
        ColumnType, Field, PropertyValue, Relation, SourcePosition, SqlIdentifier,
    };

    fn relation(fields: &[&str]) -> Relation {
        Relation {
            name: SqlIdentifier::from("T".to_string()),
            fields: fields
                .iter()
                .map(|name| {
                    Field::new(
                        SqlIdentifier::from((*name).to_string()),
                        ColumnType::varchar(true),
                    )
                })
                .collect(),
            materialized: false,
            properties: Default::default(),
            primary_key: None,
        }
    }

    #[test]
    fn builds_snapshot_query() {
        let query = build_snapshot_query(
            "DB.SCHEMA.T",
            Some("ID > 10"),
            false,
            &relation(&["ID", "\"customer name\""]),
        )
        .unwrap();

        assert_eq!(
            query,
            "SELECT ID, \"customer name\" FROM DB.SCHEMA.T WHERE ID > 10"
        );
    }

    #[test]
    fn reads_declared_unused_columns_by_default() {
        let mut relation = relation(&["ID", "UNUSED"]);
        relation.fields[1].unused = true;

        assert_eq!(
            build_snapshot_query("T", None, false, &relation).unwrap(),
            "SELECT ID, UNUSED FROM T"
        );
    }

    #[test]
    fn skips_unused_columns_when_requested_by_table() {
        let mut relation = relation(&["ID", "UNUSED"]);
        relation.fields[1].unused = true;
        let position = SourcePosition {
            start_line_number: 0,
            start_column: 0,
            end_line_number: 0,
            end_column: 0,
        };
        relation.properties.insert(
            "skip_unused_columns".to_string(),
            PropertyValue {
                value: "true".to_string(),
                key_position: position,
                value_position: position,
            },
        );

        assert_eq!(
            build_snapshot_query("T", None, false, &relation).unwrap(),
            "SELECT ID FROM T"
        );
    }

    #[test]
    fn skips_unused_columns_when_requested_by_connector() {
        let mut relation = relation(&["ID", "UNUSED"]);
        relation.fields[1].unused = true;

        assert_eq!(
            build_snapshot_query("T", None, true, &relation).unwrap(),
            "SELECT ID FROM T"
        );
    }

    #[test]
    fn retains_nonnullable_unused_columns_without_defaults() {
        let mut relation = relation(&["ID", "REQUIRED"]);
        relation.fields[1].unused = true;
        relation.fields[1].columntype.nullable = false;

        assert_eq!(
            build_snapshot_query("T", None, true, &relation).unwrap(),
            "SELECT ID, REQUIRED FROM T"
        );
    }

    #[test]
    fn rejects_empty_table() {
        let err = build_snapshot_query(" ", None, false, &relation(&["ID"])).unwrap_err();
        assert!(err.to_string().contains("table name"));
    }
}
