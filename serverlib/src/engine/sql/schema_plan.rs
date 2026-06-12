use std::collections::HashSet;

use common::schema::FieldMetadata;
use sqlparser::ast::{AlterTableOperation, ColumnOption, DataType, Statement, TableConstraint};

use crate::{FieldDef, FieldIndex, FieldType, TableSchema};

use super::literals::parse_default_value;
use super::{parse_mysql_statements, AlterTableChangeOp, AlterTableChangePlan, SqlParseError};

pub fn create_table_schema_from_statement(
    statement: &str,
) -> Result<(String, TableSchema), SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::CreateTable(create_table) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE TABLE".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(create_table.name.to_string());
    
    let (primary_key_fields, indexed_fields) =
        derive_indexed_fields_from_constraints(&create_table.constraints);

    let mut fields = Vec::with_capacity(create_table.columns.len());

    for (idx, column) in create_table.columns.iter().enumerate() {

        let metadata = extract_field_metadata(column);

        let nullable = !column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::NotNull));

        let indexed = if column.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        }) {
            FieldIndex::PrimaryKey
        } else if column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::Unique { .. }))
        {
            FieldIndex::Indexed
        } else if primary_key_fields.contains(&common::normalize_identifier!(&column.name.value)) {
            FieldIndex::PrimaryKey
        } else if indexed_fields.contains(&common::normalize_identifier!(&column.name.value)) {
            FieldIndex::Indexed
        } else {
            FieldIndex::None
        };

        let default_value = column.options.iter().find_map(|opt| match &opt.option {
            ColumnOption::Default(expr) => parse_default_value(expr.to_string()),
            _ => None,
        });

        fields.push(FieldDef {
            seqno: (idx + 1) as u32,
            field_name: column.name.value.clone(),
            field_type: map_sql_data_type(&column.data_type),
            nullable,
            indexed,
            default_value,
            metadata,
        });

    }

    let schema = TableSchema::new(fields);

    schema.validate().map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("invalid CREATE TABLE schema: {err}"))
    })?;

    Ok((table_id, schema))

}

pub fn parse_alter_table_change_plan_from_statement(
    statement: &str,
) -> Result<AlterTableChangePlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::AlterTable {
        name,
        operations,
        ..
    } = single
    else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not ALTER TABLE".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(name.to_string());
    let mut plan_ops = Vec::new();

    for operation in operations {

        match operation {

            AlterTableOperation::AddColumn { column_def, .. } => {
                let nullable = !column_def
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::NotNull));

                let indexed = if column_def.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        ColumnOption::Unique {
                            is_primary: true,
                            ..
                        }
                    )
                }) {
                    FieldIndex::PrimaryKey
                } else if column_def
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::Unique { .. }))
                {
                    FieldIndex::Indexed
                } else {
                    FieldIndex::None
                };

                let default_value = column_def.options.iter().find_map(|opt| match &opt.option {
                    ColumnOption::Default(expr) => parse_default_value(expr.to_string()),
                    _ => None,
                });

                plan_ops.push(AlterTableChangeOp::AddField(FieldDef {
                    seqno: 0,
                    field_name: column_def.name.value.clone(),
                    field_type: map_sql_data_type(&column_def.data_type),
                    nullable,
                    indexed,
                    default_value,
                    metadata: extract_field_metadata(column_def),
                }));
            }

            AlterTableOperation::DropColumn { column_name, .. } => {
                plan_ops.push(AlterTableChangeOp::DropField(column_name.value.clone()));
            }

            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                plan_ops.push(AlterTableChangeOp::RenameField {
                    from: old_column_name.value.clone(),
                    to: new_column_name.value.clone(),
                });
            }

            _ => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "unsupported ALTER TABLE operation: {operation}"
                )));
            }

        }

    }

    Ok(AlterTableChangePlan {
        table_id,
        operations: plan_ops,
    })
    
}

fn derive_indexed_fields_from_constraints(
    constraints: &[TableConstraint],
) -> (Vec<String>, HashSet<String>) {

    let mut primary = Vec::new();
    let mut indexed = HashSet::new();

    for constraint in constraints {
        let rendered = constraint.to_string();
        let lowered = rendered.to_ascii_lowercase();
        let Some(columns) = extract_constraint_columns(&rendered) else {
            continue;
        };

        if lowered.contains("primary key") {
            for column in columns {
                if !primary.contains(&column) {
                    primary.push(column.clone());
                }
                indexed.insert(column);
            }
            continue;
        }

        if lowered.starts_with("key ")
            || lowered.starts_with("index ")
            || lowered.starts_with("unique")
        {
            indexed.extend(columns);
        }
    }

    (primary, indexed)

}

fn extract_constraint_columns(constraint: &str) -> Option<Vec<String>> {

    let start = constraint.find('(')?;
    let end = constraint.rfind(')')?;

    if end <= start + 1 {
        return None;
    }

    let columns = constraint[start + 1..end]
        .split(',')
        .filter_map(|segment| {
            let raw = segment.trim();
            if raw.is_empty() {
                return None;
            }
            Some(common::normalize_identifier!(raw))
        })
        .collect::<Vec<_>>();

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

fn map_sql_data_type(data_type: &DataType) -> FieldType {

    let rendered = data_type.to_string();
    let lowered = rendered.to_ascii_lowercase();

    if lowered.starts_with("enum(") {
        if let Some(variants) = parse_enum_variants(&rendered) {
            return FieldType::Enum(variants);
        }
        return FieldType::Text;
    }

    if lowered.starts_with("timestamp") {
        return FieldType::Timestamp;
    }

    if lowered.starts_with("datetime") {
        return FieldType::DateTime;
    }

    if lowered == "date" {
        return FieldType::Date;
    }

    if lowered.contains("unsigned") {
        if lowered.contains("bigint") {
            return FieldType::UInt(64);
        }

        if lowered.contains("smallint") {
            return FieldType::UInt(16);
        }

        if lowered.contains("tinyint") {
            return FieldType::UInt(8);
        }

        return FieldType::UInt(32);
    }

    if lowered.contains("bigint") {
        return FieldType::Int(64);
    }

    if lowered.contains("smallint") {
        return FieldType::Int(16);
    }

    if lowered.contains("tinyint") {
        return FieldType::Int(8);
    }

    if lowered.contains("int") {
        return FieldType::Int(32);
    }

    if lowered.contains("double") {
        return FieldType::Float(64);
    }

    if lowered.contains("float") || lowered.contains("real") {
        return FieldType::Float(32);
    }

    if lowered.contains("blob") || lowered.contains("binary") {
        return FieldType::Blob;
    }

    if let Some(len) = parse_sql_type_len(&lowered, "varchar(") {
        return FieldType::StringFixed(len.max(1));
    }

    if lowered.contains("varchar") {
        return FieldType::StringFixed(255);
    }

    if let Some(len) = parse_sql_type_len(&lowered, "char(") {
        return FieldType::StringFixed(len.max(1));
    }

    if lowered.contains("char") {
        return FieldType::StringFixed(32);
    }

    if lowered.contains("text") || lowered.contains("varchar") || lowered.contains("string") {
        return FieldType::Text;
    }

    FieldType::Text

}

fn parse_enum_variants(sql_type: &str) -> Option<Vec<String>> {

    let start = sql_type.find('(')?;
    let end = sql_type.rfind(')')?;

    if end <= start + 1 {
        return None;
    }

    let mut variants = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = sql_type[start + 1..end].chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quote {
            if ch == '\'' {
                if matches!(chars.peek(), Some('\'')) {
                    current.push('\'');
                    let _ = chars.next();
                } else {
                    in_quote = false;
                    variants.push(std::mem::take(&mut current));
                }
            } else {
                current.push(ch);
            }
            continue;
        }

        if ch == '\'' {
            in_quote = true;
        }
    }

    if in_quote || variants.is_empty() {
        return None;
    }

    Some(variants)
}

fn parse_sql_type_len(lowered_type: &str, marker: &str) -> Option<usize> {
    let start = lowered_type.find(marker)? + marker.len();
    let end = lowered_type[start..].find(')')? + start;
    lowered_type[start..end].trim().parse::<usize>().ok()
}

fn extract_field_metadata(column: &sqlparser::ast::ColumnDef) -> Option<FieldMetadata> {

    let mut metadata = FieldMetadata::default();
    metadata.original_sql_type = Some(column.data_type.to_string());

    for option in &column.options {
        match &option.option {
            ColumnOption::Comment(comment) => {
                metadata.comment = Some(comment.clone());
            }

            ColumnOption::CharacterSet(charset) => {
                metadata.character_set = Some(charset.to_string());
            }

            ColumnOption::DialectSpecific(tokens) => {
                let lowered = tokens
                    .iter()
                    .map(|token| token.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_ascii_lowercase();

                if lowered.contains("auto_increment") || lowered.contains("autoincrement") {
                    metadata.auto_increment = true;
                }
            }

            _ => {}
        }
    }

    if metadata.collation.is_none() {
        metadata.collation = extract_collation_from_column(column);
    }

    if metadata.comment.is_none()
        && !metadata.auto_increment
        && metadata.original_sql_type.is_none()
        && metadata.character_set.is_none()
        && metadata.collation.is_none()
    {
        return None;
    }

    Some(metadata)

}

fn extract_collation_from_column(column: &sqlparser::ast::ColumnDef) -> Option<String> {

    let rendered = column.to_string();
    let segments = rendered.split_whitespace().collect::<Vec<_>>();

    for idx in 0..segments.len() {
        if segments[idx].eq_ignore_ascii_case("collate") {
            return segments.get(idx + 1).map(|value| {
                value
                    .trim_matches('`')
                    .trim_matches('"')
                    .trim_matches('\'')
                    .trim_end_matches(',')
                    .to_string()
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{FieldIndex, FieldType};

    #[test]
    fn create_table_schema_helper_maps_fields() {
        let (table_id, schema) = create_table_schema_from_statement(
            "create table users (id bigint not null primary key, email varchar(255) not null, age int)",
        )
        .expect("create table schema should parse");

        assert_eq!(table_id, "users");
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].field_name, "id");
        assert_eq!(schema.fields[0].field_type, FieldType::Int(64));
        assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);
        assert!(!schema.fields[0].nullable);

        assert_eq!(schema.fields[1].field_name, "email");
        assert_eq!(schema.fields[1].field_type, FieldType::StringFixed(255));
        assert!(!schema.fields[1].nullable);

        assert_eq!(schema.fields[2].field_name, "age");
        assert_eq!(schema.fields[2].field_type, FieldType::Int(32));
        assert!(schema.fields[2].nullable);
    }

    #[test]
    fn create_table_schema_maps_varchar_with_length() {
        let (_, schema) = create_table_schema_from_statement("create table users (email varchar(34) not null)")
            .expect("create table schema should parse");

        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].field_name, "email");
        assert_eq!(schema.fields[0].field_type, FieldType::StringFixed(34));
        assert!(!schema.fields[0].nullable);
    }

    #[test]
    fn create_table_schema_captures_auto_increment_and_encoding_metadata() {
        let (_, schema) = create_table_schema_from_statement(
            "create table users (id bigint not null auto_increment primary key, username varchar(34) character set utf8mb3 collate utf8mb3_general_ci comment 'login handle')",
        )
        .expect("auto increment and encoding metadata should parse");

        assert_eq!(schema.fields.len(), 2);

        let id_metadata = schema.fields[0]
            .metadata
            .as_ref()
            .expect("id field should include metadata");
        assert!(id_metadata.auto_increment);

        let username_metadata = schema.fields[1]
            .metadata
            .as_ref()
            .expect("username field should include metadata");
        assert_eq!(username_metadata.original_sql_type.as_deref(), Some("VARCHAR(34)"));
        assert_eq!(username_metadata.character_set.as_deref(), Some("utf8mb3"));
        assert_eq!(
            username_metadata.collation.as_deref(),
            Some("utf8mb3_general_ci")
        );
        assert_eq!(username_metadata.comment.as_deref(), Some("login handle"));
    }

    #[test]
    fn create_table_schema_maps_temporal_types_and_preserves_original_sql_type() {
        let (_, schema) = create_table_schema_from_statement(
            "create table events (created_on date, created_at datetime, updated_at timestamp)",
        )
        .expect("temporal types should parse");

        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].field_type, FieldType::Date);
        assert_eq!(schema.fields[1].field_type, FieldType::DateTime);
        assert_eq!(schema.fields[2].field_type, FieldType::Timestamp);

        assert_eq!(
            schema.fields[0]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.original_sql_type.as_deref()),
            Some("DATE")
        );
        assert_eq!(
            schema.fields[1]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.original_sql_type.as_deref()),
            Some("DATETIME")
        );
        assert_eq!(
            schema.fields[2]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.original_sql_type.as_deref()),
            Some("TIMESTAMP")
        );
    }

    #[test]
    fn create_table_schema_tracks_table_level_keys_defaults_and_enum() {
        let sql = "CREATE TABLE `__account` (
          `uid` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
          `id_person` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
          `id_device` varchar(34) DEFAULT NULL,
          `id_organization` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
          `role` enum('user','admin') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT 'user',
          `date_created` bigint NOT NULL DEFAULT '0',
          `date_updated` bigint NOT NULL DEFAULT '0',
          `date_lastlogin` bigint NOT NULL DEFAULT '0',
          `is_verified` tinyint unsigned NOT NULL DEFAULT '0',
          `is_deleted` tinyint unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`uid`),
          KEY `id_device` (`id_device`),
          KEY `id_person` (`id_person`),
          CONSTRAINT `__account_ibfk_1` FOREIGN KEY (`id_device`) REFERENCES `__devices` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE,
          CONSTRAINT `__account_ibfk_2` FOREIGN KEY (`id_person`) REFERENCES `__person` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;";

        let (table_id, schema) =
            create_table_schema_from_statement(sql).expect("schema should parse");

        assert_eq!(table_id, "__account");

        let uid = schema.field("uid").expect("uid field should exist");
        assert_eq!(uid.indexed, FieldIndex::PrimaryKey);
        assert_eq!(uid.default_value.as_deref(), Some(&b""[..]));

        let id_person = schema
            .field("id_person")
            .expect("id_person field should exist");
        assert_eq!(id_person.indexed, FieldIndex::Indexed);
        assert!(id_person.default_value.is_none());

        let id_device = schema
            .field("id_device")
            .expect("id_device field should exist");
        assert_eq!(id_device.indexed, FieldIndex::Indexed);
        assert!(id_device.default_value.is_none());

        let role = schema.field("role").expect("role field should exist");
        assert_eq!(
            role.field_type,
            FieldType::Enum(vec!["user".to_string(), "admin".to_string()])
        );
        assert_eq!(role.default_value.as_deref(), Some(&b"user"[..]));
        assert_eq!(
            role.metadata
                .as_ref()
                .and_then(|metadata| metadata.original_sql_type.as_deref()),
            Some("ENUM('user', 'admin')")
        );
    }

    #[test]
    fn alter_table_change_plan_parses_add_drop_and_rename() {
        let plan = parse_alter_table_change_plan_from_statement(
            "alter table users add column status varchar(20) not null default 'active', drop column legacy, rename column email to login_email",
        )
        .expect("alter table should parse");

        assert_eq!(plan.table_id, "users");
        assert_eq!(plan.operations.len(), 3);

        match &plan.operations[0] {
            AlterTableChangeOp::AddField(field) => {
                assert_eq!(field.field_name, "status");
                assert_eq!(field.default_value.as_deref(), Some(&b"active"[..]));
            }
            _ => panic!("expected add field operation"),
        }

        match &plan.operations[1] {
            AlterTableChangeOp::DropField(name) => assert_eq!(name, "legacy"),
            _ => panic!("expected drop field operation"),
        }

        match &plan.operations[2] {
            AlterTableChangeOp::RenameField { from, to } => {
                assert_eq!(from, "email");
                assert_eq!(to, "login_email");
            }
            _ => panic!("expected rename field operation"),
        }
    }
}
