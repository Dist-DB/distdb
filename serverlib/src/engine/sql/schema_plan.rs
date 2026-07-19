use std::collections::HashSet;

use common::schema::FieldMetadata;
use sqlparser::ast::{AlterTableOperation, ColumnOption, DataType, Statement, TableConstraint};

use crate::{FieldDef, FieldIndex, FieldType, TableSchema};

use super::literals::parse_default_value;
use super::{parse_mysql_statements, AlterTableChangeOp, AlterTableChangePlan, SqlParseError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub table_id: String,
    pub schema: TableSchema,
    pub temporary: bool,
}

pub fn create_table_schema_from_statement(
    statement: &str,
) -> Result<(String, TableSchema), SqlParseError> {

    let plan = create_table_plan_from_statement(statement)?;
    Ok((plan.table_id, plan.schema))

}

pub fn create_table_plan_from_statement(
    statement: &str,
) -> Result<CreateTablePlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::CreateTable(create_table) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE TABLE".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(create_table.name.to_string());
    
    let (primary_key_fields, indexed_fields, unique_fields) =
        derive_indexed_fields_from_constraints(&create_table.constraints);

    let mut fields = Vec::with_capacity(create_table.columns.len());

    for (idx, column) in create_table.columns.iter().enumerate() {

        let mut metadata = extract_field_metadata(column);

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

        if unique_fields.contains(&common::normalize_identifier!(&column.name.value)) {
            let mut resolved = metadata.unwrap_or_default();
            resolved.unique = true;
            metadata = Some(resolved);
        }

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

    Ok(CreateTablePlan {
        table_id,
        schema,
        temporary: create_table.temporary,
    })

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

        if let Some(modify_op) = parse_modify_column_change_op(operation)? {
            plan_ops.push(modify_op);
            continue;
        }

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

            },

            AlterTableOperation::DropColumn { column_name, .. } => {
                plan_ops.push(AlterTableChangeOp::DropField(column_name.value.clone()));
            },

            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                plan_ops.push(AlterTableChangeOp::RenameField {
                    from: old_column_name.value.clone(),
                    to: new_column_name.value.clone(),
                });
            },

            _ => {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "unsupported ALTER TABLE operation: {operation}"
                )));
            },

        }

    }

    Ok(AlterTableChangePlan {
        table_id,
        operations: plan_ops,
    })
    
}

fn parse_modify_column_change_op(
    operation: &AlterTableOperation,
) -> Result<Option<AlterTableChangeOp>, SqlParseError> {

    let rendered = operation.to_string();
    let lowered = rendered.to_ascii_lowercase();

    let specification = if lowered.starts_with("modify column ") {
        rendered["modify column ".len()..].trim()
    } else if lowered.starts_with("modify ") {
        rendered["modify ".len()..].trim()
    } else {
        ""
    };

    if specification.is_empty() {
        return Ok(None);
    }

    let synthetic_create = format!("create table __distdb_modify_probe__ ({specification})");
    let (_, schema) = create_table_schema_from_statement(&synthetic_create).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!(
            "unsupported ALTER TABLE operation: {operation} ({err})"
        ))
    })?;

    let Some(field) = schema.fields.first() else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "unsupported ALTER TABLE operation: {operation}"
        )));
    };

    Ok(Some(AlterTableChangeOp::ModifyField {
        field_name: common::normalize_identifier!(&field.field_name),
        new_type: field.field_type.clone(),
    }))

}

fn derive_indexed_fields_from_constraints(
    constraints: &[TableConstraint],
) -> (Vec<String>, HashSet<String>, HashSet<String>) {

    let mut primary = Vec::new();
    let mut indexed = HashSet::new();
    let mut unique = HashSet::new();

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

        if lowered.starts_with("unique") {
            unique.extend(columns.iter().cloned());
            indexed.extend(columns);
            continue;
        }

        if lowered.starts_with("key ")
            || lowered.starts_with("index ")
        {
            indexed.extend(columns);
        }

    }

    (primary, indexed, unique)

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

    if lowered == "uuid" || lowered.starts_with("uuid(") {
        return FieldType::Uuid;
    }

    if lowered == "db_uuid" || lowered.starts_with("db_uuid(") {
        return FieldType::Uuid;
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
        None
    } else {
        Some(variants)
    }

}

fn parse_sql_type_len(lowered_type: &str, marker: &str) -> Option<usize> {
    let start = lowered_type.find(marker)? + marker.len();
    let end = lowered_type[start..].find(')')? + start;
    lowered_type[start..end].trim().parse::<usize>().ok()
}

#[expect(clippy::field_reassign_with_default, reason = "metadata fields are conditionally set based on column options")]
fn extract_field_metadata(column: &sqlparser::ast::ColumnDef) -> Option<FieldMetadata> {

    let mut metadata = FieldMetadata::default();
    metadata.original_sql_type = Some(column.data_type.to_string());

    for option in &column.options {

        match &option.option {
            
            ColumnOption::Comment(comment) => {
                metadata.comment = Some(comment.clone());
            },

            ColumnOption::CharacterSet(charset) => {
                metadata.character_set = Some(charset.to_string());
            },

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
            },

            ColumnOption::Unique {
                is_primary: false,
                ..
            } => {
                metadata.unique = true;
            },

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
#[path = "schema_plan_test.rs"]
mod tests;
