use crate::{FieldDef, FieldIndex, FieldType, TableSchema};

use super::select::SelectExecutionResult;

pub fn show_databases_result<I>(database_ids: I) -> SelectExecutionResult
where
    I: IntoIterator<Item = String>,
{
    let mut database_ids = database_ids.into_iter().collect::<Vec<_>>();
    database_ids.sort();

    single_text_column_result(
        "database_name",
        database_ids
            .into_iter()
            .map(|database_id| vec![database_id.into_bytes()])
            .collect(),
    )
}

pub fn show_tables_result<I>(table_ids: I) -> SelectExecutionResult
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut table_rows = table_ids.into_iter().collect::<Vec<_>>();
    table_rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

    two_text_column_result(
        "table_name",
        "store_kind",
        table_rows
            .into_iter()
            .map(|(table_id, store_kind)| vec![table_id.into_bytes(), store_kind.into_bytes()])
            .collect(),
    )
}

pub fn describe_table_result(schema: &TableSchema) -> SelectExecutionResult {

    let rows = schema
        .fields
        .iter()
        .map(|field| {
            let nullable = if field.nullable { "YES" } else { "NO" };
            let key = match field.indexed {
                FieldIndex::PrimaryKey => "PRI",
                FieldIndex::Indexed => "MUL",
                FieldIndex::None => "",
            };
            let default_value = field
                .default_value
                .as_ref()
                .map(|value| String::from_utf8_lossy(value).to_string())
                .unwrap_or_else(|| "NULL".to_string());

            vec![
                b"table".to_vec(),
                field.field_name.clone().into_bytes(),
                field
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.original_sql_type.clone())
                    .unwrap_or_else(|| format!("{:?}", field.field_type))
                    .into_bytes(),
                nullable.as_bytes().to_vec(),
                key.as_bytes().to_vec(),
                default_value.into_bytes(),
            ]
        })
        .collect();

    SelectExecutionResult {
        columns: vec![
            text_column(1, "object_type"),
            text_column(2, "field"),
            text_column(3, "type"),
            text_column(4, "null"),
            text_column(5, "key"),
            text_column(6, "default"),
        ],
        rows,
    }

}

pub fn describe_sql_object_result(
    object_type: &str,
    object_name: &str,
    sql: &str,
) -> SelectExecutionResult {

    SelectExecutionResult {
        columns: vec![
            text_column(1, "object_type"),
            text_column(2, "object_name"),
            text_column(3, "sql"),
        ],
        rows: vec![vec![
            object_type.as_bytes().to_vec(),
            object_name.as_bytes().to_vec(),
            sql.as_bytes().to_vec(),
        ]],
    }

}

fn single_text_column_result(field_name: &str, rows: Vec<Vec<Vec<u8>>>) -> SelectExecutionResult {

    SelectExecutionResult {
        columns: vec![text_column(1, field_name)],
        rows,
    }
    
}

fn two_text_column_result(
    first_field_name: &str,
    second_field_name: &str,
    rows: Vec<Vec<Vec<u8>>>,
) -> SelectExecutionResult {

    SelectExecutionResult {
        columns: vec![text_column(1, first_field_name), text_column(2, second_field_name)],
        rows,
    }

}

fn text_column(seqno: u32, field_name: &str) -> FieldDef {
    FieldDef {
        seqno,
        field_name: field_name.to_string(),
        field_type: FieldType::Text,
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }
}


#[cfg(test)]
#[path = "introspection_test.rs"]
mod tests;
