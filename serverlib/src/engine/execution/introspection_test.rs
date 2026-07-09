use super::*;

use crate::{FieldDef, FieldIndex, FieldType, TableSchema};

#[test]
fn show_databases_result_sorts_names() {
    let result = show_databases_result(vec!["zeta".to_string(), "alpha".to_string()]);

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "database_name");
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| String::from_utf8(row[0].clone()).expect("value should be utf8"))
            .collect::<Vec<_>>(),
        vec!["alpha".to_string(), "zeta".to_string()]
    );
}

#[test]
fn show_tables_result_sorts_names() {
    let result = show_tables_result(vec![
        ("users".to_string(), "permanent".to_string()),
        ("accounts".to_string(), "memory".to_string()),
    ]);

    assert_eq!(result.columns[0].field_name, "table_name");
    assert_eq!(result.columns[1].field_name, "store_kind");
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| String::from_utf8(row[0].clone()).expect("value should be utf8"))
            .collect::<Vec<_>>(),
        vec!["accounts".to_string(), "users".to_string()]
    );
}

#[test]
fn show_privileges_result_sorts_users_and_emits_three_columns() {
    let result = show_privileges_result(vec![
        (
            "zeta".to_string(),
            vec!["UPDATE".to_string(), "SELECT".to_string()],
            vec!["SELECT".to_string()],
        ),
        (
            "alpha".to_string(),
            vec!["CREATE USER".to_string()],
            vec![],
        ),
    ]);

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0].field_name, "userid");
    assert_eq!(result.columns[1].field_name, "privileges");
    assert_eq!(result.columns[2].field_name, "grantable_privileges");
    assert_eq!(String::from_utf8(result.rows[0][0].clone()).unwrap(), "alpha");
    assert_eq!(String::from_utf8(result.rows[0][1].clone()).unwrap(), "*");
    assert_eq!(String::from_utf8(result.rows[0][2].clone()).unwrap(), "");
    assert_eq!(String::from_utf8(result.rows[1][0].clone()).unwrap(), "zeta");
    assert_eq!(String::from_utf8(result.rows[1][1].clone()).unwrap(), "*");
    assert_eq!(String::from_utf8(result.rows[1][2].clone()).unwrap(), "*");
    
}

#[test]
fn describe_table_result_uses_schema_metadata() {
    let schema = TableSchema::new(vec![
        FieldDef {
            seqno: 1,
            field_name: "id".to_string(),
            field_type: FieldType::UInt(64),
            nullable: false,
            indexed: FieldIndex::PrimaryKey,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 2,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: true,
            indexed: FieldIndex::Indexed,
            default_value: Some(b"sam@example.com".to_vec()),
            metadata: None,
        },
    ]);

    let result = describe_table_result(&schema);

    assert_eq!(result.columns.len(), 6);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(String::from_utf8(result.rows[0][0].clone()).unwrap(), "table");
    assert_eq!(String::from_utf8(result.rows[0][1].clone()).unwrap(), "id");
    assert_eq!(String::from_utf8(result.rows[0][3].clone()).unwrap(), "NO");
    assert_eq!(String::from_utf8(result.rows[0][4].clone()).unwrap(), "PRI");
    assert_eq!(String::from_utf8(result.rows[1][3].clone()).unwrap(), "YES");
    assert_eq!(String::from_utf8(result.rows[1][4].clone()).unwrap(), "MUL");
    assert_eq!(
        String::from_utf8(result.rows[1][5].clone()).unwrap(),
        "sam@example.com"
    );
}

#[test]
fn describe_sql_object_result_returns_original_sql() {
    let result = describe_sql_object_result(
        "stored_procedure",
        "p_sync",
        "create procedure p_sync() begin select 1; end",
    );

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(String::from_utf8(result.rows[0][0].clone()).unwrap(), "stored_procedure");
    assert_eq!(String::from_utf8(result.rows[0][1].clone()).unwrap(), "p_sync");
    assert_eq!(
        String::from_utf8(result.rows[0][2].clone()).unwrap(),
        "create procedure p_sync() begin select 1; end"
    );
}
