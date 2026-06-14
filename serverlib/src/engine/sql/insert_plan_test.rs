use super::*;

#[test]
fn insert_values_helper_extracts_rows() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email, is_active, nickname) values (1, 'sam@example.com', true, null)",
    )
    .expect("insert values should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.columns, vec!["id", "email", "is_active", "nickname"]);
    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some(b"1".to_vec()));
    assert_eq!(rows[0][1], Some(b"sam@example.com".to_vec()));
    assert_eq!(rows[0][2], Some(b"true".to_vec()));
    assert_eq!(rows[0][3], None);
}

#[test]
fn insert_values_supports_inbuilt_commands() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (created_at, email) values (UNIXTIMESTAMP(), CONCAT('sam', '@example.com'))",
    )
    .expect("insert values with inbuilt commands should parse");

    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].as_ref().is_some_and(|v| !v.is_empty()));
    assert_eq!(rows[0][1], Some(b"sam@example.com".to_vec()));
}

#[test]
fn insert_select_helper_extracts_select_plan() {
    let plan = parse_insert_rows_from_statement(
        "insert into users_archive (id, email) select id, email from users where id = 1",
    )
    .expect("insert select should parse");

    assert_eq!(plan.table_id, "users_archive");
    assert_eq!(plan.columns, vec!["id", "email"]);

    let InsertRowsSource::Select(select_plan) = plan.source else {
        panic!("expected SELECT source");
    };

    assert_eq!(select_plan.table_id, "users");
    assert!(select_plan.where_condition.is_some());
    assert!(!select_plan.is_explain);
}

#[test]
fn insert_select_join_helper_extracts_join_plan() {
    let plan = parse_insert_rows_from_statement(
        "insert into user_profile_flat (email, name) select u.email, p.name from users u inner join profiles p on u.id = p.user_id",
    )
    .expect("insert select join should parse");

    let InsertRowsSource::Select(select_plan) = plan.source else {
        panic!("expected SELECT source");
    };

    assert_eq!(select_plan.relations.len(), 2);
    assert_eq!(select_plan.joins.len(), 1);
}
