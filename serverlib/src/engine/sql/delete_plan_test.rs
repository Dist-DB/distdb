use super::*;
use crate::SelectCondition;

#[test]
fn delete_rows_helper_extracts_table_and_where() {
    let plan = parse_delete_rows_from_statement("delete from users where id = 1")
        .expect("delete statement should parse");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
    assert!(plan.joins.is_empty());
}

#[test]
fn delete_rows_helper_supports_delete_without_where() {
    let plan = parse_delete_rows_from_statement("delete from users")
        .expect("delete without where should parse");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_none());
}

#[test]
fn delete_rows_helper_parses_join_information() {
    let plan = parse_delete_rows_from_statement(
        "delete u from users u join profiles p on u.id = p.user_id where p.name = 'Sam'",
    )
    .expect("delete with join should parse");

    assert_eq!(plan.relations.len(), 2);
    assert_eq!(plan.joins.len(), 1);
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_parses_multiple_joins() {
    let plan = parse_delete_rows_from_statement(
            "delete u from users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where t.label is null",
        )
        .expect("delete with multiple joins should parse");

    assert_eq!(plan.relations.len(), 3);
    assert_eq!(plan.joins.len(), 2);
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_parses_complex_join_on_condition() {
    let plan = parse_delete_rows_from_statement(
        "delete u from users u join profiles p on u.id = p.user_id and p.name = 'Sam'",
    )
    .expect("delete with complex join ON should parse");

    assert_eq!(plan.joins.len(), 1);
    assert!(matches!(plan.joins[0].on_condition, SelectCondition::And(_)));
}
