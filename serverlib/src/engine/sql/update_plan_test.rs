use super::*;

#[test]
fn update_rows_helper_extracts_assignments_and_where() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true, nickname = null where id = 1",
    )
    .expect("update statement should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 2);
    assert_eq!(plan.assignments[0].field_name, "active");
    assert_eq!(plan.assignments[0].value, Some(b"true".to_vec()));
    assert_eq!(plan.assignments[1].field_name, "nickname");
    assert_eq!(plan.assignments[1].value, None);
    assert!(plan.where_condition.is_some());
    assert!(plan.joins.is_empty());
}

#[test]
fn update_rows_helper_supports_inbuilt_function_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set updated_at = UNIXTIMESTAMP() where id = 1",
    )
    .expect("update with inbuilt function should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].field_name, "updated_at");
    assert!(plan.assignments[0]
        .value
        .as_ref()
        .is_some_and(|v| !v.is_empty()));
}

#[test]
fn update_rows_helper_parses_join_information() {
    let plan = parse_update_rows_from_statement(
            "update users u join profiles p on u.id = p.user_id set u.email = 'sam@example.com' where p.name = 'Sam'",
        )
        .expect("update with join should parse");

    assert_eq!(plan.relations.len(), 2);
    assert_eq!(plan.joins.len(), 1);
    assert!(plan.where_condition.is_some());
}

#[test]
fn update_rows_helper_parses_multiple_joins() {
    let plan = parse_update_rows_from_statement(
            "update users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id set u.email = 'sam@example.com' where t.label = 'core'",
        )
        .expect("update with multiple joins should parse");

    assert_eq!(plan.relations.len(), 3);
    assert_eq!(plan.joins.len(), 2);
    assert!(plan.where_condition.is_some());
}
