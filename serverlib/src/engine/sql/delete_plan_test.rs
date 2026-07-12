use super::*;
use crate::SelectCondition;
use crate::engine::sql::MutationReturningItem;

#[test]
fn delete_rows_helper_extracts_table_and_where() {
    let plan = parse_delete_rows_from_statement("delete from users where id = 1")
        .expect("delete statement should parse");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
    assert!(plan.joins.is_empty());
}

#[test]
fn delete_rows_helper_accepts_low_priority_quick_ignore_modifiers() {
    let plan = parse_delete_rows_from_statement(
        "delete low_priority quick ignore from users where id = 1",
    )
    .expect("delete low_priority quick ignore should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_accepts_force_index_hint_in_compat_mode() {
    let plan = parse_delete_rows_from_statement(
        "delete from users force index (idx_users_id) where id = 1",
    )
    .expect("delete force index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_accepts_ignore_index_hint_in_compat_mode() {
    let plan = parse_delete_rows_from_statement(
        "delete from users ignore index (idx_users_id) where id = 1",
    )
    .expect("delete ignore index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_accepts_force_key_hint_in_compat_mode() {
    let plan = parse_delete_rows_from_statement(
        "delete from users force key for join (idx_users_id) where id = 1",
    )
    .expect("delete force key hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert!(plan.where_condition.is_some());
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

#[test]
fn delete_rows_helper_parses_using_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users using profiles where users.id = profiles.user_id",
    )
    .expect("delete using should parse");

    assert_eq!(plan.relations.len(), 2);
    assert_eq!(plan.joins.len(), 1);
    assert!(matches!(plan.joins[0].on_condition, SelectCondition::And(ref children) if children.is_empty()));
    assert!(plan.where_condition.is_some());
}

#[test]
fn delete_rows_helper_parses_returning_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users where id = 1 returning id",
    )
    .expect("delete returning should parse");

    let returning = plan.returning.expect("returning plan should be present");
    assert_eq!(returning.len(), 1);
    assert!(matches!(
        &returning[0],
        MutationReturningItem::Column {
            field_name,
            output_name,
        } if field_name == "id" && output_name == "id"
    ));
}

#[test]
fn delete_rows_helper_rejects_returning_expression() {
    let err = parse_delete_rows_from_statement(
        "delete from users where id = 1 returning id + 1",
    )
    .expect_err("delete returning expression should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message.contains("DELETE RETURNING expression")
    ));
}

#[test]
fn delete_rows_helper_parses_order_by_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by id desc",
    )
    .expect("delete order by should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "id");
    assert!(plan.order_by[0].descending);
}

#[test]
fn delete_rows_helper_parses_limit_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users limit 1",
    )
    .expect("delete limit should parse");

    assert_eq!(plan.limit, Some(1));
}

#[test]
fn delete_rows_helper_supports_order_by_lower_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by lower(id)",
    )
    .expect("delete order by lower expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_lower__:id");
    assert!(!plan.order_by[0].descending);
}

#[test]
fn delete_rows_helper_supports_order_by_abs_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by abs(id)",
    )
    .expect("delete order by abs expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_abs__:id");
}

#[test]
fn delete_rows_helper_supports_order_by_length_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by length(name)",
    )
    .expect("delete order by length expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_length__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_ucase_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by ucase(name)",
    )
    .expect("delete order by ucase expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_upper__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_char_length_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by char_length(name)",
    )
    .expect("delete order by char_length expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_length__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_reverse_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by reverse(name)",
    )
    .expect("delete order by reverse expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_reverse__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_trim_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by trim(name)",
    )
    .expect("delete order by trim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_trim__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_ltrim_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by ltrim(name)",
    )
    .expect("delete order by ltrim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_ltrim__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_rtrim_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by rtrim(name)",
    )
    .expect("delete order by rtrim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_rtrim__:name");
}

#[test]
fn delete_rows_helper_supports_order_by_floor_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by floor(score)",
    )
    .expect("delete order by floor expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_floor__:score");
}

#[test]
fn delete_rows_helper_supports_order_by_round_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by round(score)",
    )
    .expect("delete order by round expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_round__:score");
}

#[test]
fn delete_rows_helper_supports_order_by_round_with_scale_expression_clause() {
    let plan = parse_delete_rows_from_statement(
        "delete from users order by round(score,1)",
    )
    .expect("delete order by round with scale expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_round_scale__:1:score");
}
