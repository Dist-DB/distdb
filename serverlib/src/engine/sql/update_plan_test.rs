use super::*;
use crate::SelectCondition;
use crate::engine::sql::MutationReturningItem;

#[test]
fn update_rows_helper_extracts_assignments_and_where() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true, nickname = null where id = 1",
    )
    .expect("update statement should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 2);
    assert_eq!(plan.assignments[0].field_name, "active");
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Literal(Some(value)) if value == b"true"
    ));
    assert_eq!(plan.assignments[1].field_name, "nickname");
    assert!(matches!(
        plan.assignments[1].value,
        UpdateAssignmentValue::Literal(None)
    ));
    assert!(plan.where_condition.is_some());
    assert!(plan.joins.is_empty());
}

#[test]
fn update_rows_helper_accepts_low_priority_ignore_modifiers() {
    let plan = parse_update_rows_from_statement(
        "update low_priority ignore users set active = true where id = 1",
    )
    .expect("update low_priority ignore should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 1);
}

#[test]
fn update_rows_helper_accepts_use_index_hint_in_compat_mode() {
    let plan = parse_update_rows_from_statement(
        "update users use index (idx_users_id) set active = true where id = 1",
    )
    .expect("update use index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 1);
}

#[test]
fn update_rows_helper_accepts_ignore_index_hint_in_compat_mode() {
    let plan = parse_update_rows_from_statement(
        "update users ignore index (idx_users_id) set active = true where id = 1",
    )
    .expect("update ignore index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 1);
}

#[test]
fn update_rows_helper_accepts_force_key_hint_in_compat_mode() {
    let plan = parse_update_rows_from_statement(
        "update users force key for join (idx_users_id) set active = true where id = 1",
    )
    .expect("update force key hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.assignments.len(), 1);
}

#[test]
fn update_rows_helper_supports_inbuilt_function_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set updated_at = UNIXTIMESTAMP() where id = 1",
    )
    .expect("update with inbuilt function should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].field_name, "updated_at");
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::FunctionExpression(expression)
            if expression.eq_ignore_ascii_case("unixtimestamp()")
    ));
}

#[test]
fn update_rows_helper_supports_existing_column_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set email = nickname where id = 1",
    )
    .expect("update with existing-column assignment should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].field_name, "email");
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::ExistingColumn(column) if column == "nickname"
    ));
}

#[test]
fn update_rows_helper_supports_arithmetic_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set login_count = login_count + 1 where id = 1",
    )
    .expect("update with arithmetic assignment should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].field_name, "login_count");
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Arithmetic {
            left: UpdateAssignmentOperand::ExistingColumn(left),
            op: UpdateArithmeticOp::Add,
            right: UpdateAssignmentOperand::Literal(Some(right)),
        } if left == "login_count" && right == b"1"
    ));
}

#[test]
fn update_rows_helper_supports_nested_arithmetic_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set login_count = (login_count + 1) * 2 where id = 1",
    )
    .expect("update with nested arithmetic assignment should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert_eq!(plan.assignments[0].field_name, "login_count");
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Arithmetic {
            left,
            op: UpdateArithmeticOp::Multiply,
            right: UpdateAssignmentOperand::Literal(Some(right)),
        }
            if matches!(
                left,
                UpdateAssignmentOperand::Arithmetic {
                    left: inner_left,
                    op: UpdateArithmeticOp::Add,
                    right: inner_right,
                }
                    if matches!(
                        inner_left.as_ref(),
                        UpdateAssignmentOperand::ExistingColumn(column) if column == "login_count"
                    )
                    && matches!(
                        inner_right.as_ref(),
                        UpdateAssignmentOperand::Literal(Some(value)) if value == b"1"
                    )
            ) && right == b"2"
    ));
}

#[test]
fn update_rows_helper_supports_arithmetic_function_operand() {
    let plan = parse_update_rows_from_statement(
        "update users set login_count = login_count + abs(1) where id = 1",
    )
    .expect("update arithmetic function operand should parse");

    assert_eq!(plan.assignments.len(), 1);
    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Arithmetic {
            left: UpdateAssignmentOperand::ExistingColumn(left),
            op: UpdateArithmeticOp::Add,
            right: UpdateAssignmentOperand::FunctionExpression(expression),
        } if left == "login_count" && expression.eq_ignore_ascii_case("abs(1)")
    ));
}

#[test]
fn update_rows_helper_supports_unary_arithmetic_operand() {
    let plan = parse_update_rows_from_statement(
        "update users set login_count = -login_count + 5 where id = 1",
    )
    .expect("update unary arithmetic operand should parse");

    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Arithmetic {
            left: UpdateAssignmentOperand::Unary {
                op: UnaryArithmeticOp::Minus,
                operand,
            },
            op: UpdateArithmeticOp::Add,
            right: UpdateAssignmentOperand::Literal(Some(right)),
        }
            if matches!(
                operand.as_ref(),
                UpdateAssignmentOperand::ExistingColumn(column) if column == "login_count"
            )
            && right == b"5"
    ));
}

#[test]
fn update_rows_helper_supports_top_level_unary_assignment() {
    let plan = parse_update_rows_from_statement(
        "update users set login_count = -login_count where id = 1",
    )
    .expect("update top-level unary assignment should parse");

    assert!(matches!(
        &plan.assignments[0].value,
        UpdateAssignmentValue::Arithmetic {
            left: UpdateAssignmentOperand::Literal(Some(zero)),
            op: UpdateArithmeticOp::Add,
            right: UpdateAssignmentOperand::Unary {
                op: UnaryArithmeticOp::Minus,
                operand,
            },
        }
            if zero == b"0"
                && matches!(
                    operand.as_ref(),
                    UpdateAssignmentOperand::ExistingColumn(column) if column == "login_count"
                )
    ));
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

#[test]
fn update_rows_helper_parses_complex_join_on_condition() {
    let plan = parse_update_rows_from_statement(
        "update users u join profiles p on u.id = p.user_id and p.name = 'Sam' set u.email = 'sam@example.com'",
    )
    .expect("update with complex join ON should parse");

    assert_eq!(plan.joins.len(), 1);
    assert!(matches!(plan.joins[0].on_condition, SelectCondition::And(_)));
}

#[test]
fn update_rows_helper_parses_update_from_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true from audit where users.id = audit.user_id",
    )
    .expect("update from should parse");

    assert_eq!(plan.relations.len(), 2);
    assert_eq!(plan.joins.len(), 1);
    assert!(matches!(plan.joins[0].on_condition, SelectCondition::And(ref children) if children.is_empty()));
    assert!(plan.where_condition.is_some());
}

#[test]
fn update_rows_helper_parses_returning_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true where id = 1 returning id",
    )
    .expect("update returning should parse");

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
fn update_rows_helper_rejects_returning_expression() {
    let err = parse_update_rows_from_statement(
        "update users set active = true where id = 1 returning id + 1",
    )
    .expect_err("update returning expression should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message.contains("UPDATE RETURNING expression")
    ));
}

#[test]
fn update_rows_helper_parses_order_by_limit_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true where id > 0 order by id desc limit 1",
    )
    .expect("update order by limit should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "id");
    assert!(plan.order_by[0].descending);
    assert_eq!(plan.limit, Some(1));
}

#[test]
fn update_rows_helper_supports_order_by_lower_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by lower(id)",
    )
    .expect("update order by lower expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_lower__:id");
    assert!(!plan.order_by[0].descending);
}

#[test]
fn update_rows_helper_supports_order_by_abs_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by abs(id)",
    )
    .expect("update order by abs expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_abs__:id");
}

#[test]
fn update_rows_helper_supports_order_by_length_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by length(name)",
    )
    .expect("update order by length expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_length__:name");
}

#[test]
fn update_rows_helper_supports_order_by_lcase_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by lcase(name)",
    )
    .expect("update order by lcase expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_lower__:name");
}

#[test]
fn update_rows_helper_supports_order_by_char_length_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by char_length(name)",
    )
    .expect("update order by char_length expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_length__:name");
}

#[test]
fn update_rows_helper_supports_order_by_reverse_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by reverse(name)",
    )
    .expect("update order by reverse expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_reverse__:name");
}

#[test]
fn update_rows_helper_supports_order_by_trim_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by trim(name)",
    )
    .expect("update order by trim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_trim__:name");
}

#[test]
fn update_rows_helper_supports_order_by_ltrim_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by ltrim(name)",
    )
    .expect("update order by ltrim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_ltrim__:name");
}

#[test]
fn update_rows_helper_supports_order_by_rtrim_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by rtrim(name)",
    )
    .expect("update order by rtrim expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_rtrim__:name");
}

#[test]
fn update_rows_helper_supports_order_by_ceil_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by ceil(score)",
    )
    .expect("update order by ceil expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_ceil__:score");
}

#[test]
fn update_rows_helper_supports_order_by_round_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by round(score)",
    )
    .expect("update order by round expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_round__:score");
}

#[test]
fn update_rows_helper_supports_order_by_round_with_scale_expression_clause() {
    let plan = parse_update_rows_from_statement(
        "update users set active = true order by round(score,1)",
    )
    .expect("update order by round with scale expression should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "__order_expr_round_scale__:1:score");
}
