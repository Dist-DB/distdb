use super::*;
use crate::engine::sql::MutationReturningItem;

#[test]
fn insert_values_helper_extracts_rows() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email, is_active, nickname) values (1, 'sam@example.com', true, null)",
    )
    .expect("insert values should parse");

    assert_eq!(plan.table_id, "users");
    assert!(!plan.ignore);
    assert!(!plan.replace_into);
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
fn replace_into_values_sets_replace_flag() {
    let plan = parse_insert_rows_from_statement(
        "replace into users (id, email) values (1, 'sam@example.com')",
    )
    .expect("replace into values should parse");

    assert_eq!(plan.table_id, "users");
    assert!(!plan.ignore);
    assert!(plan.replace_into);
    assert_eq!(plan.columns, vec!["id", "email"]);
}

#[test]
fn insert_default_values_parses_as_empty_values_row() {
    let plan = parse_insert_rows_from_statement("insert into users default values")
        .expect("insert default values should parse");

    assert_eq!(plan.table_id, "users");
    assert!(plan.columns.is_empty());
    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows, vec![Vec::<Option<Vec<u8>>>::new()]);
}

#[test]
fn insert_default_values_with_column_list_parses_as_default_row() {
    let plan = parse_insert_rows_from_statement("insert into users (id, email) default values")
        .expect("insert default values with columns should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.columns, vec!["id", "email"]);
    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows, vec![vec![None, None]]);
}

#[test]
fn insert_set_parses_as_values_insert() {
    let plan = parse_insert_rows_from_statement(
        "insert into users set id = 1, email = 'sam@example.com'",
    )
    .expect("insert set should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.columns, vec!["id", "email"]);
    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some(b"1".to_vec()));
    assert_eq!(rows[0][1], Some(b"sam@example.com".to_vec()));
}

#[test]
fn insert_set_with_on_duplicate_and_returning_parses() {
    let plan = parse_insert_rows_from_statement(
        "insert into users set id = 1, email = 'sam@example.com' on duplicate key update email = values(email) returning id, email",
    )
    .expect("insert set with on duplicate and returning should parse");

    assert_eq!(plan.columns, vec!["id", "email"]);
    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert!(plan.returning.is_some());
}

#[test]
fn insert_set_with_qualified_targets_parses_to_leaf_columns() {
    let plan = parse_insert_rows_from_statement(
        "insert into users set app.users.id = 1, users.email = 'sam@example.com'",
    )
    .expect("insert set with qualified targets should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.columns, vec!["id", "email"]);

    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };
    assert_eq!(rows, vec![vec![Some(b"1".to_vec()), Some(b"sam@example.com".to_vec())]]);
}

#[test]
fn insert_ignore_sets_ignore_flag() {
    let plan = parse_insert_rows_from_statement(
        "insert ignore into users (id, email) values (1, 'sam@example.com')",
    )
    .expect("insert ignore should parse");

    assert!(plan.ignore);
}

#[test]
fn insert_low_priority_modifier_parses_in_compat_mode() {
    let plan = parse_insert_rows_from_statement(
        "insert low_priority into users (id, email) values (1, 'sam@example.com')",
    )
    .expect("insert low_priority should parse in compatibility mode");

    assert!(!plan.ignore);
    assert_eq!(plan.table_id, "users");
}

#[test]
fn insert_high_priority_modifier_parses_in_compat_mode() {
    let plan = parse_insert_rows_from_statement(
        "insert high_priority into users (id, email) values (1, 'sam@example.com')",
    )
    .expect("insert high_priority should parse in compatibility mode");

    assert!(!plan.ignore);
    assert_eq!(plan.table_id, "users");
}

#[test]
fn insert_ignore_low_priority_modifier_parses_in_compat_mode() {
    let plan = parse_insert_rows_from_statement(
        "insert ignore low_priority into users (id, email) values (1, 'sam@example.com')",
    )
    .expect("insert ignore low_priority should parse in compatibility mode");

    assert!(plan.ignore);
    assert_eq!(plan.table_id, "users");
}

#[test]
fn insert_on_duplicate_key_update_parses_assignments() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') on duplicate key update email = 'new@example.com'",
    )
    .expect("insert with on duplicate key update should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "email");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Literal(Some(value))
            if value == b"new@example.com"
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_values_column_reference() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') on duplicate key update email = values(email)",
    )
    .expect("insert on duplicate values(column) should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "email");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::IncomingColumn(column)
            if column == "email"
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_existing_column_reference() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') on duplicate key update email = email",
    )
    .expect("insert on duplicate existing-column reference should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "email");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::ExistingColumn(column)
            if column == "email"
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_arithmetic_assignment() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, login_count) values (1, 1) on duplicate key update login_count = login_count + values(login_count)",
    )
    .expect("insert on duplicate arithmetic assignment should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "login_count");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Arithmetic {
            left: InsertOnDuplicateAssignmentOperand::ExistingColumn(left),
            op: InsertOnDuplicateArithmeticOp::Add,
            right: InsertOnDuplicateAssignmentOperand::IncomingColumn(right),
        } if left == "login_count" && right == "login_count"
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_nested_arithmetic_assignment() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, login_count) values (1, 1) on duplicate key update login_count = (login_count + values(login_count)) * 2",
    )
    .expect("insert on duplicate nested arithmetic assignment should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "login_count");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Arithmetic {
            left,
            op: InsertOnDuplicateArithmeticOp::Multiply,
            right: InsertOnDuplicateAssignmentOperand::Literal(Some(right)),
        }
            if matches!(
                left,
                InsertOnDuplicateAssignmentOperand::Arithmetic {
                    left: inner_left,
                    op: InsertOnDuplicateArithmeticOp::Add,
                    right: inner_right,
                }
                    if matches!(
                        inner_left.as_ref(),
                        InsertOnDuplicateAssignmentOperand::ExistingColumn(column)
                            if column == "login_count"
                    )
                    && matches!(
                        inner_right.as_ref(),
                        InsertOnDuplicateAssignmentOperand::IncomingColumn(column)
                            if column == "login_count"
                    )
            ) && right == b"2"
    ));
}

#[test]
fn insert_on_duplicate_key_update_supports_arithmetic_function_operand() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, login_count) values (1, 1) on duplicate key update login_count = login_count + abs(1)",
    )
    .expect("insert on duplicate arithmetic function operand should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Arithmetic {
            left: InsertOnDuplicateAssignmentOperand::ExistingColumn(left),
            op: InsertOnDuplicateArithmeticOp::Add,
            right: InsertOnDuplicateAssignmentOperand::FunctionExpression(expression),
        } if left == "login_count" && expression.eq_ignore_ascii_case("abs(1)")
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_unary_arithmetic_operand() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, login_count) values (1, 1) on duplicate key update login_count = -login_count + values(login_count)",
    )
    .expect("insert on duplicate unary arithmetic operand should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Arithmetic {
            left: InsertOnDuplicateAssignmentOperand::Unary {
                op: UnaryArithmeticOp::Minus,
                operand,
            },
            op: InsertOnDuplicateArithmeticOp::Add,
            right: InsertOnDuplicateAssignmentOperand::IncomingColumn(column),
        }
            if matches!(
                operand.as_ref(),
                InsertOnDuplicateAssignmentOperand::ExistingColumn(left) if left == "login_count"
            )
            && column == "login_count"
    ));
}

#[test]
fn insert_on_duplicate_key_update_parses_top_level_unary_assignment() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, login_count) values (1, 1) on duplicate key update login_count = -values(login_count)",
    )
    .expect("insert on duplicate top-level unary assignment should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Arithmetic {
            left: InsertOnDuplicateAssignmentOperand::Literal(Some(zero)),
            op: InsertOnDuplicateArithmeticOp::Add,
            right: InsertOnDuplicateAssignmentOperand::Unary {
                op: UnaryArithmeticOp::Minus,
                operand,
            },
        }
            if zero == b"0"
                && matches!(
                    operand.as_ref(),
                    InsertOnDuplicateAssignmentOperand::IncomingColumn(column)
                        if column == "login_count"
                )
    ));
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

#[test]
fn insert_values_supports_unary_signed_numbers() {
    let plan = parse_insert_rows_from_statement(
        "insert into geo (lat, lon, z) values (-1307825, +4512, -3)",
    )
    .expect("insert values with signed numeric literals should parse");

    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some(b"-1307825".to_vec()));
    assert_eq!(rows[0][1], Some(b"4512".to_vec()));
    assert_eq!(rows[0][2], Some(b"-3".to_vec()));
}

#[test]
fn insert_values_supports_placeholder_literal_tokens() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (?, :email)",
    )
    .expect("insert values with placeholders should parse");

    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Some(b"?".to_vec()));
    assert_eq!(rows[0][1], Some(b":email".to_vec()));
}

#[test]
fn insert_on_duplicate_key_update_supports_placeholder_literal_assignment() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') on duplicate key update email = :incoming_email",
    )
    .expect("insert on duplicate placeholder literal assignment should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "email");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::Literal(Some(value)) if value == b":incoming_email"
    ));
}

#[test]
fn insert_values_supports_default_keyword_literal() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (default, 'sam@example.com')",
    )
    .expect("insert values with DEFAULT keyword should parse");

    let InsertRowsSource::Values(rows) = plan.source else {
        panic!("expected VALUES source");
    };

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], None);
    assert_eq!(rows[0][1], Some(b"sam@example.com".to_vec()));
}

#[test]
fn insert_on_duplicate_key_update_allows_fully_qualified_target() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') on duplicate key update app.users.email = values(email)",
    )
    .expect("insert on duplicate fully qualified assignment target should parse");

    assert_eq!(plan.on_duplicate_update.len(), 1);
    assert_eq!(plan.on_duplicate_update[0].field_name, "email");
    assert!(matches!(
        &plan.on_duplicate_update[0].value,
        InsertOnDuplicateAssignmentValue::IncomingColumn(column)
            if column == "email"
    ));
}

#[test]
fn insert_values_parses_returning_clause() {
    let plan = parse_insert_rows_from_statement(
        "insert into users (id, email) values (1, 'sam@example.com') returning id, email as login",
    )
    .expect("insert values with returning should parse");

    let returning = plan.returning.expect("returning plan should be present");
    assert_eq!(returning.len(), 2);
    assert!(matches!(
        &returning[0],
        MutationReturningItem::Column {
            field_name,
            output_name,
        } if field_name == "id" && output_name == "id"
    ));
    assert!(matches!(
        &returning[1],
        MutationReturningItem::Column {
            field_name,
            output_name,
        } if field_name == "email" && output_name == "login"
    ));
}
