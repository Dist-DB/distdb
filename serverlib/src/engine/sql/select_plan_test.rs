use super::*;

#[test]
fn select_projection_returns_requested_columns() {
    let projection = parse_select_projection_from_statement("SELECT uid, id_person FROM __account")
        .expect("projection should parse");

    assert_eq!(
        projection,
        Some(vec!["uid".to_string(), "id_person".to_string()])
    );
}

#[test]
fn select_star_projection_returns_none() {
    let projection = parse_select_projection_from_statement("SELECT * FROM __account")
        .expect("projection should parse");

    assert_eq!(projection, None);
}

#[test]
fn select_read_plan_parses_or_and_is_null() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where uid = '1' or id_device is null",
    )
    .expect("select read plan should parse");

    assert_eq!(plan.table_id, "__account");
    assert!(!plan.is_explain);
    assert!(matches!(plan.where_condition, Some(SelectCondition::Or(_))));
}

#[test]
fn select_read_plan_parses_in_and_not_equal() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where role in ('user','admin') and is_deleted <> 1",
    )
    .expect("select read plan should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::And(_))
    ));
}

#[test]
fn select_read_plan_parses_parenthesized_nested_groups() {
    let plan = parse_select_read_plan_from_statement(
            "select uid from __account where ((uid = '1' or (role = 'admin' and id_device is null)) and ((is_deleted <> 1)))",
        )
        .expect("select read plan with nested parentheses should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::And(_))
    ));
}

#[test]
fn select_read_plan_parses_parenthesized_operands() {
    let plan =
        parse_select_read_plan_from_statement("select uid from __account where ((uid)) = (('1'))")
            .expect("select read plan with parenthesized operands should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::Comparison { .. }
        ))
    ));
}

#[test]
fn select_read_plan_parses_between_condition() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where date_created between 10 and 20",
    )
    .expect("between condition should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::And(_))
    ));
}

#[test]
fn select_read_plan_parses_in_subquery_condition() {
    let plan = parse_select_read_plan_from_statement(
            "select uid from __account where id_person in (select uid from __person where is_deleted = 0)",
        )
        .expect("in-subquery condition should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::InSubquery { .. }
        ))
    ));
}

#[test]
fn select_read_plan_parses_inbuilt_function_literals() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where uid = CONCAT('user', '001')",
    )
    .expect("where inbuilt function literal should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::Comparison { .. }
        ))
    ));
}

#[test]
fn select_read_plan_parses_like_predicates() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where email like 'sam%@example.com'",
    )
    .expect("like condition should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::Like {
            negated: false,
            case_insensitive: false,
            ..
        }))
    ));
}

#[test]
fn select_read_plan_parses_regex_predicates() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where email regexp '^sam.*@example\\.com$'",
    )
    .expect("regex condition should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::Regex {
            negated: false,
            case_insensitive: false,
            ..
        }))
    ));
}

#[test]
fn select_read_plan_parses_inbuilt_function_projection_with_from() {
    let plan =
        parse_select_read_plan_from_statement("select unixtimestamp() as time from __account")
            .expect("function projection with from should parse");

    assert_eq!(plan.table_id, "__account");
    assert_eq!(plan.projection_items.len(), 1);
    assert!(matches!(
        plan.projection_items[0],
        SelectProjectionItem::InbuiltFunction { .. }
    ));
}

#[test]
fn select_read_plan_parses_inbuilt_function_projection_without_from() {
    let plan = parse_select_read_plan_from_statement("select unixtimestamp() as time")
        .expect("function projection without from should parse");

    assert!(plan.table_id.is_empty());
    assert_eq!(plan.projection_items.len(), 1);
    assert!(matches!(
        plan.projection_items[0],
        SelectProjectionItem::InbuiltFunction { .. }
    ));
}

#[test]
fn select_alias_qualified_projection_is_valid() {
    let plan = parse_select_read_plan_from_statement("select ac.uid from __account as ac")
        .expect("alias-qualified projection should parse");

    assert_eq!(plan.projection, Some(vec!["uid".to_string()]));
}

#[test]
fn select_unknown_alias_in_projection_is_rejected() {
    let err = parse_select_read_plan_from_statement("select zz.uid from __account as ac")
        .expect_err("unknown alias should fail parsing");

    assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
}

#[test]
fn select_unknown_alias_in_where_is_rejected() {
    let err =
        parse_select_read_plan_from_statement("select uid from __account as ac where zz.uid = '1'")
            .expect_err("unknown alias in where should fail parsing");

    assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
}

#[test]
fn select_alias_qualified_wildcard_is_valid() {
    let plan = parse_select_read_plan_from_statement("select ac.* from __account as ac")
        .expect("alias-qualified wildcard should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::Wildcard { relation }]
            if relation.as_deref() == Some("ac")
    ));
    assert!(!plan.projection_is_wildcard);
}

#[test]
fn select_join_unqualified_wildcard_is_valid() {
    let plan = parse_select_read_plan_from_statement(
        "select * from users u inner join profiles p on u.id = p.user_id",
    )
    .expect("unqualified wildcard join should parse");

    assert_eq!(plan.relations.len(), 2);
    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::Wildcard { relation }] if relation.is_none()
    ));
}

#[test]
fn select_unknown_alias_qualified_wildcard_is_rejected() {
    let err = parse_select_read_plan_from_statement("select zz.* from __account as ac")
        .expect_err("unknown alias-qualified wildcard should fail parsing");

    assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
}

#[test]
fn select_inner_join_parses_relations_and_on_fields() {
    let plan = parse_select_read_plan_from_statement(
            "select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1",
        )
        .expect("join select should parse");

    assert_eq!(plan.relations.len(), 2);
    assert_eq!(plan.joins.len(), 1);
    assert_eq!(
        plan.projection,
        Some(vec!["u.email".to_string(), "p.name".to_string()])
    );
    assert!(matches!(
        &plan.joins[0].on_condition,
        SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name,
            op: SelectComparisonOp::Eq,
            right_field_name,
        }) if left_field_name == "u.id" && right_field_name == "p.user_id"
    ));
}

#[test]
fn select_join_requires_qualified_projection_columns() {
    let err = parse_select_read_plan_from_statement(
        "select email from users u inner join profiles p on u.id = p.user_id",
    )
    .expect_err("join projection without qualifiers should fail");

    assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
}

#[test]
fn select_left_join_parses_join_kind() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u left join profiles p on u.id = p.user_id",
    )
    .expect("left join select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Left);
}

#[test]
fn select_left_outer_join_parses_join_kind() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u left outer join profiles p on u.id = p.user_id",
    )
    .expect("left outer join select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Left);
}

#[test]
fn select_right_outer_join_parses_join_kind() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u right outer join profiles p on u.id = p.user_id",
    )
    .expect("right outer join select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Right);
}

#[test]
fn select_full_outer_join_parses_join_kind() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u full outer join profiles p on u.id = p.user_id",
    )
    .expect("full outer join select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Full);
}

#[test]
fn select_multiple_joins_parse_in_order() {
    let plan = parse_select_read_plan_from_statement(
            "select u.email, p.name, t.label from users u inner join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where u.id = 1",
        )
        .expect("multi-join select should parse");

    assert_eq!(plan.relations.len(), 3);
    assert_eq!(plan.joins.len(), 2);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Inner);
    assert_eq!(plan.joins[1].kind, SelectJoinKind::Left);
    assert!(!plan.is_explain);
    assert!(plan.where_condition.is_some());
}

#[test]
fn explain_select_multiple_joins_sets_explain_flag() {
    let plan = parse_select_read_plan_from_statement(
            "explain select u.email, p.name, t.label from users u inner join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where u.id = 1",
        )
        .expect("explain multi-join select should parse");

    assert!(plan.is_explain);
    assert_eq!(plan.relations.len(), 3);
    assert_eq!(plan.joins.len(), 2);
}
