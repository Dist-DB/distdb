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
fn select_read_plan_parses_in_subquery_with_inbuilt_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where id_person in (select lower('UID'))",
    )
    .expect("in-subquery with inbuilt projection should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::InSubquery { .. }))
    ));
}

#[test]
fn select_read_plan_parses_exists_predicates() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where exists (select uid from __person where is_deleted = 0) or not exists (select uid from __person where is_deleted = 1)",
    )
    .expect("exists condition should parse");

    assert!(matches!(plan.where_condition, Some(SelectCondition::Or(_))));
}

#[test]
fn select_read_plan_parses_scalar_subquery_comparison() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where id_person = (select uid from __person where uid = '1')",
    )
    .expect("scalar subquery comparison should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::ScalarSubqueryComparison { .. }
        ))
    ));
}

#[test]
fn select_read_plan_parses_any_subquery_comparison() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where id_person = any ((select uid from __person))",
    )
    .expect("any-subquery comparison should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::AnySubqueryComparison { .. }
        ))
    ));
}

#[test]
fn select_read_plan_parses_all_subquery_comparison() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where id_person = all ((select uid from __person))",
    )
    .expect("all-subquery comparison should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(
            SelectPredicate::AllSubqueryComparison { .. }
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
            escape_char: None,
            ..
        }))
    ));
}

#[test]
fn select_read_plan_parses_like_predicates_with_escape() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where email like 'sam\\_%@example.com' escape '\\\\'",
    )
    .expect("like condition with escape should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::Like {
            negated: false,
            case_insensitive: false,
            escape_char: Some('\\'),
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
fn select_read_plan_parses_not_predicate_forms() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account where email not like 'sam%@example.com' and role not in ('guest') and date_created not between 10 and 20",
    )
    .expect("negated predicate forms should parse");

    assert!(matches!(plan.where_condition, Some(SelectCondition::And(_))));
}

#[test]
fn select_read_plan_parses_limit_and_offset() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account limit 10 offset 2",
    )
    .expect("limit and offset should parse");

    assert_eq!(plan.limit, Some(10));
    assert_eq!(plan.offset, Some(2));
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
fn select_cross_join_parses_join_kind() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u cross join profiles p",
    )
    .expect("cross join select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Cross);
}

#[test]
fn select_join_using_parses_equality_condition() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u inner join profiles p using (id)",
    )
    .expect("join using select should parse");

    assert_eq!(plan.joins.len(), 1);
    assert_eq!(plan.joins[0].kind, SelectJoinKind::Inner);
    assert!(matches!(
        &plan.joins[0].on_condition,
        SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name,
            op: SelectComparisonOp::Eq,
            right_field_name,
        }) if left_field_name == "u.id" && right_field_name == "p.id"
    ));
}

#[test]
fn select_join_on_supports_complex_conditions() {
    let plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u inner join profiles p on u.id = p.user_id and p.name = 'Sam'",
    )
    .expect("join select with complex ON should parse");

    assert_eq!(plan.joins.len(), 1);
    assert!(matches!(
        plan.joins[0].on_condition,
        SelectCondition::And(_)
    ));
}

#[test]
fn select_case_projection_parses_with_alias() {
    let plan = parse_select_read_plan_from_statement(
        "select case when active = 1 then 'yes' else 'no' end as state from users",
    )
    .expect("searched CASE projection should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::Case { output_name, .. }] if output_name == "state"
    ));
}

#[test]
fn select_simple_case_projection_parses_with_operand() {
    let plan = parse_select_read_plan_from_statement(
        "select case active when 1 then 'yes' else 'no' end as state from users",
    )
    .expect("simple CASE projection should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::Case {
            output_name,
            operand: Some(_),
            branches,
            ..
        }] if output_name == "state" && matches!(branches.first(), Some((SelectCaseWhen::Equals(_), _)))
    ));
}

#[test]
fn select_case_projection_parses_inbuilt_function_values() {
    let plan = parse_select_read_plan_from_statement(
        "select case when active = 1 then upper('yes') else lower('NO') end as state from users",
    )
    .expect("CASE projection with inbuilt function values should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::Case { branches, else_value, .. }]
            if matches!(branches.first(), Some((_, SelectExpression::InbuiltFunction { .. })))
                && matches!(else_value, Some(SelectExpression::InbuiltFunction { .. }))
    ));
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

#[test]
fn select_passthrough_derived_wrapper_parses() {
    let plan = parse_select_read_plan_from_statement(
        "select * from (select uid from __account where is_deleted = 0) d",
    )
    .expect("passthrough derived wrapper should parse");

    assert_eq!(plan.table_id, "__account");
    assert_eq!(plan.relations.len(), 1);
    assert!(plan.where_condition.is_some());
}

#[test]
fn select_passthrough_derived_wrapper_parses_qualified_wildcard() {
    let plan = parse_select_read_plan_from_statement(
        "select d.* from (select uid from __account where is_deleted = 0) d",
    )
    .expect("passthrough derived wrapper with qualified wildcard should parse");

    assert_eq!(plan.table_id, "__account");
    assert_eq!(plan.relations.len(), 1);
    assert!(plan.where_condition.is_some());
}

#[test]
fn select_passthrough_derived_wrapper_rewrites_outer_where() {
    let plan = parse_select_read_plan_from_statement(
        "select * from (select id, email from users) d where d.id = 1",
    )
    .expect("passthrough derived wrapper with outer where should parse");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::Comparison { .. }))
    ));
}

#[test]
fn select_passthrough_derived_wrapper_composes_outer_limit_offset() {
    let plan = parse_select_read_plan_from_statement(
        "select * from (select uid from __account limit 5 offset 2) d limit 3 offset 1",
    )
    .expect("passthrough derived wrapper with outer limit/offset should parse");

    assert_eq!(plan.limit, Some(3));
    assert_eq!(plan.offset, Some(3));
}

#[test]
fn select_passthrough_derived_wrapper_rewrites_outer_projection_with_aliases() {
    let plan = parse_select_read_plan_from_statement(
        "select d.id as user_id, d.email from (select id, email from users) d",
    )
    .expect("passthrough derived wrapper with outer projection aliases should parse");

    assert_eq!(plan.projection_is_wildcard, false);
    assert_eq!(plan.projection, Some(vec!["id".to_string(), "email".to_string()]));
    assert_eq!(
        plan.projection_items,
        vec![
            SelectProjectionItem::Column {
                field_name: "id".to_string(),
                output_name: "user_id".to_string(),
            },
            SelectProjectionItem::Column {
                field_name: "email".to_string(),
                output_name: "email".to_string(),
            },
        ]
    );
}

#[test]
fn select_passthrough_derived_wrapper_rejects_unknown_outer_projection_column() {
    let err = parse_select_read_plan_from_statement(
        "select d.missing from (select id, email from users) d",
    )
    .expect_err("unknown outer projection column should fail");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message.contains("references unknown projected column 'missing'")
    ));
}
