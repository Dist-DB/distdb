use super::*;

fn set_query_branch_plans(steps: &[SelectSetQueryStep]) -> Vec<&SelectReadPlan> {
    steps
        .iter()
        .filter_map(|step| match step {
            SelectSetQueryStep::Branch(plan) => Some(plan),
            SelectSetQueryStep::BoundaryOperation(_) => None,
        })
        .collect::<Vec<_>>()
}

fn set_query_boundary_operations(steps: &[SelectSetQueryStep]) -> Vec<SelectSetBoundaryOp> {
    steps
        .iter()
        .filter_map(|step| match step {
            SelectSetQueryStep::Branch(_) => None,
            SelectSetQueryStep::BoundaryOperation(operation) => Some(*operation),
        })
        .collect::<Vec<_>>()
}

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
fn recursive_cte_parses_into_seed_and_recursive_plans() {
    let plan = parse_select_read_plan_from_statement(
        "with recursive chain as (select parent_id, child_id from edges where parent_id = 1 union select e.parent_id, e.child_id from edges e join chain c on e.parent_id = c.child_id) select child_id from chain",
    )
    .expect("recursive cte should parse");

    assert_eq!(plan.ctes.len(), 1);
    assert_eq!(plan.ctes[0].table_id, "chain");
    assert!(plan.ctes[0].recursive_read_plan.is_some());
}

#[test]
fn recursive_cte_union_all_parses_and_marks_union_all_mode() {
    let plan = parse_select_read_plan_from_statement(
        "with recursive chain as (select parent_id, child_id from edges where parent_id = 1 union all select e.parent_id, e.child_id from edges e join chain c on e.parent_id = c.child_id) select child_id from chain",
    )
    .expect("recursive cte union all should parse");

    assert_eq!(plan.ctes.len(), 1);
    assert!(plan.ctes[0].recursive_read_plan.is_some());
    assert!(plan.ctes[0].recursive_union_all);
}

#[test]
fn select_star_projection_returns_none() {
    let projection = parse_select_projection_from_statement("SELECT * FROM __account")
        .expect("projection should parse");

    assert_eq!(projection, None);
}

#[test]
fn select_distinct_returns_explicit_unsupported_error() {
    let plan = parse_select_read_plan_from_statement("select distinct id from users")
        .expect("distinct select should parse");

    assert!(plan.distinct);
}

#[test]
fn select_group_by_returns_explicit_unsupported_error() {
    let plan = parse_select_read_plan_from_statement("select id from users group by id")
        .expect("group by select should parse in first-pass mode");

    assert_eq!(plan.group_by, vec!["id".to_string()]);
    assert!(plan.distinct);
}

#[test]
fn select_order_by_returns_explicit_unsupported_error() {
    let plan = parse_select_read_plan_from_statement("select id from users order by id")
        .expect("order by select should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "id");
    assert!(!plan.order_by[0].descending);
}

#[test]
fn select_projection_only_order_by_ordinal_is_supported() {
    let plan = parse_select_read_plan_from_statement("select now() as ts order by 1 desc")
        .expect("projection-only order by ordinal should parse");

    assert_eq!(plan.order_by.len(), 1);
    assert_eq!(plan.order_by[0].field_name, "ts");
    assert!(plan.order_by[0].descending);
}

#[test]
fn select_projection_only_order_by_unknown_alias_is_rejected() {
    let error = parse_select_read_plan_from_statement("select now() as ts order by missing")
        .expect_err("projection-only order by unknown alias should be rejected");

    assert!(matches!(
        error,
        SqlParseError::UnsupportedStatement(message)
            if message == "ORDER BY without FROM references unknown output field 'missing'"
    ));
}

#[test]
fn create_view_dependency_extraction_collects_base_table() {

    let dependencies = parse_create_view_dependencies_from_sql(
        "create view users_v as select * from users",
    )
    .expect("create view dependencies should parse");

    assert_eq!(dependencies, vec!["users".to_string()]);

}

#[test]
fn union_select_parses_branch_plans_and_quantifier() {

    let (steps, order_by, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users",
    )
    .expect("union all should parse branch plans");

    let branch_plans = set_query_branch_plans(&steps);
    let boundary_operations = set_query_boundary_operations(&steps);

    assert_eq!(branch_plans.len(), 2);
    assert_eq!(boundary_operations, vec![SelectSetBoundaryOp::UnionAll]);
    assert!(order_by.is_empty());
    assert!(limit_by.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    assert_eq!(limit, None);
    assert_eq!(offset, None);
    assert_eq!(branch_plans[0].table_id, "users");
    assert_eq!(branch_plans[1].table_id, "archived_users");

}

#[test]
fn union_select_parses_mixed_quantifiers_and_query_level_windowing() {

    let (steps, order_by, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) =
        parse_union_select_read_plans_from_statement(
            "select id from users union all select id from archived_users union select id from users order by id desc limit 5 offset 1",
        )
        .expect("mixed union quantifiers should parse");

    let branch_plans = set_query_branch_plans(&steps);
    let boundary_operations = set_query_boundary_operations(&steps);

    assert_eq!(branch_plans.len(), 3);
    assert_eq!(
        boundary_operations,
        vec![
            SelectSetBoundaryOp::UnionAll,
            SelectSetBoundaryOp::UnionDistinct,
        ]
    );
    assert_eq!(order_by.len(), 1);
    assert_eq!(order_by[0].field_name, "id");
    assert!(order_by[0].descending);
    assert!(limit_by.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    assert_eq!(limit, Some(5));
    assert_eq!(offset, Some(1));
    
}

#[test]
fn union_select_parses_query_level_limit_by_plan() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id limit 1 by id",
    )
    .expect("union query limit by should parse");

    assert!(limit.is_none());
    assert!(offset.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    let limit_by = limit_by.expect("limit by plan should be present");
    assert_eq!(limit_by.per_key_limit, 1);
    assert_eq!(limit_by.fields, vec!["id".to_string()]);
}

#[test]
fn union_select_parses_query_level_limit_by_with_offset() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id limit 1 offset 1 by id",
    )
    .expect("union query limit by + offset should parse");

    assert!(limit.is_none());
    assert_eq!(offset, Some(1));
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    let limit_by = limit_by.expect("limit by should be present");
    assert_eq!(limit_by.per_key_limit, 1);
    assert_eq!(limit_by.fields, vec!["id".to_string()]);
}

#[test]
fn except_select_parses_boundary_operation() {
    let (steps, _, _, _, _, _, _, _) = parse_union_select_read_plans_from_statement(
        "select id from users except select id from archived_users",
    )
    .expect("except query should parse");

    let branch_plans = set_query_branch_plans(&steps);
    let boundary_operations = set_query_boundary_operations(&steps);

    assert_eq!(branch_plans.len(), 2);
    assert_eq!(
        boundary_operations,
        vec![SelectSetBoundaryOp::ExceptDistinct]
    );
}

#[test]
fn intersect_select_parses_boundary_operation() {
    let (steps, _, _, _, _, _, _, _) = parse_union_select_read_plans_from_statement(
        "select id from users intersect select id from archived_users",
    )
    .expect("intersect query should parse");

    let branch_plans = set_query_branch_plans(&steps);
    let boundary_operations = set_query_boundary_operations(&steps);

    assert_eq!(branch_plans.len(), 2);
    assert_eq!(
        boundary_operations,
        vec![SelectSetBoundaryOp::IntersectDistinct]
    );
}

#[test]
fn mixed_set_operators_preserve_parser_precedence_order() {
    let (steps, _, _, _, _, _, _, _) = parse_union_select_read_plans_from_statement(
        "select id from users union select id from archived_users except select id from users",
    )
    .expect("mixed set operators should parse");

    assert_eq!(set_query_branch_plans(&steps).len(), 3);
    assert_eq!(
        set_query_boundary_operations(&steps),
        vec![
            SelectSetBoundaryOp::UnionDistinct,
            SelectSetBoundaryOp::ExceptDistinct,
        ]
    );
}

#[test]
fn union_select_parses_order_by_ordinal_position() {
    let (_, order_by, _, _, _, _, _, _) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by 1 desc",
    )
    .expect("union order by ordinal should parse");

    assert_eq!(order_by.len(), 1);
    assert_eq!(order_by[0].field_name, "__union_order_by_ordinal__1");
    assert!(order_by[0].descending);
}

#[test]
fn union_select_with_cte_propagates_ctes_to_branch_plans() {
    let (steps, _, _, _, _, _, _, _) = parse_union_select_read_plans_from_statement(
        "with staged as (select id from users) select id from staged union all select id from staged",
    )
    .expect("union with cte should parse");

    let branch_plans = set_query_branch_plans(&steps);
    let boundary_operations = set_query_boundary_operations(&steps);

    assert_eq!(branch_plans.len(), 2);
    assert_eq!(boundary_operations, vec![SelectSetBoundaryOp::UnionAll]);
    assert_eq!(branch_plans[0].ctes.len(), 1);
    assert_eq!(branch_plans[1].ctes.len(), 1);
    assert_eq!(branch_plans[0].ctes[0].table_id, "staged");
}

#[test]
fn select_with_cte_parses_cte_plan() {
    let plan = parse_select_read_plan_from_statement(
        "with staged as (select id from users) select id from staged",
    )
    .expect("cte select should parse");

    assert_eq!(plan.ctes.len(), 1);
    assert_eq!(plan.ctes[0].table_id, "staged");
    assert_eq!(plan.ctes[0].read_plan.table_id, "users");
    assert_eq!(plan.table_id, "staged");
}

#[test]
fn select_group_by_having_combines_having_into_filter() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users group by id having id = 1",
    )
    .expect("group by having should parse");

    assert_eq!(plan.group_by, vec!["id".to_string()]);
    assert!(plan.having_condition.is_some());
    assert!(plan.where_condition.is_some());
}

#[test]
fn select_with_window_clause_sets_window_flag() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users window w as (partition by id)",
    )
    .expect("window clause should parse");

    assert!(plan.has_window_clause);
    assert_eq!(plan.named_windows.len(), 1);
    assert_eq!(plan.named_windows[0].0.value, "w");
    assert!(matches!(
        plan.named_windows[0].1,
        sqlparser::ast::NamedWindowExpr::WindowSpec(_)
    ));
}

#[test]
fn select_row_number_over_empty_window_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select row_number() over () as rn from users",
    )
    .expect("row_number window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, .. }]
            if output_name == "rn"
    ));
}

#[test]
fn select_row_number_over_order_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select row_number() over (order by email desc) as rn from users",
    )
    .expect("row_number window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "rn"
                && function
                    .over
                    .as_ref()
                    .and_then(|window| match window {
                        sqlparser::ast::WindowType::WindowSpec(spec) => Some(spec.order_by.len()),
                        _ => None,
                    })
                    == Some(1)
    ));
}

#[test]
fn select_row_number_over_named_window_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select row_number() over w as rn from users window w as (partition by id order by email desc)",
    )
    .expect("row_number window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "rn"
                && matches!(function.over.as_ref(), Some(sqlparser::ast::WindowType::NamedWindow(_)))
    ));
}

#[test]
fn select_row_number_over_partition_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select row_number() over (partition by user_id order by id desc) as rn from profiles",
    )
    .expect("row_number window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "rn"
                && function
                    .over
                    .as_ref()
                    .and_then(|window| match window {
                        sqlparser::ast::WindowType::WindowSpec(spec) => {
                            Some((spec.partition_by.len(), spec.order_by.len()))
                        }
                        _ => None,
                    })
                    == Some((1, 1))
    ));
}

#[test]
fn select_sum_over_named_window_with_frame_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select sum(id) over (w rows between unbounded preceding and current row) as running_sum from profiles window w as (partition by user_id order by id)",
    )
    .expect("sum window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "running_sum"
                && function.name.to_string().eq_ignore_ascii_case("sum")
                && function
                    .over
                    .as_ref()
                    .and_then(|window| match window {
                        sqlparser::ast::WindowType::WindowSpec(spec) => {
                            Some((spec.window_name.is_some(), spec.window_frame.is_some()))
                        }
                        _ => None,
                    })
                    == Some((true, true))
    ));
}

#[test]
fn select_count_over_named_window_with_frame_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select count(id) over (w rows between unbounded preceding and current row) as running_count from profiles window w as (partition by user_id order by id)",
    )
    .expect("count window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "running_count"
                && function.name.to_string().eq_ignore_ascii_case("count")
                && function
                    .over
                    .as_ref()
                    .and_then(|window| match window {
                        sqlparser::ast::WindowType::WindowSpec(spec) => {
                            Some((spec.window_name.is_some(), spec.window_frame.is_some()))
                        }
                        _ => None,
                    })
                    == Some((true, true))
    ));
}

#[test]
fn select_rank_over_order_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select rank() over (order by email) as rnk from users",
    )
    .expect("rank window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "rnk"
                && function.name.to_string().eq_ignore_ascii_case("rank")
    ));
}

#[test]
fn select_dense_rank_over_order_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select dense_rank() over (order by email) as drnk from users",
    )
    .expect("dense_rank window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "drnk"
                && function.name.to_string().eq_ignore_ascii_case("dense_rank")
    ));
}

#[test]
fn select_lag_over_order_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select lag(id, 1, 0) over (order by id) as prev_id from users",
    )
    .expect("lag window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "prev_id"
                && function.name.to_string().eq_ignore_ascii_case("lag")
    ));
}

#[test]
fn select_lead_over_order_by_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select lead(id, 1) over (order by id) as next_id from users",
    )
    .expect("lead window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "next_id"
                && function.name.to_string().eq_ignore_ascii_case("lead")
    ));
}

#[test]
fn select_with_sql_no_cache_modifier_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select sql_no_cache id from users",
    )
    .expect("select sql_no_cache should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_distinct_with_sql_calc_found_rows_modifier_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select distinct sql_calc_found_rows id from users",
    )
    .expect("select distinct sql_calc_found_rows should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert!(plan.distinct);
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_all_with_sql_small_result_modifier_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select all sql_small_result id from users",
    )
    .expect("select all sql_small_result should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_with_optimizer_hint_comment_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select /*+ max_execution_time(500) */ id from users",
    )
    .expect("select optimizer hint comment should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_with_use_index_hint_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users use index (idx_users_id)",
    )
    .expect("select use index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_with_ignore_index_hint_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users ignore index (idx_users_id)",
    )
    .expect("select ignore index hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_with_force_key_hint_parses_in_compat_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users force key for join (idx_users_id)",
    )
    .expect("select force key hint should parse in compatibility mode");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
}

#[test]
fn select_hint_like_text_inside_string_literal_is_preserved() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users where note = 'use index (idx_users_id)'",
    )
    .expect("hint-like string literal should parse without normalization side effects");

    assert!(matches!(
        plan.where_condition,
        Some(SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op: SelectComparisonOp::Eq,
            value,
        })) if field_name == "note" && value == b"use index (idx_users_id)"
    ));
}

#[test]
fn select_avg_over_named_window_with_frame_parses_as_window_projection() {
    let plan = parse_select_read_plan_from_statement(
        "select avg(id) over (w rows between unbounded preceding and current row) as running_avg from profiles window w as (partition by user_id order by id)",
    )
    .expect("avg window select should parse");

    assert!(matches!(
        plan.projection_items.as_slice(),
        [SelectProjectionItem::WindowFunction { output_name, function }]
            if output_name == "running_avg"
                && function.name.to_string().eq_ignore_ascii_case("avg")
    ));
}

#[test]
fn select_qualify_is_stored_for_post_window_filtering() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users qualify id = 1",
    )
    .expect("qualify select should parse");

    assert!(plan.where_condition.is_none());
    assert!(plan.qualify_condition.is_some());
}

#[test]
fn select_for_update_parses_in_first_pass_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users for update",
    )
    .expect("for update select should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
    assert_eq!(plan.lock_mode, SelectLockMode::ForUpdate);
}

#[test]
fn select_for_share_parses_in_first_pass_mode() {
    let plan = parse_select_read_plan_from_statement(
        "select id from users for share",
    )
    .expect("for share select should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.projection, Some(vec!["id".to_string()]));
    assert_eq!(plan.lock_mode, SelectLockMode::ForShare);
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
            case_insensitive: true,
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
            case_insensitive: true,
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
fn select_read_plan_parses_top_as_limit() {
    let plan = parse_select_read_plan_from_statement(
        "select top 2 uid from __account order by uid",
    )
    .expect("top should parse as limit");

    assert_eq!(plan.limit, Some(2));
    assert!(plan.limit_by.is_none());
}

#[test]
fn select_read_plan_parses_top_percent() {
    let plan = parse_select_read_plan_from_statement(
        "select top 50 percent uid from __account order by uid",
    )
    .expect("top percent should parse");

    assert_eq!(plan.top_percent, Some(50));
    assert_eq!(plan.limit, None);
}

#[test]
fn select_read_plan_parses_top_percent_with_ties() {
    let plan = parse_select_read_plan_from_statement(
        "select top 50 percent with ties uid from __account order by uid",
    )
    .expect("top percent with ties should parse");

    assert!(plan.top_percent.is_none());
    assert_eq!(plan.top_percent_with_ties, Some(50));
    assert_eq!(plan.limit, None);
}

#[test]
fn select_read_plan_parses_top_with_ties() {
    let plan = parse_select_read_plan_from_statement(
        "select top 2 with ties uid from __account order by uid",
    )
    .expect("top with ties should parse");

    assert_eq!(plan.top_with_ties_limit, Some(2));
    assert_eq!(plan.limit, None);
}

#[test]
fn select_read_plan_rejects_top_with_ties_without_order_by() {
    let err = parse_select_read_plan_from_statement(
        "select top 2 with ties uid from __account",
    )
    .expect_err("top with ties without order by should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message == "SELECT TOP WITH TIES requires ORDER BY in current execution model"
    ));
}

#[test]
fn select_read_plan_rejects_top_with_limit() {
    let err = parse_select_read_plan_from_statement(
        "select top 2 uid from __account limit 1",
    )
    .expect_err("top + limit should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message == "SELECT currently supports TOP or LIMIT/FETCH, but not both"
    ));
}

#[test]
fn select_read_plan_combines_prewhere_and_where_filters() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account prewhere uid = '1' where role = 'admin'",
    )
    .expect("prewhere + where should parse");

    assert!(matches!(plan.where_condition, Some(SelectCondition::And(_))));
}

#[test]
fn select_read_plan_parses_limit_by_plan() {
    let plan = parse_select_read_plan_from_statement(
        "select uid, role from __account order by uid limit 1 by role",
    )
    .expect("limit by should parse");

    assert!(plan.limit.is_none());
    assert!(plan.offset.is_none());
    let limit_by = plan.limit_by.expect("limit by plan should be present");
    assert_eq!(limit_by.per_key_limit, 1);
    assert_eq!(limit_by.fields, vec!["role".to_string()]);
}

#[test]
fn select_read_plan_parses_limit_by_with_offset() {
    let plan = parse_select_read_plan_from_statement(
        "select uid, role from __account order by uid limit 1 offset 1 by role",
    )
    .expect("limit by with offset should parse");

    assert!(plan.limit.is_none());
    assert_eq!(plan.offset, Some(1));
    assert!(plan.limit_by.is_some());
}

#[test]
fn select_read_plan_parses_sort_cluster_distribute_as_ordering() {
    let sort_plan = parse_select_read_plan_from_statement(
        "select uid from __account sort by uid",
    )
    .expect("sort by should parse");
    assert_eq!(sort_plan.order_by.len(), 1);
    assert_eq!(sort_plan.order_by[0].field_name, "uid");

    let cluster_plan = parse_select_read_plan_from_statement(
        "select uid from __account cluster by uid",
    )
    .expect("cluster by should parse");
    assert_eq!(cluster_plan.order_by.len(), 1);
    assert_eq!(cluster_plan.order_by[0].field_name, "uid");

    let distribute_plan = parse_select_read_plan_from_statement(
        "select uid from __account distribute by uid",
    )
    .expect("distribute by should parse");
    assert_eq!(distribute_plan.order_by.len(), 1);
    assert_eq!(distribute_plan.order_by[0].field_name, "uid");
}

#[test]
fn select_read_plan_parses_fetch_first_rows_only_as_limit() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account order by uid fetch first 2 rows only",
    )
    .expect("fetch first rows only should parse");

    assert_eq!(plan.limit, Some(2));
    assert_eq!(plan.offset, None);
}

#[test]
fn select_read_plan_parses_fetch_first_rows_with_ties() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account order by uid fetch first 2 rows with ties",
    )
    .expect("fetch with ties should parse");

    assert_eq!(plan.fetch_with_ties_limit, Some(2));
    assert_eq!(plan.limit, None);
}

#[test]
fn select_read_plan_rejects_fetch_with_ties_without_order_by() {
    let err = parse_select_read_plan_from_statement(
        "select uid from __account fetch first 2 rows with ties",
    )
    .expect_err("fetch with ties without order by should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message == "SELECT FETCH WITH TIES requires ORDER BY in current execution model"
    ));
}

#[test]
fn select_read_plan_parses_fetch_first_row_only_as_limit_one() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account fetch first row only",
    )
    .expect("fetch first row only should parse");

    assert_eq!(plan.limit, Some(1));
}

#[test]
fn select_read_plan_rejects_limit_and_fetch_combination() {
    let err = parse_select_read_plan_from_statement(
        "select uid from __account limit 1 fetch first 1 row only",
    )
    .expect_err("limit + fetch should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message == "SELECT currently supports LIMIT or FETCH, but not both"
    ));
}

#[test]
fn union_select_parses_query_level_fetch_as_limit() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id fetch first 2 rows only",
    )
    .expect("union query fetch should parse");

    assert!(limit_by.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    assert_eq!(limit, Some(2));
    assert_eq!(offset, None);
}

#[test]
fn union_select_parses_query_level_fetch_with_ties() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id fetch first 2 rows with ties",
    )
    .expect("union query fetch with ties should parse");

    assert!(limit_by.is_none());
    assert_eq!(fetch_with_ties_limit, Some(2));
    assert!(fetch_percent.is_none());
    assert!(fetch_percent_with_ties.is_none());
    assert_eq!(limit, None);
    assert_eq!(offset, None);
}

#[test]
fn select_read_plan_parses_fetch_first_percent() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account order by uid fetch first 50 percent rows only",
    )
    .expect("fetch percent should parse");

    assert_eq!(plan.fetch_percent, Some(50));
    assert!(plan.fetch_percent_with_ties.is_none());
    assert_eq!(plan.limit, None);
}

#[test]
fn select_read_plan_parses_fetch_percent_with_ties() {
    let plan = parse_select_read_plan_from_statement(
        "select uid from __account order by uid fetch first 50 percent rows with ties",
    )
    .expect("fetch percent with ties should parse");

    assert!(plan.fetch_percent.is_none());
    assert_eq!(plan.fetch_percent_with_ties, Some(50));
    assert_eq!(plan.limit, None);
}

#[test]
fn union_select_parses_query_level_fetch_percent() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id fetch first 50 percent rows only",
    )
    .expect("union query fetch percent should parse");

    assert!(limit_by.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert_eq!(fetch_percent, Some(50));
    assert!(fetch_percent_with_ties.is_none());
    assert_eq!(limit, None);
    assert_eq!(offset, None);
}

#[test]
fn union_select_parses_query_level_fetch_percent_with_ties() {
    let (_, _, limit_by, fetch_with_ties_limit, fetch_percent, fetch_percent_with_ties, limit, offset) = parse_union_select_read_plans_from_statement(
        "select id from users union all select id from archived_users order by id fetch first 50 percent rows with ties",
    )
    .expect("union query fetch percent with ties should parse");

    assert!(limit_by.is_none());
    assert!(fetch_with_ties_limit.is_none());
    assert!(fetch_percent.is_none());
    assert_eq!(fetch_percent_with_ties, Some(50));
    assert_eq!(limit, None);
    assert_eq!(offset, None);
}

#[test]
fn select_partition_selection_is_rejected_until_supported() {
    let err = parse_select_read_plan_from_statement(
        "select uid from __account partition (p0)",
    )
    .expect_err("partition selection should be rejected");

    assert!(matches!(
        err,
        SqlParseError::UnsupportedStatement(message)
            if message.contains("PARTITION selection")
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

    assert!(!plan.projection_is_wildcard);

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
