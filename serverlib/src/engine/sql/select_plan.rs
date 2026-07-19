use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, Query,
    NamedWindowDefinition, NamedWindowExpr, SelectItem, SetExpr, SetOperator, SetQuantifier,
    Statement, TableFactor, WindowSpec, WindowType, With,
};

use super::literals::parse_default_value;
use super::{
    dialect_capabilities_for_target, evaluate_sql_function, is_supported_sql_function,
    parse_mysql_statements,
    validate_regex_pattern, SelectCaseWhen, SelectComparisonOp, SelectCondition, SelectCtePlan,
    SelectJoin, SelectJoinKind, SelectLimitByPlan, SelectOrderByItem, SelectSetBoundaryOp, SelectSetQueryStep, SelectExpression, SelectPredicate,
    SelectLockMode, SelectProjectionItem, SelectReadPlan, SelectRelation,
    SqlParseError, DEFAULT_SQL_COMPATIBILITY_TARGET,
};

type SelectRelationBinding = SelectRelation;
type SetQueryParseResult = (
    Vec<SelectSetQueryStep>,
    Vec<SelectOrderByItem>,
    Option<SelectLimitByPlan>,
    Option<usize>,
    Option<usize>,
    Option<usize>,
    Option<usize>,
    Option<usize>,
);

pub fn parse_select_projection_from_statement(
    statement: &str,
) -> Result<Option<Vec<String>>, SqlParseError> {

    parse_select_read_plan_from_statement(statement).map(|plan| plan.projection)
    
}

pub fn parse_select_read_plan_from_statement(
    statement: &str,
) -> Result<SelectReadPlan, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let (inner_statement, is_explain) = if lowered.starts_with("explain ") {
        (trimmed["explain".len()..].trim(), true)
    } else {
        (trimmed, false)
    };

    let parsed = parse_mysql_statements(inner_statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Query(query) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not SELECT".to_string(),
        ));
    };

    parse_select_read_plan_from_query(query, is_explain)

}

pub fn parse_select_read_plan_from_parsed_statement(
    statement: &Statement,
) -> Result<SelectReadPlan, SqlParseError> {

    let Statement::Query(query) = statement else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not SELECT".to_string(),
        ));
    };

    parse_select_read_plan_from_query(query, false)

}

pub fn parse_create_view_dependencies_from_statement(
    statement: &Statement,
) -> Result<Vec<String>, SqlParseError> {

    let Statement::CreateView { query, .. } = statement else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE VIEW".to_string(),
        ));
    };

    let plan = parse_select_read_plan_from_query(query.as_ref(), false)?;

    Ok(collect_select_plan_dependencies(&plan))

}

pub fn parse_create_view_dependencies_from_sql(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    if parsed.len() > 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "expected a single CREATE VIEW statement".to_string(),
        ));
    }

    parse_create_view_dependencies_from_statement(single)

}

pub(super) fn parse_select_read_plan_from_query(
    query: &Query,
    is_explain: bool,
) -> Result<SelectReadPlan, SqlParseError> {

    let query_sql = query.to_string();

    let ctes = parse_cte_plans_from_query(query, &query_sql)?;

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlParseError::UnsupportedStatement(
            "only simple SELECT queries are currently supported for projection parsing".to_string(),
        ));
    };

    let mut distinct = select.distinct.is_some();

    let top_limit = parse_select_top_limit(select.top.as_ref())?;

    if !select.lateral_views.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT LATERAL VIEW is not supported yet".to_string(),
        ));
    }


    let named_windows = select.named_window.clone();

    let has_window_clause = !named_windows.is_empty();

    if let Some(plan) = parse_passthrough_derived_select_plan(query, select, is_explain)? {
        return Ok(plan);
    }

    let relation_bindings = parse_relation_bindings_from_table_with_joins(
        select.from.first(),
        &query_sql,
    )?;

    let joins = parse_joins_from_table_with_joins(
        select.from.first(),
        &query_sql,
        &relation_bindings,
    )?;

    let has_wildcard_projection = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)));

    let projection_is_wildcard = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));

    let (projection, projection_items) = if has_wildcard_projection {
        
        for item in &select.projection {
            if let SelectItem::QualifiedWildcard(prefix, _) = item {
                validate_qualified_prefix(prefix, &relation_bindings)?;
            }
        }

        (None, select
            .projection
            .iter()
            .map(|item| parse_select_projection_item(item, &relation_bindings))
            .collect::<Result<Vec<_>, _>>()?)

    } else {

        let mut fields = Vec::new();
        let mut items = Vec::new();

        for item in &select.projection {

            let projection_item = parse_select_projection_item(item, &relation_bindings)?;

            if let SelectProjectionItem::Column { field_name, .. } = &projection_item {
                fields.push(field_name.clone());
            }

            items.push(projection_item);
            
        }

        (Some(fields), items)

    };

    let table_id = match relation_bindings.first() {

        Some(binding) => binding.table_id.clone(),

        None => {

            if projection_is_wildcard {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "from",
                    statement: query_sql.clone(),
                });
            }

            if projection_items
                .iter()
                .all(is_projection_only_without_from_item)
            {
                String::new()
            } else {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "from",
                    statement: query_sql,
                });
            }

        }

    };

    let where_condition = parse_select_condition_from_expr(
        select.selection.as_ref(),
        &relation_bindings,
    )?;

    let prewhere_condition = parse_select_condition_from_expr(
        select.prewhere.as_ref(),
        &relation_bindings,
    )?;

    let where_condition = combine_where_having_conditions(prewhere_condition, where_condition);

    let qualify_condition = parse_select_condition_from_expr(
        select.qualify.as_ref(),
        &relation_bindings,
    )?;

    let group_by = parse_group_by_fields(&select.group_by, &relation_bindings)?;
    let having_condition = parse_select_condition_from_expr(select.having.as_ref(), &relation_bindings)?;

    if having_condition.is_some() && group_by.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT HAVING requires GROUP BY in current execution model".to_string(),
        ));
    }

    if !group_by.is_empty() {
        ensure_group_by_projection_is_supported(&projection_items, &group_by)?;
        distinct = true;
    }

    let where_condition = combine_where_having_conditions(where_condition, having_condition.clone());

    let query_order_by = parse_order_by_items(
        query.order_by.as_ref(),
        &relation_bindings,
        &projection_items,
    )?;

    let compat_order_by = parse_select_compat_order_by_items(select, &relation_bindings)?;
    let order_by = if !query_order_by.is_empty() && !compat_order_by.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT ORDER BY cannot be combined with CLUSTER/DISTRIBUTE/SORT BY in current execution model"
                .to_string(),
        ));
    } else if !query_order_by.is_empty() {
        query_order_by
    } else {
        compat_order_by
    };

    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );

    let query_limit_plan = parse_query_limit_or_fetch_plan(
        query.limit.as_ref(),
        query.fetch.as_ref(),
        "SELECT",
    )?;
    let query_limit = query_limit_plan.limit;
    if (top_limit.limit.is_some() || top_limit.percent.is_some())
        && (query_limit_plan.limit.is_some() || query_limit_plan.percent.is_some())
    {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT currently supports TOP or LIMIT/FETCH, but not both".to_string(),
        ));
    }

    if query_limit_plan.with_ties && order_by.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT FETCH WITH TIES requires ORDER BY in current execution model".to_string(),
        ));
    }

    if top_limit.with_ties && order_by.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT TOP WITH TIES requires ORDER BY in current execution model".to_string(),
        ));
    }

    let limit = query_limit.or(top_limit.limit);
    let top_with_ties_limit = if top_limit.with_ties {
        top_limit.limit
    } else {
        None
    };
    let top_percent_with_ties = if top_limit.with_ties {
        top_limit.percent
    } else {
        None
    };
    let top_percent = if top_limit.with_ties {
        None
    } else {
        top_limit.percent
    };
    let fetch_with_ties_limit = if query_limit_plan.with_ties {
        query_limit_plan.limit
    } else {
        None
    };
    let fetch_percent_with_ties = if query_limit_plan.with_ties {
        query_limit_plan.percent
    } else {
        None
    };
    let fetch_percent = if query_limit_plan.with_ties {
        None
    } else {
        query_limit_plan.percent
    };

    let limit_by = parse_select_limit_by_plan(
        &query.limit_by,
        limit,
        query.offset.as_ref(),
        query.fetch.as_ref(),
        &relation_bindings,
        "SELECT",
    )?;

    let limit = if limit_by.is_some()
        || top_with_ties_limit.is_some()
        || top_percent_with_ties.is_some()
        || fetch_with_ties_limit.is_some()
        || fetch_percent_with_ties.is_some()
        || fetch_percent.is_some()
    {
        None
    } else {
        limit
    };
    let offset = parse_query_offset(query.offset.as_ref())?;
    let lock_mode = parse_select_lock_mode(query, &query_sql)?;

    Ok(SelectReadPlan {
        table_id,
        ctes,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        named_windows,
        projection,
        projection_items,
        projection_is_wildcard,
        distinct,
        order_by,
        group_by,
        having_condition,
        has_window_clause,
        limit_by,
        top_percent,
        top_percent_with_ties,
        top_with_ties_limit,
        fetch_percent,
        fetch_percent_with_ties,
        fetch_with_ties_limit,
        limit,
        offset,
        where_condition,
        qualify_condition,
        lock_mode,
        is_explain,
    })

}

fn parse_cte_plans_from_query(
    query: &Query,
    query_sql: &str,
) -> Result<Vec<SelectCtePlan>, SqlParseError> {

    let Some(with) = query.with.as_ref() else {
        return Ok(Vec::new());
    };

    let mut ctes = Vec::with_capacity(with.cte_tables.len());

    for cte in &with.cte_tables {

        if cte.materialized.is_some() {
            return Err(SqlParseError::UnsupportedStatement(
                "CTE MATERIALIZED/NOT MATERIALIZED is not supported yet".to_string(),
            ));
        }

        let cte_table_id = common::normalize_identifier!(&cte.alias.name.value);
        if cte_table_id.is_empty() {
            return Err(SqlParseError::MissingIdentifier {
                keyword: "with",
                statement: query_sql.to_string(),
            });
        }

        if with.recursive
            && let Some(recursive_plan) = parse_recursive_cte_plan(cte, &cte_table_id)?
        {
            ctes.push(recursive_plan);
            continue;
        }

        let cte_plan = parse_select_read_plan_from_query(cte.query.as_ref(), false)?;

        ctes.push(SelectCtePlan {
            table_id: cte_table_id,
            read_plan: Box::new(cte_plan),
            recursive_read_plan: None,
            recursive_union_all: false,
        });
    }

    Ok(ctes)

}

fn parse_recursive_cte_plan(
    cte: &sqlparser::ast::Cte,
    cte_table_id: &str,
) -> Result<Option<SelectCtePlan>, SqlParseError> {

    if cte.query.order_by.is_some()
        || cte.query.limit.is_some()
        || !cte.query.limit_by.is_empty()
        || cte.query.offset.is_some()
        || cte.query.fetch.is_some()
    {
        return Err(SqlParseError::UnsupportedStatement(
            "recursive CTE branch-level ORDER BY/LIMIT/OFFSET/FETCH is not supported yet"
                .to_string(),
        ));
    }

    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = cte.query.body.as_ref()
    else {
        return Ok(None);
    };

    if *op != SetOperator::Union {
        return Err(SqlParseError::UnsupportedStatement(
            "recursive CTE currently supports only UNION between seed and recursive terms"
                .to_string(),
        ));
    }

    let recursive_union_all = matches!(set_quantifier, SetQuantifier::All);

    let seed_plan = parse_union_branch_set_expr(left, None)?;
    let recursive_plan = parse_union_branch_set_expr(right, None)?;

    let seed_references_self = select_plan_references_table(&seed_plan, cte_table_id);
    let recursive_references_self = select_plan_references_table(&recursive_plan, cte_table_id);

    if seed_references_self {
        return Err(SqlParseError::UnsupportedStatement(
            "recursive CTE seed term must not reference the recursive table".to_string(),
        ));
    }

    if !recursive_references_self {
        return Ok(None);
    }

    Ok(Some(SelectCtePlan {
        table_id: cte_table_id.to_string(),
        read_plan: Box::new(seed_plan),
        recursive_read_plan: Some(Box::new(recursive_plan)),
        recursive_union_all,
    }))

}

fn select_plan_references_table(plan: &SelectReadPlan, table_id: &str) -> bool {

    let normalized_target = common::normalize_identifier!(table_id);

    if !plan.table_id.is_empty() && common::normalize_identifier!(&plan.table_id) == normalized_target {
        return true;
    }

    if plan
        .relations
        .iter()
        .any(|relation| common::normalize_identifier!(&relation.table_id) == normalized_target)
    {
        return true;
    }

    if plan
        .joins
        .iter()
        .any(|join| common::normalize_identifier!(&join.relation.table_id) == normalized_target)
    {
        return true;
    }

    if plan
        .where_condition
        .as_ref()
        .is_some_and(|condition| select_condition_references_table(condition, &normalized_target))
    {
        return true;
    }

    for cte in &plan.ctes {
        if select_plan_references_table(&cte.read_plan, &normalized_target)
            || cte
                .recursive_read_plan
                .as_ref()
                .is_some_and(|recursive| select_plan_references_table(recursive, &normalized_target))
        {
            return true;
        }
    }

    false

}

fn select_condition_references_table(
    condition: &SelectCondition,
    normalized_table_id: &str,
) -> bool {

    match condition {

        SelectCondition::And(children) | 
        SelectCondition::Or(children) => children
            .iter()
            .any(|child| select_condition_references_table(child, normalized_table_id)),

        SelectCondition::Not(child) => {
            select_condition_references_table(child, normalized_table_id)
        },

        SelectCondition::Predicate(predicate) => {
            select_predicate_references_table(predicate, normalized_table_id)
        },

    }

}

fn select_predicate_references_table(
    predicate: &SelectPredicate,
    normalized_table_id: &str,
) -> bool {

    match predicate {

        SelectPredicate::InSubquery { subquery, .. } |
        SelectPredicate::ScalarSubqueryComparison { subquery, .. } |
        SelectPredicate::AnySubqueryComparison { subquery, .. } |
        SelectPredicate::AllSubqueryComparison { subquery, .. } |
        SelectPredicate::Exists { subquery, .. } => {
            select_plan_references_table(subquery, normalized_table_id)
        }

        _ => false,

    }

}

fn parse_group_by_fields(
    group_by: &GroupByExpr,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Vec<String>, SqlParseError> {

    match group_by {

        GroupByExpr::All(_) => Err(SqlParseError::UnsupportedStatement(
            "GROUP BY ALL is not supported yet".to_string(),
        )),

        GroupByExpr::Expressions(expressions, _) => {

            let mut fields = Vec::with_capacity(expressions.len());

            for expression in expressions {
                let field = parse_condition_column_name(expression, relation_bindings).map_err(|_| {
                    SqlParseError::UnsupportedStatement(
                        "GROUP BY currently supports only direct column references".to_string(),
                    )
                })?;

                fields.push(field);
            }

            Ok(fields)

        },

    }

}


fn collect_select_plan_dependencies(plan: &SelectReadPlan) -> Vec<String> {

    let mut seen = HashSet::new();
    let mut dependencies = Vec::new();

    collect_select_plan_dependencies_into(plan, &mut seen, &mut dependencies);

    dependencies

}

fn collect_select_plan_dependencies_into(
    plan: &SelectReadPlan,
    seen: &mut HashSet<String>,
    dependencies: &mut Vec<String>,
) {

    push_select_plan_dependency(&plan.table_id, seen, dependencies);

    for relation in &plan.relations {
        push_select_plan_dependency(&relation.table_id, seen, dependencies);
    }

    for join in &plan.joins {
        push_select_plan_dependency(&join.relation.table_id, seen, dependencies);
    }

    for cte in &plan.ctes {
        collect_select_plan_dependencies_into(&cte.read_plan, seen, dependencies);
        if let Some(recursive_plan) = cte.recursive_read_plan.as_ref() {
            collect_select_plan_dependencies_into(recursive_plan, seen, dependencies);
        }
    }

}

fn push_select_plan_dependency(
    dependency: &str,
    seen: &mut HashSet<String>,
    dependencies: &mut Vec<String>,
) {

    if dependency.is_empty() || !seen.insert(dependency.to_string()) {
        return;
    }

    dependencies.push(dependency.to_string());

}

fn ensure_group_by_projection_is_supported(
    projection_items: &[SelectProjectionItem],
    group_by_fields: &[String],
) -> Result<(), SqlParseError> {

    for projection in projection_items {

        let SelectProjectionItem::Column { field_name, .. } = projection else {
            return Err(SqlParseError::UnsupportedStatement(
                "GROUP BY currently supports only direct column projections".to_string(),
            ));
        };

        if !group_by_fields.iter().any(|field| field == field_name) {
            return Err(SqlParseError::UnsupportedStatement(
                "GROUP BY projection must only reference grouped columns in current execution model"
                    .to_string(),
            ));
        }

    }

    Ok(())

}

fn combine_where_having_conditions(
    where_condition: Option<SelectCondition>,
    having_condition: Option<SelectCondition>,
) -> Option<SelectCondition> {

    match (where_condition, having_condition) {

        (Some(where_condition), Some(having_condition)) => {
            Some(SelectCondition::And(vec![where_condition, having_condition]))
        },
        
        (Some(where_condition), None) => Some(where_condition),
        
        (None, Some(having_condition)) => Some(having_condition),
        
        (None, None) => None,

    }

}

fn parse_order_by_items(
    order_by: Option<&sqlparser::ast::OrderBy>,
    relation_bindings: &[SelectRelationBinding],
    projection_items: &[SelectProjectionItem],
) -> Result<Vec<SelectOrderByItem>, SqlParseError> {

    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };

    if order_by.interpolate.is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "ORDER BY INTERPOLATE is not supported yet".to_string(),
        ));
    }

    if relation_bindings.is_empty() {

        let projection_outputs = projection_items
            .iter()
            .filter_map(|item| match item {
                
                SelectProjectionItem::Column {
                    field_name,
                    output_name,
                } => {
                    let mut names = vec![output_name.clone()];
                    if output_name != field_name {
                        names.push(field_name.clone());
                    }

                    Some(names)
                },

                SelectProjectionItem::Case { output_name, .. } |
                SelectProjectionItem::InbuiltFunction { output_name, .. } |
                SelectProjectionItem::WindowFunction { output_name, .. } => {
                    Some(vec![output_name.clone()])
                },

                SelectProjectionItem::Wildcard { .. } => None,

            })
            .collect::<Vec<_>>();

        let mut items = Vec::with_capacity(order_by.exprs.len());

        for expression in &order_by.exprs {

            if expression.nulls_first.is_some() || expression.with_fill.is_some() {
                return Err(SqlParseError::UnsupportedStatement(
                    "ORDER BY NULLS FIRST/LAST or WITH FILL is not supported yet".to_string(),
                ));
            }

            let field_name = match &expression.expr {

                Expr::Identifier(identifier) => {

                    let normalized = common::normalize_identifier!(&identifier.value);

                    let Some(resolved) = projection_outputs
                        .iter()
                        .find_map(|output_names| {
                            if output_names.iter().any(|name| name == &normalized) {
                                output_names.first().cloned()
                            } else {
                                None
                            }
                        })
                    else {
                        return Err(SqlParseError::UnsupportedStatement(format!(
                            "ORDER BY without FROM references unknown output field '{}'",
                            identifier.value
                        )));
                    };

                    resolved

                },

                Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

                    let position = position.parse::<usize>().map_err(|_| {
                        SqlParseError::UnsupportedStatement(
                            "ORDER BY without FROM ordinal must be an unsigned numeric literal"
                                .to_string(),
                        )
                    })?;

                    if position == 0 {
                        return Err(SqlParseError::UnsupportedStatement(
                            "ORDER BY without FROM ordinal must start at 1".to_string(),
                        ));
                    }

                    let index = position - 1;

                    let Some(output_names) = projection_outputs.get(index) else {
                        return Err(SqlParseError::UnsupportedStatement(format!(
                            "ORDER BY without FROM ordinal {} is out of range",
                            position
                        )));
                    };

                    output_names.first().cloned().ok_or_else(|| {
                        SqlParseError::UnsupportedStatement(
                            "ORDER BY without FROM could not resolve output field".to_string(),
                        )
                    })?
                
                },

                _ => {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ORDER BY without FROM currently supports only output aliases or ordinal positions"
                            .to_string(),
                    ));
                }

            };

            items.push(SelectOrderByItem {
                field_name,
                descending: expression.asc == Some(false),
            });

        }

        return Ok(items);
    }

    let mut items = Vec::with_capacity(order_by.exprs.len());
    
    for expression in &order_by.exprs {

        if expression.nulls_first.is_some() || expression.with_fill.is_some() {
            return Err(SqlParseError::UnsupportedStatement(
                "ORDER BY NULLS FIRST/LAST or WITH FILL is not supported yet".to_string(),
            ));
        }

        let field_name = parse_condition_column_name(&expression.expr, relation_bindings).map_err(|_| {
            SqlParseError::UnsupportedStatement(
                "ORDER BY currently supports only direct column references".to_string(),
            )
        })?;

        items.push(SelectOrderByItem {
            field_name,
            descending: expression.asc == Some(false),
        });

    }

    Ok(items)

}

pub fn parse_union_select_read_plans_from_statement(
    statement: &str,
) -> Result<SetQueryParseResult, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let parsed = parse_mysql_statements(trimmed)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Query(query) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not SELECT set query".to_string(),
        ));
    };

    let mut steps = Vec::new();
    collect_set_query_steps(&query.body, query.with.as_ref(), &mut steps)?;

    let branch_count = steps
        .iter()
        .filter(|step| matches!(step, SelectSetQueryStep::Branch(_)))
        .count();

    if branch_count < 2 {
        return Err(SqlParseError::UnsupportedStatement(
            "set query requires at least two SELECT branches".to_string(),
        ));
    }

    let order_by = parse_union_order_by_items(query.order_by.as_ref())?;
    let query_limit_plan = parse_query_limit_or_fetch_plan(
        query.limit.as_ref(),
        query.fetch.as_ref(),
        "set query",
    )?;
    let parsed_limit = query_limit_plan.limit;

    if query_limit_plan.with_ties && order_by.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "set query FETCH WITH TIES requires ORDER BY in current execution model".to_string(),
        ));
    }

    let limit_by = parse_set_query_limit_by_plan(
        &query.limit_by,
        parsed_limit,
        query.offset.as_ref(),
        query.fetch.as_ref(),
    )?;

    let fetch_with_ties_limit = if query_limit_plan.with_ties {
        query_limit_plan.limit
    } else {
        None
    };
    let fetch_percent_with_ties = if query_limit_plan.with_ties {
        query_limit_plan.percent
    } else {
        None
    };
    let fetch_percent = if query_limit_plan.with_ties {
        None
    } else {
        query_limit_plan.percent
    };

    let limit = if limit_by.is_some()
        || fetch_with_ties_limit.is_some()
        || fetch_percent_with_ties.is_some()
        || fetch_percent.is_some()
    {
        None
    } else {
        parsed_limit
    };

    let offset = parse_query_offset(query.offset.as_ref())?;

    Ok((
        steps,
        order_by,
        limit_by,
        fetch_with_ties_limit,
        fetch_percent,
        fetch_percent_with_ties,
        limit,
        offset,
    ))

}

fn collect_set_query_steps(
    set_expr: &SetExpr,
    inherited_with: Option<&With>,
    steps: &mut Vec<SelectSetQueryStep>,
) -> Result<(), SqlParseError> {

    match set_expr {

        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {

            collect_set_query_steps(left, inherited_with, steps)?;
            collect_set_query_steps(right, inherited_with, steps)?;

            let boundary_operation = match op {

                SetOperator::Union => {
                    if matches!(set_quantifier, SetQuantifier::All) {
                        SelectSetBoundaryOp::UnionAll
                    } else {
                        SelectSetBoundaryOp::UnionDistinct
                    }
                },

                SetOperator::Except => {
                    if matches!(set_quantifier, SetQuantifier::All) {
                        return Err(SqlParseError::UnsupportedStatement(
                            "EXCEPT ALL is not supported yet".to_string(),
                        ));
                    }

                    SelectSetBoundaryOp::ExceptDistinct
                },

                SetOperator::Intersect => {
                    if matches!(set_quantifier, SetQuantifier::All) {
                        return Err(SqlParseError::UnsupportedStatement(
                            "INTERSECT ALL is not supported yet".to_string(),
                        ));
                    }

                    SelectSetBoundaryOp::IntersectDistinct
                },

            };

            steps.push(SelectSetQueryStep::BoundaryOperation(boundary_operation));
            Ok(())
        
        },

        SetExpr::Select(_) => {
            let branch_plan = parse_union_branch_set_expr(set_expr, inherited_with)?;
            steps.push(SelectSetQueryStep::Branch(branch_plan));
            Ok(())
        },

        SetExpr::Query(query) => {

            let query = query.as_ref();

            if matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
                if query.order_by.is_some()
                    || query.limit.is_some()
                    || !query.limit_by.is_empty()
                    || query.offset.is_some()
                    || query.fetch.is_some()
                {
                    return Err(SqlParseError::UnsupportedStatement(
                        "set-query branch-level ORDER BY/LIMIT/OFFSET/FETCH clauses are not supported yet"
                            .to_string(),
                    ));
                }

                let nested_with = query.with.as_ref().or(inherited_with);
                collect_set_query_steps(query.body.as_ref(), nested_with, steps)
            } else {
                let branch_plan = parse_union_branch_set_expr(set_expr, inherited_with)?;
                steps.push(SelectSetQueryStep::Branch(branch_plan));
                Ok(())
            }
        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "set query branch must be a SELECT query".to_string(),
        )),

    }

}

fn parse_union_branch_set_expr(
    set_expr: &SetExpr,
    inherited_with: Option<&With>,
) -> Result<SelectReadPlan, SqlParseError> {

    match set_expr {

        SetExpr::Select(select) => {
            let query = Query {
                with: inherited_with.cloned(),
                body: Box::new(SetExpr::Select(select.clone())),
                order_by: None,
                limit: None,
                limit_by: Vec::new(),
                offset: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
            };

            parse_select_read_plan_from_query(&query, false)
        },

        SetExpr::Query(query) => {
            let mut branch_query = query.as_ref().clone();
            if branch_query.with.is_none() {
                branch_query.with = inherited_with.cloned();
            }

            parse_select_read_plan_from_query(&branch_query, false)
        }

        _ => Err(SqlParseError::UnsupportedStatement(
            "set query branch must be a SELECT query".to_string(),
        )),

    }

}

fn parse_union_order_by_items(
    order_by: Option<&sqlparser::ast::OrderBy>,
) -> Result<Vec<SelectOrderByItem>, SqlParseError> {

    const UNION_ORDER_BY_ORDINAL_PREFIX: &str = "__union_order_by_ordinal__";

    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };

    if order_by.interpolate.is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "UNION ORDER BY INTERPOLATE is not supported yet".to_string(),
        ));
    }

    let mut items = Vec::with_capacity(order_by.exprs.len());

    for expression in &order_by.exprs {

        if expression.nulls_first.is_some() || expression.with_fill.is_some() {
            return Err(SqlParseError::UnsupportedStatement(
                "UNION ORDER BY NULLS FIRST/LAST or WITH FILL is not supported yet".to_string(),
            ));
        }

        let field_name = match &expression.expr {

            Expr::Identifier(identifier) => common::normalize_identifier!(&identifier.value),

            Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                common::normalize_identifier!(
                    &parts
                        .iter()
                        .map(|part| part.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                )
            },

            Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

                let position = position.parse::<usize>().map_err(|_| {
                    SqlParseError::UnsupportedStatement(
                        "UNION ORDER BY ordinal must be an unsigned numeric literal".to_string(),
                    )
                })?;

                if position == 0 {
                    return Err(SqlParseError::UnsupportedStatement(
                        "UNION ORDER BY ordinal must start at 1".to_string(),
                    ));
                }

                format!("{UNION_ORDER_BY_ORDINAL_PREFIX}{position}")

            },

            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    "UNION ORDER BY currently supports only direct column references or ordinal positions"
                        .to_string(),
                ));
            }

        };

        items.push(SelectOrderByItem {
            field_name,
            descending: expression.asc == Some(false),
        });

    }

    Ok(items)

}

fn parse_select_lock_mode(query: &Query, query_sql: &str) -> Result<SelectLockMode, SqlParseError> {

    if query.for_clause.is_none() && query.locks.is_empty() {
        return Ok(SelectLockMode::None);
    }

    let lowered = query_sql.to_ascii_lowercase();

    if lowered.contains(" for update") {
        return Ok(SelectLockMode::ForUpdate);
    }

    if lowered.contains(" for share") {
        return Ok(SelectLockMode::ForShare);
    }

    Err(SqlParseError::UnsupportedStatement(
        "SELECT lock clause is not supported in current execution model".to_string(),
    ))

}

fn parse_passthrough_derived_select_plan(
    query: &Query,
    select: &sqlparser::ast::Select,
    is_explain: bool,
) -> Result<Option<SelectReadPlan>, SqlParseError> {
    
    if select.from.len() != 1 {
        return Ok(None);
    }

    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return Ok(None);
    }

    let TableFactor::Derived {
        subquery,
        alias,
        ..
    } = &table_with_joins.relation
    else {
        return Ok(None);
    };

    let alias_name = alias
        .as_ref()
        .map(|alias| common::normalize_identifier!(&alias.name.value));

    let is_wildcard_passthrough = match select.projection.as_slice() {

        [SelectItem::Wildcard(_)] => true,
        
        [SelectItem::QualifiedWildcard(prefix, _)] => {
            let Some(alias_name) = alias_name.as_ref() else {
                return Ok(None);
            };
            common::normalize_identifier!(&prefix.to_string()) == *alias_name
        },

        _ => false,

    };

    let mut inner_plan = parse_select_read_plan_from_query(subquery.as_ref(), false)?;
    let projection_map = passthrough_projection_map(&inner_plan)?;

    if !is_wildcard_passthrough {

        let derived_binding = SelectRelationBinding {
            table_id: alias_name
                .clone()
                .unwrap_or_else(|| "__derived".to_string()),
            alias: alias_name.clone(),
        };

        let rewritten_projection_items = rewrite_passthrough_outer_projection_items(
            &select.projection,
            std::slice::from_ref(&derived_binding),
            &projection_map,
        )?;

        inner_plan.projection = Some(
            rewritten_projection_items
                .iter()
                .map(|item| match item {

                    SelectProjectionItem::Column { field_name, .. } => Ok(field_name.clone()),
                    
                    _ => Err(SqlParseError::UnsupportedStatement(
                        "derived wrapper projection currently supports only direct outer column projections"
                            .to_string(),
                    )),

                })
                .collect::<Result<Vec<_>, _>>()?,
        );

        inner_plan.projection_items = rewritten_projection_items;
        inner_plan.projection_is_wildcard = false;

    }

    if let Some(selection) = select.selection.as_ref() {

        let derived_binding = SelectRelationBinding {
            table_id: alias_name
                .clone()
                .unwrap_or_else(|| "__derived".to_string()),
            alias: alias_name.clone(),
        };

        let outer_condition = parse_select_condition_from_expr(
            Some(selection),
            std::slice::from_ref(&derived_binding),
        )?
        .ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "derived wrapper WHERE condition is invalid".to_string(),
            )
        })?;

        let rewritten_outer_condition = rewrite_passthrough_outer_condition(
            &outer_condition,
            &projection_map,
        )?;

        let merged_where = match inner_plan.where_condition.take() {
            Some(inner_where) => SelectCondition::And(vec![inner_where, rewritten_outer_condition]),
            None => rewritten_outer_condition,
        };

        inner_plan.where_condition = Some(merged_where);
        inner_plan.pushdown_conditions = derive_relation_pushdown_conditions(
            inner_plan.where_condition.as_ref(),
            &inner_plan.relations,
            &inner_plan.joins,
        );

    }

    let outer_limit = parse_query_limit_or_fetch(
        query.limit.as_ref(),
        query.fetch.as_ref(),
        "SELECT",
    )?;
    let outer_offset = parse_query_offset(query.offset.as_ref())?;
    
    let (limit, offset) = compose_row_windows(
        inner_plan.limit,
        inner_plan.offset,
        outer_limit,
        outer_offset,
    );

    inner_plan.limit = limit;
    inner_plan.offset = offset;

    if is_explain {
        inner_plan.is_explain = true;
    }

    Ok(Some(inner_plan))

}

fn rewrite_passthrough_outer_projection_items(
    projection_items: &[SelectItem],
    relation_bindings: &[SelectRelationBinding],
    projection_map: &HashMap<String, String>,
) -> Result<Vec<SelectProjectionItem>, SqlParseError> {

    projection_items
        .iter()
        .map(|item| {

            let projection_item = parse_select_projection_item(item, relation_bindings)?;

            match projection_item {

                SelectProjectionItem::Column {
                    field_name,
                    output_name,
                } => Ok(SelectProjectionItem::Column {
                    field_name: rewrite_passthrough_field_name(&field_name, projection_map)?,
                    output_name,
                }),

                _ => Err(SqlParseError::UnsupportedStatement(
                    "derived wrapper projection currently supports only direct outer column projections"
                        .to_string(),
                )),

            }

        })
        .collect::<Result<Vec<_>, _>>()

}

fn passthrough_projection_map(
    inner_plan: &SelectReadPlan,
) -> Result<HashMap<String, String>, SqlParseError> {

    let mut projection_map = HashMap::new();

    for item in &inner_plan.projection_items {

        let SelectProjectionItem::Column {
            field_name,
            output_name,
        } = item
        else {
            return Err(SqlParseError::UnsupportedStatement(
                "derived wrapper WHERE currently supports only inner direct column projections"
                    .to_string(),
            ));
        };

        if projection_map
            .insert(output_name.clone(), field_name.clone())
            .is_some()
        {
            return Err(SqlParseError::UnsupportedStatement(
                "derived wrapper WHERE requires unique projected column names".to_string(),
            ));
        }

    }

    Ok(projection_map)

}

fn rewrite_passthrough_outer_condition(
    condition: &SelectCondition,
    projection_map: &HashMap<String, String>,
) -> Result<SelectCondition, SqlParseError> {

    match condition {

        SelectCondition::And(children) => Ok(SelectCondition::And(
            children
                .iter()
                .map(|child| rewrite_passthrough_outer_condition(child, projection_map))
                .collect::<Result<Vec<_>, _>>()?,
        )),

        SelectCondition::Or(children) => Ok(SelectCondition::Or(
            children
                .iter()
                .map(|child| rewrite_passthrough_outer_condition(child, projection_map))
                .collect::<Result<Vec<_>, _>>()?,
        )),

        SelectCondition::Not(child) => Ok(SelectCondition::Not(Box::new(
            rewrite_passthrough_outer_condition(child, projection_map)?,
        ))),

        SelectCondition::Predicate(predicate) => Ok(SelectCondition::Predicate(
            rewrite_passthrough_outer_predicate(predicate, projection_map)?,
        )),

    }

}

fn rewrite_passthrough_outer_predicate(
    predicate: &SelectPredicate,
    projection_map: &HashMap<String, String>,
) -> Result<SelectPredicate, SqlParseError> {

    match predicate {

        SelectPredicate::Comparison {
            field_name,
            op,
            value,
        } => Ok(SelectPredicate::Comparison {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            op: op.clone(),
            value: value.clone(),
        }),

        SelectPredicate::Like {
            field_name,
            pattern,
            negated,
            case_insensitive,
            escape_char,
        } => Ok(SelectPredicate::Like {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape_char: *escape_char,
        }),

        SelectPredicate::Regex {
            field_name,
            pattern,
            negated,
            case_insensitive,
        } => Ok(SelectPredicate::Regex {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
        }),

        SelectPredicate::FieldComparison {
            left_field_name,
            op,
            right_field_name,
        } => Ok(SelectPredicate::FieldComparison {
            left_field_name: rewrite_passthrough_field_name(left_field_name, projection_map)?,
            op: op.clone(),
            right_field_name: rewrite_passthrough_field_name(right_field_name, projection_map)?,
        }),

        SelectPredicate::InList {
            field_name,
            values,
            negated,
        } => Ok(SelectPredicate::InList {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            values: values.clone(),
            negated: *negated,
        }),

        SelectPredicate::IsNull { field_name, negated } => Ok(SelectPredicate::IsNull {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            negated: *negated,
        }),

        SelectPredicate::InSubquery {
            field_name,
            subquery,
            negated,
        } => Ok(SelectPredicate::InSubquery {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            subquery: subquery.clone(),
            negated: *negated,
        }),

        SelectPredicate::ScalarSubqueryComparison {
            field_name,
            op,
            subquery,
        } => Ok(SelectPredicate::ScalarSubqueryComparison {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            op: op.clone(),
            subquery: subquery.clone(),
        }),

        SelectPredicate::AnySubqueryComparison {
            field_name,
            op,
            subquery,
        } => Ok(SelectPredicate::AnySubqueryComparison {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            op: op.clone(),
            subquery: subquery.clone(),
        }),

        SelectPredicate::AllSubqueryComparison {
            field_name,
            op,
            subquery,
        } => Ok(SelectPredicate::AllSubqueryComparison {
            field_name: rewrite_passthrough_field_name(field_name, projection_map)?,
            op: op.clone(),
            subquery: subquery.clone(),
        }),

        SelectPredicate::Exists { subquery, negated } => Ok(SelectPredicate::Exists {
            subquery: subquery.clone(),
            negated: *negated,
        }),

    }

}

fn rewrite_passthrough_field_name(
    field_name: &str,
    projection_map: &HashMap<String, String>,
) -> Result<String, SqlParseError> {

    let output_name = field_name
        .split_once('.')
        .map(|(_, output_name)| output_name)
        .unwrap_or(field_name);

    projection_map.get(output_name).cloned().ok_or_else(|| {
        SqlParseError::UnsupportedStatement(format!(
            "derived wrapper WHERE references unknown projected column '{}'",
            output_name
        ))
    })

}

fn compose_row_windows(
    inner_limit: Option<usize>,
    inner_offset: Option<usize>,
    outer_limit: Option<usize>,
    outer_offset: Option<usize>,
) -> (Option<usize>, Option<usize>) {

    let resolved_inner_offset = inner_offset.unwrap_or(0);
    let resolved_outer_offset = outer_offset.unwrap_or(0);
    
    let offset = if inner_offset.is_some() || outer_offset.is_some() {
        Some(resolved_inner_offset.saturating_add(resolved_outer_offset))
    } else {
        None
    };

    let limit = match inner_limit {
        Some(inner_limit) => {
            let remaining = inner_limit.saturating_sub(resolved_outer_offset);
            Some(match outer_limit {
                Some(outer_limit) => remaining.min(outer_limit),
                None => remaining,
            })
        }
        None => outer_limit,
    };

    (limit, offset)

}

fn parse_select_projection_item(
    item: &SelectItem,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectProjectionItem, SqlParseError> {

    let dialect_capabilities = dialect_capabilities_for_target(DEFAULT_SQL_COMPATIBILITY_TARGET);

    match item {

        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
            ensure_unqualified_allowed(relation_bindings, &ident.value, "SELECT projection")?;

            let field_name = common::normalize_identifier!(&ident.value);
            Ok(SelectProjectionItem::Column {
                field_name: field_name.clone(),
                output_name: field_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            let field_name = parse_qualified_field_name(parts, relation_bindings, "SELECT projection")?;
            let output_name = common::normalize_identifier!(&parts.last().expect("parts should exist").value);

            Ok(SelectProjectionItem::Column {
                field_name,
                output_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::Function(function)) => {
            let function_name = function.name.to_string();

            if function.over.is_some() {
                return parse_window_projection_item(function, common::normalize_identifier!(&function_name));
            }

            if !is_supported_sql_function(&function_name) {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "SELECT projection function '{}' is not supported",
                    function_name
                )));
            }

            Ok(SelectProjectionItem::InbuiltFunction {
                output_name: common::normalize_identifier!(&function_name),
                function: function.clone(),
            })
        },

        SelectItem::UnnamedExpr(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        }) => parse_case_projection_item(
            "case".to_string(),
            operand.as_deref(),
            conditions,
            results,
            else_result.as_deref(),
            relation_bindings,
            dialect_capabilities,
        ),

        SelectItem::Wildcard(_) => Ok(SelectProjectionItem::Wildcard { relation: None }),

        SelectItem::QualifiedWildcard(prefix, _) => {
            let relation = validate_qualified_prefix(prefix, relation_bindings)?;
            Ok(SelectProjectionItem::Wildcard { relation: Some(relation) })
        },

        SelectItem::ExprWithAlias { expr, alias } => match expr {

            Expr::Identifier(ident) => {
                ensure_unqualified_allowed(relation_bindings, &ident.value, "SELECT projection")?;

                let field_name = common::normalize_identifier!(&ident.value);
                let output_name = common::normalize_identifier!(&alias.value);

                Ok(SelectProjectionItem::Column {
                    field_name,
                    output_name,
                })
            },

            Expr::CompoundIdentifier(parts) => {
                let field_name =
                    parse_qualified_field_name(parts, relation_bindings, "SELECT projection")?;

                Ok(SelectProjectionItem::Column {
                    field_name,
                    output_name: common::normalize_identifier!(&alias.value),
                })
            },

            Expr::Function(function) => {
                let function_name = function.name.to_string();

                if function.over.is_some() {
                    return parse_window_projection_item(function, common::normalize_identifier!(&alias.value));
                }

                if !is_supported_sql_function(&function_name) {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "SELECT projection function '{}' is not supported",
                        function_name
                    )));
                }

                Ok(SelectProjectionItem::InbuiltFunction {
                    output_name: common::normalize_identifier!(&alias.value),
                    function: function.clone(),
                })
            },

            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => parse_case_projection_item(
                common::normalize_identifier!(&alias.value),
                operand.as_deref(),
                conditions,
                results,
                else_result.as_deref(),
                relation_bindings,
                dialect_capabilities,
            ),

            _ => Err(SqlParseError::UnsupportedStatement(
                "only direct column and inbuilt function projection is currently supported"
                    .to_string(),
            )),

        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "only direct column and inbuilt function projection is currently supported"
                .to_string(),
        )),

    }

}

fn parse_case_projection_item(
    output_name: String,
    operand: Option<&Expr>,
    conditions: &[Expr],
    results: &[Expr],
    else_result: Option<&Expr>,
    relation_bindings: &[SelectRelationBinding],
    dialect_capabilities: super::SqlDialectCapabilities,
) -> Result<SelectProjectionItem, SqlParseError> {

    if !dialect_capabilities.supports_searched_case_expressions {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE expressions are not supported for this SQL compatibility target".to_string(),
        ));
    }

    let parsed_operand = if let Some(operand) = operand {

        if !dialect_capabilities.supports_simple_case_expressions {
            return Err(SqlParseError::UnsupportedStatement(
                "simple CASE projections are not supported for this SQL compatibility target"
                    .to_string(),
            ));
        }

        Some(parse_case_projection_value(operand, relation_bindings)?)

    } else {
        None
    };

    if conditions.len() != results.len() {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE projection has mismatched WHEN/THEN clauses".to_string(),
        ));
    }

    let mut branches = Vec::with_capacity(conditions.len());

    for (condition_expr, result_expr) in conditions.iter().zip(results.iter()) {

        if parsed_operand.is_some() {
            
            branches.push((
                SelectCaseWhen::Equals(parse_case_projection_value(
                    condition_expr,
                    relation_bindings,
                )?),
                parse_case_projection_value(result_expr, relation_bindings)?,
            ));

        } else {

            let condition = parse_select_condition_expression(condition_expr, relation_bindings)?;
            ensure_condition_has_no_subqueries(&condition, "CASE WHEN")?;

            branches.push((
                SelectCaseWhen::Condition(condition),
                parse_case_projection_value(result_expr, relation_bindings)?,
            ));

        }

    }

    let else_value = else_result
        .map(|expr| parse_case_projection_value(expr, relation_bindings))
        .transpose()?;

    Ok(SelectProjectionItem::Case {
        output_name,
        operand: parsed_operand,
        branches,
        else_value,
    })

}

fn parse_window_projection_item(
    function: &sqlparser::ast::Function,
    output_name: String,
) -> Result<SelectProjectionItem, SqlParseError> {

    let function_name = function.name.to_string();

    if !function_name.eq_ignore_ascii_case("row_number") &&
        !function_name.eq_ignore_ascii_case("rank") &&
        !function_name.eq_ignore_ascii_case("dense_rank") &&
        !function_name.eq_ignore_ascii_case("percent_rank") &&
        !function_name.eq_ignore_ascii_case("cume_dist") &&
        !function_name.eq_ignore_ascii_case("ntile") &&
        !function_name.eq_ignore_ascii_case("lag") &&
        !function_name.eq_ignore_ascii_case("lead") &&
        !function_name.eq_ignore_ascii_case("sum") &&
        !function_name.eq_ignore_ascii_case("count") &&
        !function_name.eq_ignore_ascii_case("avg") &&
        !function_name.eq_ignore_ascii_case("min") &&
        !function_name.eq_ignore_ascii_case("max") &&
        !function_name.eq_ignore_ascii_case("first_value") &&
        !function_name.eq_ignore_ascii_case("last_value") &&
        !function_name.eq_ignore_ascii_case("nth_value")
    {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "SELECT window function '{}' is not supported yet",
            function.name
        )));
    }

    let has_arguments = match &function.args {
        
        FunctionArguments::None => false,
        
        FunctionArguments::List(list) => !list.args.is_empty(),
        
        FunctionArguments::Subquery(_) => true,

    };

    if (function_name.eq_ignore_ascii_case("row_number") ||
        function_name.eq_ignore_ascii_case("rank") ||
        function_name.eq_ignore_ascii_case("dense_rank") ||
        function_name.eq_ignore_ascii_case("percent_rank") ||
        function_name.eq_ignore_ascii_case("cume_dist")) &&
        has_arguments
    {
        return Err(SqlParseError::UnsupportedStatement(
            format!(
                "{} window function does not accept arguments in the current execution model",
                function.name.to_string().to_ascii_uppercase()
            ),
        ));
    }

    if function_name.eq_ignore_ascii_case("sum") ||
        function_name.eq_ignore_ascii_case("count") ||
        function_name.eq_ignore_ascii_case("avg") ||
        function_name.eq_ignore_ascii_case("min") ||
        function_name.eq_ignore_ascii_case("max") ||
        function_name.eq_ignore_ascii_case("first_value") ||
        function_name.eq_ignore_ascii_case("last_value") ||
        function_name.eq_ignore_ascii_case("ntile")
    {
        
        match &function.args {

            FunctionArguments::List(list) if list.args.len() == 1 => {},

            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    format!(
                        "{} window function currently requires exactly one argument",
                        function.name.to_string().to_ascii_uppercase()
                    ),
                ));
            }

        }

    }

    if function_name.eq_ignore_ascii_case("nth_value") {
        match &function.args {

            FunctionArguments::List(list) if list.args.len() == 2 => {},
            
            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    format!(
                        "{} window function currently requires exactly two arguments",
                        function.name.to_string().to_ascii_uppercase()
                    ),
                ));
            }
            
        }
    }

    if function_name.eq_ignore_ascii_case("lag") || function_name.eq_ignore_ascii_case("lead") {
        match &function.args {

            FunctionArguments::List(list) if (1..=3).contains(&list.args.len()) => {},

            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    format!(
                        "{} window function currently requires between 1 and 3 arguments",
                        function.name.to_string().to_ascii_uppercase()
                    ),
                ));
            }
            
        }
    }

    match function.over.as_ref() {
        
        Some(WindowType::NamedWindow(_)) | Some(WindowType::WindowSpec(_)) => {
            Ok(SelectProjectionItem::WindowFunction {
                output_name,
                function: function.clone(),
            })
        },

        None => Err(SqlParseError::UnsupportedStatement(
            "window projection requires an OVER clause".to_string(),
        )),

    }

}

fn parse_case_projection_value(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectExpression, SqlParseError> {

    match expression {

        Expr::Value(value) => Ok(match parse_default_value(value.to_string()) {
            Some(value) => SelectExpression::Literal(value),
            None => SelectExpression::Null,
        }),

        Expr::Identifier(ident) => {
            ensure_unqualified_allowed(relation_bindings, &ident.value, "CASE THEN/ELSE")?;

            Ok(SelectExpression::Column {
                field_name: common::normalize_identifier!(&ident.value),
            })
        },

        Expr::CompoundIdentifier(parts) => Ok(SelectExpression::Column {
            field_name: parse_qualified_field_name(parts, relation_bindings, "CASE THEN/ELSE")?,
        }),

        Expr::Function(function) => {
            let function_name = function.name.to_string();

            if !is_supported_sql_function(&function_name) {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "CASE expression function '{}' is not supported",
                    function_name
                )));
            }

            Ok(SelectExpression::InbuiltFunction {
                function: function.clone(),
            })
        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "CASE expressions currently support only literals, direct columns, and inbuilt functions".to_string(),
        )),

    }

}

fn ensure_condition_has_no_subqueries(
    condition: &SelectCondition,
    location: &str,
) -> Result<(), SqlParseError> {

    match condition {

        SelectCondition::And(children) | SelectCondition::Or(children) => {
            for child in children {
                ensure_condition_has_no_subqueries(child, location)?;
            }
            Ok(())
        },

        SelectCondition::Not(child) => ensure_condition_has_no_subqueries(child, location),

        SelectCondition::Predicate(predicate) => match predicate {
            SelectPredicate::InSubquery { .. }
            | SelectPredicate::ScalarSubqueryComparison { .. }
            | SelectPredicate::AnySubqueryComparison { .. }
            | SelectPredicate::AllSubqueryComparison { .. }
            | SelectPredicate::Exists { .. } => Err(SqlParseError::UnsupportedStatement(
                format!("{location} subqueries are not supported yet"),
            )),

            _ => Ok(()),
        },
    
    }

}

fn is_inbuilt_projection_item(item: &SelectProjectionItem) -> bool {
    matches!(item, SelectProjectionItem::InbuiltFunction { .. } | SelectProjectionItem::WindowFunction { .. })
}

fn is_projection_only_without_from_item(item: &SelectProjectionItem) -> bool {
    
    matches!(
        item,
        SelectProjectionItem::InbuiltFunction { .. } |
        SelectProjectionItem::Case { .. } |
        SelectProjectionItem::WindowFunction { .. }
    )

}

pub fn parse_select_condition_from_expr(
    selection: Option<&Expr>,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Option<SelectCondition>, SqlParseError> {

    let Some(selection) = selection else {
        return Ok(None);
    };

    Ok(Some(parse_select_condition_expression(selection, relation_bindings)?))
}

pub fn derive_relation_pushdown_conditions(
    condition: Option<&SelectCondition>,
    relation_bindings: &[SelectRelationBinding],
    joins: &[SelectJoin],
) -> Vec<Option<SelectCondition>> {

    if relation_bindings.is_empty() {
        return Vec::new();
    }

    let mut per_relation = vec![Vec::new(); relation_bindings.len()];

    let Some(condition) = condition else {
        return vec![None; relation_bindings.len()];
    };

    let clauses = flatten_and_clauses(condition);

    #[expect(clippy::single_match, reason="the function may be extended in the future to support more condition types that can be pushed down, but currently only one type is supported")]
    for clause in clauses {

        match relation_index_for_condition(clause, relation_bindings) {
            
            Some(index) => {
                if is_safe_relation_pushdown(index, joins)
                    && let Some(localized) = localize_condition_for_relation(clause)
                {
                    per_relation[index].push(localized);
                }
            },

            None => {}

        }

    }

    per_relation
        .into_iter()
        .map(combine_conditions)
        .collect::<Vec<_>>()

}

fn flatten_and_clauses(condition: &SelectCondition) -> Vec<&SelectCondition> {

    match condition {
        
        SelectCondition::And(children) => children
            .iter()
            .flat_map(flatten_and_clauses)
            .collect::<Vec<_>>(),
        
        _ => vec![condition],

    }
    
}

fn combine_conditions(conditions: Vec<SelectCondition>) -> Option<SelectCondition> {

    match conditions.len() {
        
        0 => None,

        1 => conditions.into_iter().next(),

        _ => Some(SelectCondition::And(conditions)),

    }

}

fn relation_index_for_condition(
    condition: &SelectCondition,
    relation_bindings: &[SelectRelationBinding],
) -> Option<usize> {

    let field_name = match condition {
        
        SelectCondition::Predicate(SelectPredicate::Comparison { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::Like { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::Regex { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::InList { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::IsNull { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::InSubquery { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::ScalarSubqueryComparison { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::AnySubqueryComparison { field_name, .. }) |
        SelectCondition::Predicate(SelectPredicate::AllSubqueryComparison { field_name, .. }) => field_name,

        SelectCondition::Predicate(SelectPredicate::FieldComparison { .. }) => return None,

        _ => return None,

    };

    let (qualifier, _) = field_name.split_once('.')?;

    relation_bindings.iter().position(|binding| {
        binding.alias.as_deref() == Some(qualifier) || binding.table_id == qualifier
    })

}

fn is_safe_relation_pushdown(relation_index: usize, joins: &[SelectJoin]) -> bool {
    if joins.is_empty() || relation_index == 0 {
        return true;
    }

    joins.iter().all(|join| matches!(join.kind, SelectJoinKind::Inner))
}

fn localize_condition_for_relation(condition: &SelectCondition) -> Option<SelectCondition> {

    match condition {

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op,
            value,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: unqualify_field_name(field_name)?,
            op: op.clone(),
            value: value.clone(),
        })),

        SelectCondition::Predicate(SelectPredicate::Like {
            field_name,
            pattern,
            negated,
            case_insensitive,
            escape_char,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Like {
            field_name: unqualify_field_name(field_name)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
            escape_char: *escape_char,
        })),

        SelectCondition::Predicate(SelectPredicate::Regex {
            field_name,
            pattern,
            negated,
            case_insensitive,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Regex {
            field_name: unqualify_field_name(field_name)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
        })),

        SelectCondition::Predicate(SelectPredicate::InList {
            field_name,
            values,
            negated,
        }) => Some(SelectCondition::Predicate(SelectPredicate::InList {
            field_name: unqualify_field_name(field_name)?,
            values: values.clone(),
            negated: *negated,
        })),

        SelectCondition::Predicate(SelectPredicate::IsNull { field_name, negated }) => {
            Some(SelectCondition::Predicate(SelectPredicate::IsNull {
                field_name: unqualify_field_name(field_name)?,
                negated: *negated,
            }))
        }

        SelectCondition::Predicate(SelectPredicate::InSubquery {
            field_name,
            subquery,
            negated,
        }) => Some(SelectCondition::Predicate(SelectPredicate::InSubquery {
            field_name: unqualify_field_name(field_name)?,
            subquery: subquery.clone(),
            negated: *negated,
        })),

        SelectCondition::Predicate(SelectPredicate::ScalarSubqueryComparison {
            field_name,
            op,
            subquery,
        }) => Some(SelectCondition::Predicate(SelectPredicate::ScalarSubqueryComparison {
            field_name: unqualify_field_name(field_name)?,
            op: op.clone(),
            subquery: subquery.clone(),
        })),

        SelectCondition::Predicate(SelectPredicate::AnySubqueryComparison {
            field_name,
            op,
            subquery,
        }) => Some(SelectCondition::Predicate(SelectPredicate::AnySubqueryComparison {
            field_name: unqualify_field_name(field_name)?,
            op: op.clone(),
            subquery: subquery.clone(),
        })),

        SelectCondition::Predicate(SelectPredicate::AllSubqueryComparison {
            field_name,
            op,
            subquery,
        }) => Some(SelectCondition::Predicate(SelectPredicate::AllSubqueryComparison {
            field_name: unqualify_field_name(field_name)?,
            op: op.clone(),
            subquery: subquery.clone(),
        })),

        SelectCondition::Predicate(SelectPredicate::FieldComparison { .. }) => None,

        _ => None,
    
    }

}

fn unqualify_field_name(field_name: &str) -> Option<String> {
    
    field_name
        .split_once('.')
        .map(|(_, field_name)| field_name.to_string())
        .or_else(|| Some(field_name.to_string()))

}

fn parse_select_condition_expression(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {

    match expression {

        Expr::Nested(inner) => parse_select_condition_expression(inner, relation_bindings),

        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => parse_like_condition_expression(
            expr,
            pattern,
            *negated,
            true,
            escape_char.as_deref(),
            relation_bindings,
        ),

        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
        } => parse_like_condition_expression(
            expr,
            pattern,
            *negated,
            true,
            escape_char.as_deref(),
            relation_bindings,
        ),

        Expr::RLike {
            negated,
            expr,
            pattern,
            regexp: _,
        } => parse_regex_condition_expression(
            expr,
            pattern,
            *negated,
            relation_bindings,
        ),

        Expr::BinaryOp { left, op, right } => match op {

            BinaryOperator::And => Ok(SelectCondition::And(vec![
                parse_select_condition_expression(left, relation_bindings)?,
                parse_select_condition_expression(right, relation_bindings)?,
            ])),
            
            BinaryOperator::Or => Ok(SelectCondition::Or(vec![
                parse_select_condition_expression(left, relation_bindings)?,
                parse_select_condition_expression(right, relation_bindings)?,
            ])),

            BinaryOperator::Eq |
            BinaryOperator::NotEq |
            BinaryOperator::Gt |
            BinaryOperator::GtEq |
            BinaryOperator::Lt |
            BinaryOperator::LtEq => {

                let op = parse_select_comparison_op(op)?;

                let left_field_name = parse_condition_column_name(left, relation_bindings);
                let right_field_name = parse_condition_column_name(right, relation_bindings);

                match (left_field_name, right_field_name) {

                    (Ok(left_field_name), Ok(right_field_name)) => {
                        Ok(SelectCondition::Predicate(SelectPredicate::FieldComparison {
                            left_field_name,
                            op,
                            right_field_name,
                        }))
                    },

                    (Ok(field_name), Err(_)) => {
                        if let Some(subquery_plan) = parse_scalar_subquery_plan(right)? {
                            Ok(SelectCondition::Predicate(SelectPredicate::ScalarSubqueryComparison {
                                field_name,
                                op,
                                subquery: Box::new(subquery_plan),
                            }))
                        } else if let Some(right_field_name) = parse_unbound_field_reference(right) {
                            Ok(SelectCondition::Predicate(SelectPredicate::FieldComparison {
                                left_field_name: field_name,
                                op,
                                right_field_name,
                            }))
                        } else {
                            let value = parse_condition_literal_value(right)?;

                            Ok(SelectCondition::Predicate(SelectPredicate::Comparison {
                                field_name,
                                op,
                                value,
                            }))
                        }
                    },

                    (Err(_), Ok(field_name)) => {
                        if let Some(subquery_plan) = parse_scalar_subquery_plan(left)? {
                            Ok(SelectCondition::Predicate(SelectPredicate::ScalarSubqueryComparison {
                                field_name,
                                op: reverse_select_comparison_op(&op),
                                subquery: Box::new(subquery_plan),
                            }))
                        } else {
                            Err(SqlParseError::UnsupportedStatement(
                                "WHERE currently supports field-to-field comparisons only when the left side is a column reference".to_string(),
                            ))
                        }
                    },

                    (Err(_), Err(_)) => Err(SqlParseError::UnsupportedStatement(
                        "WHERE comparison requires a column reference on at least one side".to_string(),
                    )),

                }
            
            },
            
            _ => Err(SqlParseError::UnsupportedStatement(
                "WHERE operator is not supported yet".to_string(),
            )),

        },

        Expr::AnyOp {
            left,
            compare_op,
            right,
        } => {

            let field_name = parse_condition_column_name(left, relation_bindings)?;
            let op = parse_select_comparison_op(compare_op)?;
            let Some(subquery_plan) = parse_single_column_subquery_plan(
                right,
                "WHERE ANY comparison requires a subquery selecting exactly one column",
            )? else {
                return Err(SqlParseError::UnsupportedStatement(
                    "WHERE ANY comparison currently supports only subquery right operands"
                        .to_string(),
                ));
            };

            Ok(SelectCondition::Predicate(SelectPredicate::AnySubqueryComparison {
                field_name,
                op,
                subquery: Box::new(subquery_plan),
            }))

        },

        Expr::AllOp {
            left,
            compare_op,
            right,
        } => {
            let field_name = parse_condition_column_name(left, relation_bindings)?;
            let op = parse_select_comparison_op(compare_op)?;
            let Some(subquery_plan) = parse_single_column_subquery_plan(
                right,
                "WHERE ALL comparison requires a subquery selecting exactly one column",
            )? else {
                return Err(SqlParseError::UnsupportedStatement(
                    "WHERE ALL comparison currently supports only subquery right operands"
                        .to_string(),
                ));
            };

            Ok(SelectCondition::Predicate(SelectPredicate::AllSubqueryComparison {
                field_name,
                op,
                subquery: Box::new(subquery_plan),
            }))
        },

        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(SelectCondition::Not(Box::new(
            parse_select_condition_expression(expr, relation_bindings)?,
        ))),

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
            let mut values = Vec::with_capacity(list.len());

            for item in list {
                values.push(parse_condition_literal_value(item)?);
            }

            Ok(SelectCondition::Predicate(SelectPredicate::InList {
                field_name,
                values,
                negated: *negated,
            }))
        },

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
            let subquery_plan = parse_select_read_plan_from_query(subquery.as_ref(), false)?;

            if !supports_single_column_subquery(&subquery_plan) {
                return Err(SqlParseError::UnsupportedStatement(
                    "WHERE subquery membership requires selecting exactly one column".to_string(),
                ));
            }

            Ok(SelectCondition::Predicate(SelectPredicate::InSubquery {
                field_name,
                subquery: Box::new(subquery_plan),
                negated: *negated,
            }))
        },

        Expr::Exists { subquery, negated } => {
            let subquery_plan = parse_select_read_plan_from_query(subquery.as_ref(), false)?;

            Ok(SelectCondition::Predicate(SelectPredicate::Exists {
                subquery: Box::new(subquery_plan),
                negated: *negated,
            }))
        },

        Expr::IsNull(expr) => Ok(SelectCondition::Predicate(SelectPredicate::IsNull {
            field_name: parse_condition_column_name(expr, relation_bindings)?,
            negated: false,
        })),

        Expr::IsNotNull(expr) => Ok(SelectCondition::Predicate(SelectPredicate::IsNull {
            field_name: parse_condition_column_name(expr, relation_bindings)?,
            negated: true,
        })),

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
            let low_value = parse_condition_literal_value(low)?;
            let high_value = parse_condition_literal_value(high)?;

            let between = SelectCondition::And(vec![
                
                SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: field_name.clone(),
                    op: SelectComparisonOp::GtEq,
                    value: low_value,
                }),
                
                SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name,
                    op: SelectComparisonOp::LtEq,
                    value: high_value,
                }),

            ]);

            if *negated {
                Ok(SelectCondition::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "WHERE expression is not supported yet".to_string(),
        )),

    }

}

fn parse_condition_column_name(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
) -> Result<String, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let field_name = match expression {

        Expr::Identifier(ident) => {
            ensure_unqualified_allowed(relation_bindings, &ident.value, "WHERE clause")?;
            common::normalize_identifier!(&ident.value)
        },

        Expr::CompoundIdentifier(parts) => {
            parse_qualified_field_name(parts, relation_bindings, "WHERE clause")?
        },

        _ => {
            return Err(SqlParseError::UnsupportedStatement(
                "WHERE currently supports only direct column predicates".to_string(),
            ));
        },

    };

    Ok(field_name)

}

fn parse_scalar_subquery_plan(expression: &Expr) -> Result<Option<SelectReadPlan>, SqlParseError> {
    
    parse_single_column_subquery_plan(
        expression,
        "WHERE scalar subquery comparison requires selecting exactly one column",
    )

}

fn parse_single_column_subquery_plan(
    expression: &Expr,
    message: &str,
) -> Result<Option<SelectReadPlan>, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let Expr::Subquery(subquery) = expression else {
        return Ok(None);
    };

    let subquery_plan = parse_select_read_plan_from_query(subquery.as_ref(), false)?;

    if !supports_single_column_subquery(&subquery_plan) {
        return Err(SqlParseError::UnsupportedStatement(
            message.to_string(),
        ));
    }

    Ok(Some(subquery_plan))

}

fn parse_select_comparison_op(op: &BinaryOperator) -> Result<SelectComparisonOp, SqlParseError> {

    match op {

        BinaryOperator::Eq => Ok(SelectComparisonOp::Eq),

        BinaryOperator::NotEq => Ok(SelectComparisonOp::NotEq),

        BinaryOperator::Gt => Ok(SelectComparisonOp::Gt),

        BinaryOperator::GtEq => Ok(SelectComparisonOp::GtEq),

        BinaryOperator::Lt => Ok(SelectComparisonOp::Lt),

        BinaryOperator::LtEq => Ok(SelectComparisonOp::LtEq),

        _ => Err(SqlParseError::UnsupportedStatement(
            "WHERE comparison operator is not supported yet".to_string(),
        )),
    
    }

}

fn reverse_select_comparison_op(op: &SelectComparisonOp) -> SelectComparisonOp {

    match op {

        SelectComparisonOp::Eq => SelectComparisonOp::Eq,

        SelectComparisonOp::NotEq => SelectComparisonOp::NotEq,

        SelectComparisonOp::Gt => SelectComparisonOp::Lt,

        SelectComparisonOp::GtEq => SelectComparisonOp::LtEq,

        SelectComparisonOp::Lt => SelectComparisonOp::Gt,

        SelectComparisonOp::LtEq => SelectComparisonOp::GtEq,
        
    }

}

fn parse_like_condition_expression(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    escape_char: Option<&str>,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {

    let escape_char = parse_like_escape_character(escape_char)?;

    let field_name = parse_condition_column_name(expr, relation_bindings)?;
    let pattern = parse_condition_literal_value(pattern)?;

    Ok(SelectCondition::Predicate(SelectPredicate::Like {
        field_name,
        pattern,
        negated,
        case_insensitive,
        escape_char,
    }))

}

fn parse_like_escape_character(escape_char: Option<&str>) -> Result<Option<char>, SqlParseError> {

    let Some(raw_escape) = escape_char else {
        return Ok(None);
    };

    let Some(parsed_escape) = parse_default_value(raw_escape.to_string()) else {
        return Err(SqlParseError::UnsupportedStatement(
            "LIKE ESCAPE must be a single character literal".to_string(),
        ));
    };

    let escape_text = String::from_utf8(parsed_escape).map_err(|_| {
        SqlParseError::UnsupportedStatement(
            "LIKE ESCAPE must be a valid UTF-8 single character literal".to_string(),
        )
    })?;

    let mut chars = escape_text.chars();
    let Some(escape) = chars.next() else {
        return Err(SqlParseError::UnsupportedStatement(
            "LIKE ESCAPE must not be empty".to_string(),
        ));
    };

    if chars.next().is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "LIKE ESCAPE currently supports only a single character".to_string(),
        ));
    }

    Ok(Some(escape))

}

fn supports_single_column_subquery(subquery_plan: &SelectReadPlan) -> bool {

    if subquery_plan.projection_items.len() != 1 {
        return false;
    }

    matches!(
        subquery_plan.projection_items.first(),
        Some(
            SelectProjectionItem::Column { .. }
                | SelectProjectionItem::InbuiltFunction { .. }
                | SelectProjectionItem::Case { .. }
        )
    )

}

fn parse_query_limit(limit: Option<&Expr>) -> Result<Option<usize>, SqlParseError> {
    let Some(limit) = limit else {
        return Ok(None);
    };

    Ok(Some(parse_unsigned_numeric_expression(limit, "LIMIT")?))
}

#[derive(Debug, Clone, Copy, Default)]
struct SelectTopLimit {
    limit: Option<usize>,
    percent: Option<usize>,
    with_ties: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct QueryFetchLimit {
    limit: Option<usize>,
    percent: Option<usize>,
    with_ties: bool,
}

fn parse_select_top_limit(
    top: Option<&sqlparser::ast::Top>,
) -> Result<SelectTopLimit, SqlParseError> {

    let Some(top) = top else {
        return Ok(SelectTopLimit::default());
    };

    let rendered = top.to_string();
    let lowered = rendered.to_ascii_lowercase();

    let with_ties = lowered.contains("with ties");

    let mut parsed_limit = None;

    for token in lowered.split_whitespace() {
        if let Ok(value) = token.parse::<usize>() {
            parsed_limit = Some(value);
            break;
        }
    }

    let Some(parsed_limit) = parsed_limit else {
        return Err(SqlParseError::UnsupportedStatement(
            "SELECT TOP currently supports only unsigned numeric literals".to_string(),
        ));
    };

    if lowered.contains("percent") {
        return Ok(SelectTopLimit {
            limit: None,
            percent: Some(parsed_limit),
            with_ties,
        });
    }

    Ok(SelectTopLimit {
        limit: Some(parsed_limit),
        percent: None,
        with_ties,
    })

}

fn parse_query_limit_or_fetch(
    limit: Option<&Expr>,
    fetch: Option<&sqlparser::ast::Fetch>,
    clause_name: &str,
) -> Result<Option<usize>, SqlParseError> {

    Ok(parse_query_limit_or_fetch_plan(limit, fetch, clause_name)?.limit)

}

fn parse_query_limit_or_fetch_plan(
    limit: Option<&Expr>,
    fetch: Option<&sqlparser::ast::Fetch>,
    clause_name: &str,
) -> Result<QueryFetchLimit, SqlParseError> {

    let parsed_limit = parse_query_limit(limit)?;
    let parsed_fetch = parse_query_fetch_limit(fetch)?;

    if parsed_limit.is_some() && parsed_fetch.limit.is_some() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{clause_name} currently supports LIMIT or FETCH, but not both"
        )));
    }

    if parsed_limit.is_some() {
        return Ok(QueryFetchLimit {
            limit: parsed_limit,
            percent: None,
            with_ties: false,
        });
    }

    Ok(parsed_fetch)

}

fn parse_query_fetch_limit(
    fetch: Option<&sqlparser::ast::Fetch>,
) -> Result<QueryFetchLimit, SqlParseError> {

    let Some(fetch) = fetch else {
        return Ok(QueryFetchLimit::default());
    };

    let rendered = fetch.to_string();
    let lowered = rendered.to_ascii_lowercase();

    if lowered.contains("percent") {
        let with_ties = lowered.contains("with ties");

        for token in lowered.split_whitespace() {
            if let Ok(value) = token.parse::<usize>() {
                return Ok(QueryFetchLimit {
                    limit: None,
                    percent: Some(value),
                    with_ties,
                });
            }
        }

        return Err(SqlParseError::UnsupportedStatement(
            "FETCH PERCENT currently supports only unsigned numeric literals".to_string(),
        ));
    }

    let with_ties = lowered.contains("with ties");

    for token in lowered.split_whitespace() {
        if let Ok(value) = token.parse::<usize>() {
            return Ok(QueryFetchLimit {
                limit: Some(value),
                percent: None,
                with_ties,
            });
        }
    }

    if lowered.contains(" first row") || lowered.contains(" next row") {
        return Ok(QueryFetchLimit {
            limit: Some(1),
            percent: None,
            with_ties,
        });
    }

    Err(SqlParseError::UnsupportedStatement(
        "FETCH currently supports only unsigned numeric literals".to_string(),
    ))

}

fn parse_select_limit_by_plan(
    limit_by_exprs: &[Expr],
    limit: Option<usize>,
    _offset: Option<&sqlparser::ast::Offset>,
    _fetch: Option<&sqlparser::ast::Fetch>,
    relation_bindings: &[SelectRelationBinding],
    clause_name: &str,
) -> Result<Option<SelectLimitByPlan>, SqlParseError> {

    if limit_by_exprs.is_empty() {
        return Ok(None);
    }

    let Some(per_key_limit) = limit else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{clause_name} LIMIT BY requires LIMIT"
        )));
    };

    let mut fields = Vec::with_capacity(limit_by_exprs.len());
    for expr in limit_by_exprs {
        let field_name = parse_condition_column_name(expr, relation_bindings).map_err(|_| {
            SqlParseError::UnsupportedStatement(
                "SELECT LIMIT BY currently supports only direct column references".to_string(),
            )
        })?;
        fields.push(field_name);
    }

    Ok(Some(SelectLimitByPlan {
        per_key_limit,
        fields,
    }))

}

fn parse_set_query_limit_by_plan(
    limit_by_exprs: &[Expr],
    limit: Option<usize>,
    _offset: Option<&sqlparser::ast::Offset>,
    _fetch: Option<&sqlparser::ast::Fetch>,
) -> Result<Option<SelectLimitByPlan>, SqlParseError> {

    if limit_by_exprs.is_empty() {
        return Ok(None);
    }

    let Some(per_key_limit) = limit else {
        return Err(SqlParseError::UnsupportedStatement(
            "set query LIMIT BY requires LIMIT".to_string(),
        ));
    };

    let mut fields = Vec::with_capacity(limit_by_exprs.len());

    for expr in limit_by_exprs {
        let field_name = match expr {
            Expr::Identifier(identifier) => common::normalize_identifier!(&identifier.value),
            Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                common::normalize_identifier!(&parts[parts.len() - 1].value)
            }
            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    "set query LIMIT BY currently supports only direct column references"
                        .to_string(),
                ));
            }
        };

        fields.push(field_name);
    }

    Ok(Some(SelectLimitByPlan {
        per_key_limit,
        fields,
    }))

}

fn parse_select_compat_order_by_items(
    select: &sqlparser::ast::Select,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Vec<SelectOrderByItem>, SqlParseError> {

    let mut items = Vec::new();
    append_compat_order_by_items(
        &mut items,
        &select.cluster_by,
        relation_bindings,
        "SELECT CLUSTER BY",
    )?;
    append_compat_order_by_items(
        &mut items,
        &select.distribute_by,
        relation_bindings,
        "SELECT DISTRIBUTE BY",
    )?;
    append_compat_order_by_items(
        &mut items,
        &select.sort_by,
        relation_bindings,
        "SELECT SORT BY",
    )?;
    Ok(items)

}

fn append_compat_order_by_items(
    items: &mut Vec<SelectOrderByItem>,
    exprs: &[Expr],
    relation_bindings: &[SelectRelationBinding],
    clause_name: &str,
) -> Result<(), SqlParseError> {

    for expr in exprs {
        let field_name = parse_condition_column_name(expr, relation_bindings).map_err(|_| {
            SqlParseError::UnsupportedStatement(format!(
                "{clause_name} currently supports only direct column references"
            ))
        })?;

        items.push(SelectOrderByItem {
            field_name,
            descending: false,
        });
    }

    Ok(())

}

fn parse_query_offset(offset: Option<&sqlparser::ast::Offset>) -> Result<Option<usize>, SqlParseError> {
    let Some(offset) = offset else {
        return Ok(None);
    };

    Ok(Some(parse_unsigned_numeric_expression(&offset.value, "OFFSET")?))
}

fn parse_unsigned_numeric_expression(
    expression: &Expr,
    clause_name: &str,
) -> Result<usize, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let Expr::Value(sqlparser::ast::Value::Number(value, _)) = expression else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{clause_name} currently supports only unsigned numeric literals"
        )));
    };

    value.parse::<usize>().map_err(|_| {
        SqlParseError::UnsupportedStatement(format!(
            "{clause_name} value '{value}' is out of range"
        ))
    })

}

fn parse_regex_condition_expression(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {

    let field_name = parse_condition_column_name(expr, relation_bindings)?;
    let pattern = parse_condition_literal_value(pattern)?;

    validate_regex_pattern(&pattern).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("REGEXP pattern is invalid: {err}"))
    })?;

    Ok(SelectCondition::Predicate(SelectPredicate::Regex {
        field_name,
        pattern,
        negated,
        case_insensitive: false,
    }))

}

pub fn parse_relation_bindings_from_table_with_joins(
    from: Option<&sqlparser::ast::TableWithJoins>,
    statement: &str,
) -> Result<Vec<SelectRelationBinding>, SqlParseError> {

    let Some(table_with_joins) = from else {
        return Ok(Vec::new());
    };

    let mut relations = Vec::with_capacity(1 + table_with_joins.joins.len());
    relations.push(parse_relation_binding_from_factor(
        &table_with_joins.relation,
        statement,
    )?);

    for join in &table_with_joins.joins {
        relations.push(parse_relation_binding_from_factor(&join.relation, statement)?);
    }

    Ok(relations)

}

fn parse_relation_binding_from_factor(
    relation: &TableFactor,
    statement: &str,
) -> Result<SelectRelationBinding, SqlParseError> {

    validate_supported_table_factor_features(relation)?;

    let TableFactor::Table { name, alias, .. } = relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table SELECT is currently supported".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(&name.to_string());

    if table_id.is_empty() {
        return Err(SqlParseError::MissingIdentifier {
            keyword: "from",
            statement: statement.to_string(),
        });
    }

    Ok(SelectRelationBinding {
        table_id,
        alias: alias
            .as_ref()
            .map(|table_alias| common::normalize_identifier!(&table_alias.name.value)),
    })

}

fn validate_supported_table_factor_features(relation: &TableFactor) -> Result<(), SqlParseError> {

    let TableFactor::Table {
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        ..
    } = relation else {
        return Ok(());
    };

    if args.is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "table-valued FROM arguments are not supported yet".to_string(),
        ));
    }

    let _ = with_hints;

    if version.is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "table version qualifiers are not supported yet".to_string(),
        ));
    }

    if *with_ordinality {
        return Err(SqlParseError::UnsupportedStatement(
            "WITH ORDINALITY table factors are not supported yet".to_string(),
        ));
    }

    if !partitions.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "table PARTITION selection is not supported yet".to_string(),
        ));
    }

    Ok(())

}

pub fn parse_joins_from_table_with_joins(
    from: Option<&sqlparser::ast::TableWithJoins>,
    statement: &str,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Vec<SelectJoin>, SqlParseError> {

    let Some(table_with_joins) = from else {
        return Ok(Vec::new());
    };

    let mut available = Vec::with_capacity(relation_bindings.len());
    if let Some(primary) = relation_bindings.first() {
        available.push(primary.clone());
    }

    let mut joins = Vec::with_capacity(table_with_joins.joins.len());

    for (idx, join) in table_with_joins.joins.iter().enumerate() {
        
        let relation = relation_bindings
            .get(idx + 1)
            .cloned()
            .ok_or_else(|| SqlParseError::UnsupportedStatement(statement.to_string()))?;

        let (kind, on_condition) = match &join.join_operator {
            
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                (SelectJoinKind::Inner, parse_join_on_expression(expr, &available, &relation)?)
            },

            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                (SelectJoinKind::Left, parse_join_on_expression(expr, &available, &relation)?)
            },
            
            JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                (SelectJoinKind::Right, parse_join_on_expression(expr, &available, &relation)?)
            },
            
            JoinOperator::FullOuter(JoinConstraint::On(expr)) => {
                (SelectJoinKind::Full, parse_join_on_expression(expr, &available, &relation)?)
            },

            JoinOperator::Inner(JoinConstraint::Using(attrs)) => {
                (SelectJoinKind::Inner, parse_join_using_expression(attrs, &available, &relation)?)
            },
            
            JoinOperator::LeftOuter(JoinConstraint::Using(attrs)) => {
                (SelectJoinKind::Left, parse_join_using_expression(attrs, &available, &relation)?)
            },
            
            JoinOperator::RightOuter(JoinConstraint::Using(attrs)) => {
                (SelectJoinKind::Right, parse_join_using_expression(attrs, &available, &relation)?)
            },

            JoinOperator::FullOuter(JoinConstraint::Using(attrs)) => {
                (SelectJoinKind::Full, parse_join_using_expression(attrs, &available, &relation)?)
            },

            JoinOperator::CrossJoin => (SelectJoinKind::Cross, SelectCondition::And(Vec::new())),

            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    "only INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN, and JOIN ... USING are currently supported"
                        .to_string(),
                ));
            }

        };

        joins.push(SelectJoin {
            kind,
            relation: relation.clone(),
            on_condition,
        });

        available.push(relation);
    
    }

    Ok(joins)

}

fn parse_join_on_expression(
    expression: &Expr,
    left_relations: &[SelectRelationBinding],
    right_relation: &SelectRelationBinding,
) -> Result<SelectCondition, SqlParseError> {
    let mut relation_bindings = Vec::with_capacity(left_relations.len() + 1);
    relation_bindings.extend_from_slice(left_relations);
    relation_bindings.push(right_relation.clone());

    parse_select_condition_expression(expression, &relation_bindings)
}

fn parse_join_using_expression(
    attrs: &[sqlparser::ast::Ident],
    left_relations: &[SelectRelationBinding],
    right_relation: &SelectRelationBinding,
) -> Result<SelectCondition, SqlParseError> {

    if attrs.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "JOIN USING requires at least one column".to_string(),
        ));
    }

    let Some(left_relation) = left_relations.last() else {
        return Err(SqlParseError::UnsupportedStatement(
            "JOIN USING requires a left relation".to_string(),
        ));
    };

    let left_qualifier = relation_lookup_key(left_relation);
    let right_qualifier = relation_lookup_key(right_relation);
    let mut conditions = Vec::with_capacity(attrs.len());

    for attr in attrs {
        let column_name = common::normalize_identifier!(&attr.value);
        conditions.push(SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name: format!("{left_qualifier}.{column_name}"),
            op: SelectComparisonOp::Eq,
            right_field_name: format!("{right_qualifier}.{column_name}"),
        }));
    }

    Ok(match conditions.len() {
        1 => conditions.into_iter().next().expect("one condition should exist"),
        _ => SelectCondition::And(conditions),
    })

}

fn parse_join_field_name(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let Expr::CompoundIdentifier(parts) = expression else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{} must be a qualified column reference",
            location,
        )));
    };

    parse_join_qualified_field_name(parts, relation_bindings, location)
}

fn parse_qualified_field_name(
    parts: &[sqlparser::ast::Ident],
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {

    if parts.len() == 1 {
        ensure_unqualified_allowed(relation_bindings, &parts[0].value, location)?;
        return Ok(common::normalize_identifier!(&parts[0].value));
    }

    if parts.len() != 2 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "invalid compound identifier in {location}"
        )));
    }

    if relation_bindings.len() == 1 {
        validate_qualifier(&parts[0].value, relation_bindings, location)?;
        return Ok(common::normalize_identifier!(&parts[1].value));
    }

    parse_join_qualified_field_name(parts, relation_bindings, location)

}

fn parse_join_qualified_field_name(
    parts: &[sqlparser::ast::Ident],
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {

    if parts.len() != 2 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "invalid compound identifier in {location}"
        )));
    }

    let qualifier = validate_qualifier(&parts[0].value, relation_bindings, location)?;
    
    Ok(format!("{}.{}", qualifier, common::normalize_identifier!(&parts[1].value)))

}

fn validate_qualified_prefix(
    prefix: &sqlparser::ast::ObjectName,
    relation_bindings: &[SelectRelationBinding],
) -> Result<String, SqlParseError> {

    let parts = &prefix.0;

    if parts.len() != 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "qualified wildcard must use exactly one qualifier".to_string(),
        ));
    }

    validate_qualifier(&parts[0].value, relation_bindings, "SELECT wildcard")

}

fn validate_qualifier(
    qualifier: &str,
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {

    if relation_bindings.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "qualified reference '{}' in {} requires FROM relation",
            qualifier, location
        )));
    }

    let normalized = common::normalize_identifier!(qualifier);

    for binding in relation_bindings {
        let matches_table = normalized == binding.table_id;
        let matches_alias = binding
            .alias
            .as_ref()
            .is_some_and(|alias| normalized == *alias);

        if matches_table || matches_alias {
            return Ok(relation_lookup_key(binding));
        }
    }

    Err(SqlParseError::UnsupportedStatement(format!(
        "unknown relation qualifier '{}' in {}",
        qualifier, location
    )))

}

fn relation_lookup_key(binding: &SelectRelationBinding) -> String {
    binding.alias.clone().unwrap_or_else(|| binding.table_id.clone())
}

fn ensure_unqualified_allowed(
    relation_bindings: &[SelectRelationBinding],
    field_name: &str,
    location: &str,
) -> Result<(), SqlParseError> {

    if relation_bindings.len() > 1 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{} requires qualified column '{}' when JOIN is present",
            location, field_name
        )));
    }

    Ok(())
    
}

fn parse_condition_literal_value(expression: &Expr) -> Result<Vec<u8>, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    if let Expr::Function(function) = expression {
        let value = evaluate_sql_function(function)?;

        return value.ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "WHERE inbuilt function returned NULL; use IS NULL where appropriate".to_string(),
            )
        });
    }

    if !matches!(expression, Expr::Value(_)) {
        return Err(SqlParseError::UnsupportedStatement(
            "WHERE currently supports only literal values".to_string(),
        ));
    }

    let Some(value) = parse_default_value(expression.to_string()) else {
        return Err(SqlParseError::UnsupportedStatement(
            "WHERE comparison against NULL is not supported; use IS NULL".to_string(),
        ));
    };

    Ok(value)

}

fn parse_unbound_field_reference(expression: &Expr) -> Option<String> {
    let expression = unwrap_nested_expression(expression);

    match expression {
        Expr::Identifier(ident) => Some(common::normalize_identifier!(&ident.value)),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(format!(
            "{}.{}",
            common::normalize_identifier!(&parts[0].value),
            common::normalize_identifier!(&parts[1].value)
        )),
        _ => None,
    }
}

fn unwrap_nested_expression(expression: &Expr) -> &Expr {
    let mut current = expression;

    while let Expr::Nested(inner) = current {
        current = inner;
    }

    current
}


#[cfg(test)]
#[path = "select_plan_test.rs"]
mod tests;
