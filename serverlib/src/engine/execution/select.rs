use std::collections::{HashMap, HashSet};

use sqlparser::ast::Function;

use crate::engine::sql::{
    evaluate_expression_sql_to_bytes, evaluate_inbuilt_sql_function_with_lookup,
    function_argument_values, parse_create_function_parameter_names_from_statement,
    parse_select_read_plan_from_statement,
    SqlFunctionEvaluationStrategy, with_lookup_sql_function_evaluator,
};

use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseTable, DatabaseStoredProcedure,
    FieldDef, FieldIndex, FieldType, RelationAccessPlan, RuntimeIndexStore, SelectCondition,
    SelectJoin,
    SelectJoinKind, SelectProjectionItem, SelectReadPlan, SelectRelation,
    TableSchema,
};

use crate::engine::sql::SelectExpression;

use super::{
    build_joined_row_tuples, collect_indexable_equality_filters_for_schema,
    collect_indexable_like_filter_for_schema, collect_indexable_range_filters_for_schema,
    materialize_relation_rows,
    plan_relation_access, relation_qualifier, row_matches_condition_with_result_and_expression,
    ConditionValueProvider, JoinedRowTuple,
};

use super::runtime::{
    ChainedConditionValueProvider, QualifiedRowMapProvider, UnqualifiedFieldFallbackProvider,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectExecutionResult {
    pub columns: Vec<FieldDef>,
    pub rows: Vec<Vec<Vec<u8>>>,
}

pub fn row_matches_select_condition(
    provider: &dyn ConditionValueProvider,
    condition: Option<&SelectCondition>,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
) -> bool {

    row_matches_select_condition_result(provider, condition, catalog, wal, runtime_indexes)
        .unwrap_or(false)

}

pub fn row_matches_select_condition_result(
    provider: &dyn ConditionValueProvider,
    condition: Option<&SelectCondition>,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
) -> Result<bool, String> {

    row_matches_select_condition_with_outer_result(
        provider,
        provider,
        condition,
        catalog,
        wal,
        runtime_indexes,
    )

}

fn row_matches_select_condition_with_outer_result(
    provider: &dyn ConditionValueProvider,
    outer_provider: &dyn ConditionValueProvider,
    condition: Option<&SelectCondition>,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
) -> Result<bool, String> {

    let normalized_outer_provider = UnqualifiedFieldFallbackProvider {
        provider: outer_provider,
    };

    let chained_provider = ChainedConditionValueProvider {
        primary: provider,
        fallback: &normalized_outer_provider,
    };

    row_matches_condition_with_result_and_expression(
        &chained_provider,
        condition,
        &mut |current_provider, subquery| {
            collect_subquery_projection_values_with_outer(
                catalog,
                wal,
                runtime_indexes,
                current_provider,
                subquery,
            )
        },
        &mut |current_provider, subquery| {
            collect_subquery_exists_with_outer(
                catalog,
                wal,
                runtime_indexes,
                current_provider,
                subquery,
            )
        },
        &mut |current_provider, subquery| {
            collect_subquery_scalar_value_with_outer(
                catalog,
                wal,
                runtime_indexes,
                current_provider,
                subquery,
            )
        },
        &mut |current_provider, expression_sql| {
            evaluate_expression_sql_to_bytes(
                expression_sql,
                &mut |field_name| current_provider.value(field_name).cloned(),
                &mut |function, lookup| {
                    execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
                },
            )
            .map(Some)
        },
    )

}

fn collect_subquery_exists_with_outer(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    outer_provider: &dyn ConditionValueProvider,
    subquery: &SelectReadPlan,
) -> Result<bool, String> {

    if subquery.is_explain {
        return Ok(false);
    }

    if subquery.table_id.is_empty() {

        return execute_projection_only_select_plan(subquery, &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }))
        .map(|result| !result.rows.is_empty());

    }

    if subquery.joins.is_empty() {

        let Some(table) = catalog
            .table_handle(&subquery.table_id)
            .and_then(|handle| handle.table_snapshot()) else {
            return Ok(false);
        };
        let schema = &table.schema;

        let scoped_table_owned = catalog.entity_wal_stream_id(&subquery.table_id).map(|stream_id| {
            let mut table_with_stream = table.clone();
            table_with_stream.entity_id = stream_id;
            table_with_stream
        });
        let scoped_table = scoped_table_owned.as_ref().unwrap_or(&table);

        let mut index_filter_map = HashMap::new();
        let range_filters = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_range_filters_for_schema(schema, condition))
            .unwrap_or_default();
        let like_filter = subquery
            .where_condition
            .as_ref()
            .and_then(|condition| collect_indexable_like_filter_for_schema(schema, condition));
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| {
                collect_indexable_equality_filters_for_schema(
                    schema,
                    condition,
                    &mut index_filter_map,
                )
            })
            .unwrap_or(true);

        let access_plan = plan_relation_access(
            scoped_table,
            allow_index_short_circuit,
            index_filter_map,
            range_filters,
            like_filter,
        );

        let qualifier = subquery
            .relations
            .first()
            .map(relation_qualifier)
            .unwrap_or(&subquery.table_id)
            .to_string();

        let result = execute_relation_select_plan(
            wal,
            scoped_table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut with_lookup_sql_function_evaluator(|function, lookup| {
                execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
            }),
            &mut |row_map, nested_condition| {

                let row_provider = QualifiedRowMapProvider {
                    qualifier: &qualifier,
                    row_map,
                };

                row_matches_select_condition_with_outer_result(
                    &row_provider,
                    outer_provider,
                    nested_condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )

            },
        );

        return result.map(|result| !result.rows.is_empty());

    }

    execute_joined_select_plan(
        catalog,
        wal,
        runtime_indexes,
        subquery,
        &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }),
        &mut |row_map, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_map,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )

        },
        &mut |row_tuple, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_tuple,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )

        },

    )
    .map(|result| !result.rows.is_empty())

}

fn collect_subquery_projection_values_with_outer(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    outer_provider: &dyn ConditionValueProvider,
    subquery: &SelectReadPlan,
) -> Result<HashSet<Vec<u8>>, String> {

    if subquery.is_explain
        || subquery
            .projection_items
            .iter()
            .any(|item| matches!(item, SelectProjectionItem::Wildcard { .. }))
    {
        return Ok(HashSet::new());
    }

    if subquery.projection_items.len() != 1 {
        return Ok(HashSet::new());
    }

    if subquery.table_id.is_empty() {
        return execute_projection_only_select_plan(subquery, &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }))
        .map(first_column_values);
    }

    if subquery.joins.is_empty() {

        let Some(table) = catalog
            .table_handle(&subquery.table_id)
            .and_then(|handle| handle.table_snapshot()) else {
            return Ok(HashSet::new());
        };
        let schema = &table.schema;

        let scoped_table_owned = catalog.entity_wal_stream_id(&subquery.table_id).map(|stream_id| {
            let mut table_with_stream = table.clone();
            table_with_stream.entity_id = stream_id;
            table_with_stream
        });
        let scoped_table = scoped_table_owned.as_ref().unwrap_or(&table);

        let mut index_filter_map = HashMap::new();
        let range_filters = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_range_filters_for_schema(schema, condition))
            .unwrap_or_default();

        let like_filter = subquery
            .where_condition
            .as_ref()
            .and_then(|condition| collect_indexable_like_filter_for_schema(schema, condition));
        
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| {
                collect_indexable_equality_filters_for_schema(
                    schema,
                    condition,
                    &mut index_filter_map,
                )
            })
            .unwrap_or(true);

        let access_plan = plan_relation_access(
            scoped_table,
            allow_index_short_circuit,
            index_filter_map,
            range_filters,
            like_filter,
        );

        return execute_relation_select_plan(
            wal,
            scoped_table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut with_lookup_sql_function_evaluator(|function, lookup| {
                execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
            }),
            &mut |row_map, nested_condition| {
                row_matches_select_condition_with_outer_result(
                    row_map,
                    outer_provider,
                    nested_condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )
            },
        )
        .map(first_column_values);

    }

    execute_joined_select_plan(
        catalog,
        wal,
        runtime_indexes,
        subquery,
        &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }),
        &mut |row_map, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_map,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )

        },
        &mut |row_tuple, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_tuple,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )

        },
    )
    .map(first_column_values)

}

fn collect_subquery_scalar_value_with_outer(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    outer_provider: &dyn ConditionValueProvider,
    subquery: &SelectReadPlan,
) -> Result<Option<Vec<u8>>, String> {

    if subquery.is_explain
        || subquery
            .projection_items
            .iter()
            .any(|item| matches!(item, SelectProjectionItem::Wildcard { .. }))
    {
        return Ok(None);
    }

    if subquery.projection_items.len() != 1 {
        return Ok(None);
    }

    if subquery.table_id.is_empty() {
        return execute_projection_only_select_plan(subquery, &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }))
        .and_then(single_scalar_value);
    }

    if subquery.joins.is_empty() {

        let Some(table) = catalog
            .table_handle(&subquery.table_id)
            .and_then(|handle| handle.table_snapshot()) else {
            return Ok(None);
        };
        let schema = &table.schema;

        let mut index_filter_map = HashMap::new();
        let range_filters = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_range_filters_for_schema(schema, condition))
            .unwrap_or_default();
        let like_filter = subquery
            .where_condition
            .as_ref()
            .and_then(|condition| collect_indexable_like_filter_for_schema(schema, condition));
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| {
                collect_indexable_equality_filters_for_schema(
                    schema,
                    condition,
                    &mut index_filter_map,
                )
            })
            .unwrap_or(true);

        let access_plan = plan_relation_access(
            &table,
            allow_index_short_circuit,
            index_filter_map,
            range_filters,
            like_filter,
        );

        return execute_relation_select_plan(
            wal,
            &table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut with_lookup_sql_function_evaluator(|function, lookup| {
                execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
            }),
            &mut |row_map, nested_condition| {
                row_matches_select_condition_with_outer_result(
                    row_map,
                    outer_provider,
                    nested_condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )
            },
        )
        .and_then(single_scalar_value);

    }

    execute_joined_select_plan(
        catalog,
        wal,
        runtime_indexes,
        subquery,
        &mut with_lookup_sql_function_evaluator(|function, lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }),
        &mut |row_map, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_map,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )

        },
        &mut |row_tuple, nested_condition| {

            row_matches_select_condition_with_outer_result(
                row_tuple,
                outer_provider,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )
            
        },
    )
    .and_then(single_scalar_value)

}

fn first_column_values(result: SelectExecutionResult) -> HashSet<Vec<u8>> {

    result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()

}

fn single_scalar_value(result: SelectExecutionResult) -> Result<Option<Vec<u8>>, String> {

    let mut rows = result.rows.into_iter();
    let Some(row) = rows.next() else {
        return Ok(None);
    };

    if rows.next().is_some() {
        return Err("select failed: scalar subquery returned more than one row".to_string());
    }

    let mut columns = row.into_iter();
    let Some(value) = columns.next() else {
        return Ok(None);
    };

    if columns.next().is_some() {
        return Err("select failed: scalar subquery returned more than one column".to_string());
    }

    Ok(Some(value))

}

pub fn execute_select_plan_result_with_function_evaluator<E>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
    evaluate_function: &mut E,
) -> Result<SelectExecutionResult, String>
where
    E: SqlFunctionEvaluationStrategy,
{

    if !read_plan.joins.is_empty() {
        return execute_joined_select_plan(
            catalog,
            wal,
            runtime_indexes,
            read_plan,
            evaluate_function,
            &mut |row_map, condition| {
                row_matches_select_condition_with_outer_result(
                    row_map,
                    row_map,
                    condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )
            },
            &mut |row_tuple, condition| {
                row_matches_select_condition_with_outer_result(
                    row_tuple,
                    row_tuple,
                    condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )
            },
        );
    }

    if read_plan.table_id.is_empty() {
        return execute_projection_only_select_plan(read_plan, evaluate_function);
    }

    let table_id = read_plan.table_id.as_str();
    let table = catalog
        .table_handle(table_id)
        .and_then(|handle| handle.table_snapshot())
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;
    let schema = &table.schema;

    let scoped_table_owned = catalog.entity_wal_stream_id(table_id).map(|stream_id| {
        let mut table_with_stream = table.clone();
        table_with_stream.entity_id = stream_id;
        table_with_stream
    });
    let scoped_table = scoped_table_owned.as_ref().unwrap_or(&table);

    let mut index_filter_map = HashMap::new();
    let range_filters = read_plan
        .where_condition
        .as_ref()
        .map(|condition| collect_indexable_range_filters_for_schema(schema, condition))
        .unwrap_or_default();
    let like_filter = read_plan
        .where_condition
        .as_ref()
        .and_then(|condition| collect_indexable_like_filter_for_schema(schema, condition));
    let allow_index_short_circuit = read_plan
        .where_condition
        .as_ref()
        .map(|condition| {
            collect_indexable_equality_filters_for_schema(
                schema,
                condition,
                &mut index_filter_map,
            )
        })
        .unwrap_or(true);

    let access_plan = plan_relation_access(
        scoped_table,
        allow_index_short_circuit,
        index_filter_map,
        range_filters,
        like_filter,
    );

    execute_relation_select_plan(
        wal,
        scoped_table,
        schema,
        runtime_indexes,
        read_plan,
        &access_plan,
        evaluate_function,
        &mut |row_map, condition| {
            row_matches_select_condition_with_outer_result(
                row_map,
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
    )

}

pub fn execute_sql_function_with_lookup(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    function: &Function,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, String> {

    let function_id = common::normalize_identifier!(function.name.to_string());

    if let Some(local_function) = catalog.stored_procedure(&function_id) {
        return execute_local_sql_function_with_lookup(
            catalog,
            wal,
            runtime_indexes,
            &local_function,
            function,
            lookup,
        );
    }

    evaluate_inbuilt_sql_function_with_lookup(function, lookup)

}

fn execute_local_sql_function_with_lookup(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    local_function: &DatabaseStoredProcedure,
    function: &Function,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, String> {

    let parameter_names = parse_create_function_parameter_names_from_statement(&local_function.sql)
        .map_err(|err| format!("function '{}' parameter parse failed: {err}", local_function.procedure_id))?;

    let argument_values = function_argument_values(
        function,
        lookup,
        &mut |nested, nested_lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, nested, nested_lookup)
        },
    )?;

    if parameter_names.len() != argument_values.len() {
        return Err(format!(
            "function '{}' argument mismatch: expected {} values but received {}",
            local_function.procedure_id,
            parameter_names.len(),
            argument_values.len(),
        ));
    }

    let inbound_provider = parameter_names
        .into_iter()
        .zip(argument_values)
        .collect::<HashMap<_, _>>();

    let artifact = local_function.compiled_artifact().ok_or_else(|| {
        format!(
            "function '{}' compiled artifact is not available",
            local_function.procedure_id,
        )
    })?;

    let action_statements = if let Some(plan) = artifact.ir.if_else_end_plan() {
        let Some(action_sql) = super::commands::execute_if_else_end_plan(
            &inbound_provider,
            plan,
            &mut |sql| Ok(sql.to_string()),
        )? else {
            return Ok(None);
        };
        vec![action_sql]
    } else if let Some(action_statements) = artifact.ir.action_statements() {
        action_statements.to_vec()
    } else {
        return Err(format!(
            "function '{}' compiled action statements are unavailable",
            local_function.procedure_id,
        ));
    };

    let mut local_scope = HashMap::new();
    let mut fallback_value = None;

    for action_sql in action_statements {
        match execute_local_function_action_statement(
            catalog,
            wal,
            runtime_indexes,
            &local_function.procedure_id,
            action_sql.as_str(),
            &inbound_provider,
            &mut local_scope,
        )? {
            LocalFunctionActionOutcome::Continue => {}
            LocalFunctionActionOutcome::Scalar(value) => {
                fallback_value = Some(value);
            }
            LocalFunctionActionOutcome::Return(value) => {
                return Ok(Some(value));
            }
        }
    }

    Ok(fallback_value)

}

enum LocalFunctionActionOutcome {
    Continue,
    Scalar(Vec<u8>),
    Return(Vec<u8>),
}

fn execute_local_function_action_statement(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    function_id: &str,
    action_sql: &str,
    inbound_provider: &HashMap<String, Vec<u8>>,
    local_scope: &mut HashMap<String, Vec<u8>>,
) -> Result<LocalFunctionActionOutcome, String> {

    let statement = action_sql.trim().trim_end_matches(';').trim();
    if statement.is_empty() {
        return Ok(LocalFunctionActionOutcome::Continue);
    }

    let lowered = statement.to_ascii_lowercase();

    if lowered.starts_with("set ") {
        let (target, expression) = parse_local_function_set_assignment(statement)?;
        let value = evaluate_local_function_expression_to_value(
            catalog,
            wal,
            runtime_indexes,
            function_id,
            expression,
            inbound_provider,
            local_scope,
        )?;
        local_scope.insert(target, value);
        return Ok(LocalFunctionActionOutcome::Continue);
    }

    if lowered.starts_with("return ") {
        let expression = statement["return".len()..].trim();
        if expression.is_empty() {
            return Err(format!(
                "function '{}' action parse failed: RETURN expression is empty",
                function_id,
            ));
        }

        let value = evaluate_local_function_expression_to_value(
            catalog,
            wal,
            runtime_indexes,
            function_id,
            expression,
            inbound_provider,
            local_scope,
        )?;

        return Ok(LocalFunctionActionOutcome::Return(value));
    }

    if lowered.starts_with("select ") {
        let value = execute_local_function_scalar_select(
            catalog,
            wal,
            runtime_indexes,
            function_id,
            statement,
            inbound_provider,
            local_scope,
        )?
        .unwrap_or_else(|| b"NULL".to_vec());

        return Ok(LocalFunctionActionOutcome::Scalar(value));
    }

    Err(format!(
        "function '{}' action parse failed: unsupported statement '{}'",
        function_id,
        statement,
    ))

}

fn parse_local_function_set_assignment(statement: &str) -> Result<(String, &str), String> {

    let body = statement["set".len()..].trim();
    let eq_index = body.find('=').ok_or_else(|| {
        "local function assignment parse failed: SET statement is missing '='".to_string()
    })?;

    let target = body[..eq_index]
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_start_matches('@')
        .trim();

    if target.is_empty() {
        return Err("local function assignment parse failed: assignment target is empty".to_string());
    }

    let expression = body[(eq_index + 1)..].trim();
    if expression.is_empty() {
        return Err("local function assignment parse failed: assignment value is empty".to_string());
    }

    Ok((common::normalize_identifier!(target), expression))

}

fn evaluate_local_function_expression_to_value(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    function_id: &str,
    expression: &str,
    inbound_provider: &HashMap<String, Vec<u8>>,
    local_scope: &HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>, String> {

    let rewritten_expression = rewrite_local_function_expression_literals(
        expression,
        &mut |identifier| {
            local_scope
                .get(identifier)
                .or_else(|| inbound_provider.get(identifier))
                .map(|value| local_sql_literal_for_bytes(value.as_slice()))
        },
    )?;

    evaluate_expression_sql_to_bytes(
        rewritten_expression.as_str(),
        &mut |_| None,
        &mut |nested, nested_lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, nested, nested_lookup)
        },
    )
    .map_err(|err| format!("function '{}' expression evaluation failed: {err}", function_id))

}

fn execute_local_function_scalar_select(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    function_id: &str,
    scalar_sql: &str,
    inbound_provider: &HashMap<String, Vec<u8>>,
    local_scope: &HashMap<String, Vec<u8>>,
) -> Result<Option<Vec<u8>>, String> {

    let rewritten_sql = rewrite_local_function_expression_literals(
        scalar_sql,
        &mut |identifier| {
            local_scope
                .get(identifier)
                .or_else(|| inbound_provider.get(identifier))
                .map(|value| local_sql_literal_for_bytes(value.as_slice()))
        },
    )?;

    let read_plan = parse_select_read_plan_from_statement(&rewritten_sql)
        .map_err(|err| format!("function '{}' action parse failed: {err}", function_id))?;

    let result = execute_select_plan_result_with_function_evaluator(
        catalog,
        wal,
        runtime_indexes,
        &read_plan,
        &mut with_lookup_sql_function_evaluator(|nested, nested_lookup| {
            execute_sql_function_with_lookup(catalog, wal, runtime_indexes, nested, nested_lookup)
        }),
    )?;

    if result.columns.len() > 1 {
        return Err(format!(
            "function '{}' returned more than one column",
            function_id,
        ));
    }

    single_scalar_value(result).map_err(|err| {
        format!("function '{}' scalar evaluation failed: {err}", function_id)
    })

}

fn rewrite_local_function_expression_literals(
    input: &str,
    resolve_literal: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<String, String> {

    let mut output = String::with_capacity(input.len());
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < chars.len() {
        let ch = chars[i];

        if ch == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            output.push(ch);
            i += 1;
            continue;
        }

        if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            output.push(ch);
            i += 1;
            continue;
        }

        if in_single_quote || in_double_quote {
            output.push(ch);
            i += 1;
            continue;
        }

        if ch == '@' {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
                end += 1;
            }

            if end == start {
                output.push(ch);
                i += 1;
                continue;
            }

            let identifier = chars[start..end].iter().collect::<String>();
            let normalized = common::normalize_identifier!(identifier.as_str());

            if let Some(literal) = resolve_literal(normalized.as_str()) {
                output.push_str(literal.as_str());
            } else {
                output.push('@');
                output.push_str(identifier.as_str());
            }

            i = end;
            continue;
        }

        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            let mut end = i + 1;
            while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
                end += 1;
            }

            let identifier = chars[start..end].iter().collect::<String>();
            let normalized = common::normalize_identifier!(identifier.as_str());

            if let Some(literal) = resolve_literal(normalized.as_str()) {
                output.push_str(literal.as_str());
            } else {
                output.push_str(identifier.as_str());
            }

            i = end;
            continue;
        }

        output.push(ch);
        i += 1;
    }

    if in_single_quote || in_double_quote {
        return Err("local function expression rewrite failed: unclosed quote".to_string());
    }

    Ok(output)

}

fn local_sql_literal_for_bytes(value: &[u8]) -> String {

    if let Ok(text) = std::str::from_utf8(value) {
        let trimmed = text.trim();
        let lowered = trimmed.to_ascii_lowercase();

        if lowered == "true" || lowered == "false" || lowered == "null" {
            return lowered;
        }

        let mut saw_digit = false;
        let mut saw_dot = false;
        let is_numeric = trimmed.chars().enumerate().all(|(idx, ch)| {
            if ch.is_ascii_digit() {
                saw_digit = true;
                return true;
            }

            if (ch == '-' || ch == '+') && idx == 0 {
                return true;
            }

            if ch == '.' && !saw_dot {
                saw_dot = true;
                return true;
            }

            false
        });

        if is_numeric && saw_digit {
            return trimmed.to_string();
        }

        return format!("'{}'", trimmed.replace('\\', "\\\\").replace('\'', "\\'"));
    }

    let hex = value
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    format!("x'{hex}'")

}

pub use super::commands::{
    execute_joined_select_plan, execute_projection_only_select_plan,
    execute_relation_select_plan, explain_joined_select_plan_result,
    explain_select_plan_result,
};


#[cfg(test)]
#[path = "select_test.rs"]
mod tests;
