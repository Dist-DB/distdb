use std::collections::{HashMap, HashSet};

use sqlparser::ast::Function;

use crate::engine::sql::{
    evaluate_expression_sql_to_bytes, evaluate_inbuilt_sql_function_with_lookup,
    extract_create_function_action_sql, extract_create_function_return_expression,
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
    collect_indexable_like_filter_for_schema, materialize_relation_rows,
    plan_relation_access, relation_qualifier, row_matches_condition_with_result,
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

    row_matches_condition_with_result(
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

        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return Ok(false);
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return Ok(false);
        };

        let mut scoped_table = table.clone();
        if let Some(stream_id) = catalog.entity_wal_stream_id(&subquery.table_id) {
            scoped_table.entity_id = stream_id;
        }

        let mut index_filter_map = HashMap::new();
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
            &scoped_table,
            allow_index_short_circuit,
            index_filter_map,
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
            &scoped_table,
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

        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return Ok(HashSet::new());
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return Ok(HashSet::new());
        };

        let mut scoped_table = table.clone();
        if let Some(stream_id) = catalog.entity_wal_stream_id(&subquery.table_id) {
            scoped_table.entity_id = stream_id;
        }

        let mut index_filter_map = HashMap::new();

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
            &scoped_table,
            allow_index_short_circuit,
            index_filter_map,
            like_filter,
        );

        return execute_relation_select_plan(
            wal,
            &scoped_table,
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

        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return Ok(None);
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return Ok(None);
        };

        let mut index_filter_map = HashMap::new();
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
            table,
            allow_index_short_circuit,
            index_filter_map,
            like_filter,
        );

        return execute_relation_select_plan(
            wal,
            table,
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
    let schema = catalog
        .table_schema(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let table = catalog
        .table(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let mut scoped_table = table.clone();
    if let Some(stream_id) = catalog.entity_wal_stream_id(table_id) {
        scoped_table.entity_id = stream_id;
    }

    let mut index_filter_map = HashMap::new();
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
        &scoped_table,
        allow_index_short_circuit,
        index_filter_map,
        like_filter,
    );

    execute_relation_select_plan(
        wal,
        &scoped_table,
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
            local_function,
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

    let inbound_parameters = parameter_names
        .into_iter()
        .zip(argument_values)
        .collect::<Vec<_>>();

    let inbound_provider = inbound_parameters
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();

    let artifact = local_function.compiled_artifact().ok_or_else(|| {
        format!(
            "function '{}' compiled artifact is not available",
            local_function.procedure_id,
        )
    })?;

    let inbound_provider = inbound_provider
        .into_iter()
        .collect::<HashMap<_, _>>();

    if let Some(return_expression) = extract_create_function_return_expression(&local_function.sql)
        .map_err(|err| format!("function '{}' return extraction failed: {err}", local_function.procedure_id))?
    {
        let value = evaluate_expression_sql_to_bytes(
            &return_expression,
            &mut |field_name| inbound_provider.get(field_name).cloned(),
            &mut |nested, nested_lookup| {
                execute_sql_function_with_lookup(catalog, wal, runtime_indexes, nested, nested_lookup)
            },
        )?;

        return Ok(Some(value));
    }

    let action_sql = if let Some(plan) = artifact.ir.if_else_end_plan() {
        super::commands::execute_if_else_end_plan(&inbound_provider, plan, &mut |sql| {
            Ok(sql.to_string())
        })?
    } else {
        Some(extract_create_function_action_sql(&local_function.sql).map_err(|err| {
            format!("function '{}' action extraction failed: {err}", local_function.procedure_id)
        })?)
    };

    let Some(action_sql) = action_sql else {
        return Ok(None);
    };

    let read_plan = parse_select_read_plan_from_statement(&action_sql)
        .map_err(|err| format!("function '{}' action parse failed: {err}", local_function.procedure_id))?;

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
            local_function.procedure_id,
        ));
    }

    single_scalar_value(result).map_err(|err| {
        format!("function '{}' scalar evaluation failed: {err}", local_function.procedure_id)
    })

}

pub use super::commands::{
    execute_joined_select_plan, execute_projection_only_select_plan,
    execute_relation_select_plan, explain_joined_select_plan_result,
    explain_select_plan_result,
};


#[cfg(test)]
#[path = "select_test.rs"]
mod tests;
