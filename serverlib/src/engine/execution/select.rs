use std::collections::{HashMap, HashSet};

use sqlparser::ast::Function;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseTable, FieldDef, FieldIndex,
    FieldType, RelationAccessPlan, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectProjectionItem, SelectReadPlan, SelectRelation, TableSchema,
};
use crate::engine::sql::SelectExpression;

use super::{
    build_joined_row_tuples, collect_indexable_equality_filters, materialize_relation_rows,
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

        return execute_projection_only_select_plan(subquery, &mut |function| {
            evaluate_inbuilt_sql_function(function)
        })
        .map(|result| !result.rows.is_empty());

    }

    if subquery.joins.is_empty() {

        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return Ok(false);
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return Ok(false);
        };

        let mut index_filter_map = HashMap::new();
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_equality_filters(condition, &mut index_filter_map))
            .unwrap_or(true);

        let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);
        let qualifier = subquery
            .relations
            .first()
            .map(relation_qualifier)
            .unwrap_or(&subquery.table_id)
            .to_string();

        let result = execute_relation_select_plan(
            wal,
            table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut |function| evaluate_inbuilt_sql_function(function),
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
        &mut |function| evaluate_inbuilt_sql_function(function),
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
        return execute_projection_only_select_plan(subquery, &mut |function| {
            evaluate_inbuilt_sql_function(function)
        })
        .map(first_column_values);
    }

    if subquery.joins.is_empty() {

        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return Ok(HashSet::new());
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return Ok(HashSet::new());
        };

        let mut index_filter_map = HashMap::new();
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_equality_filters(condition, &mut index_filter_map))
            .unwrap_or(true);

        let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);

        return execute_relation_select_plan(
            wal,
            table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut |function| evaluate_inbuilt_sql_function(function),
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
        &mut |function| evaluate_inbuilt_sql_function(function),
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
        return execute_projection_only_select_plan(subquery, &mut |function| {
            evaluate_inbuilt_sql_function(function)
        })
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
        let allow_index_short_circuit = subquery
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_equality_filters(condition, &mut index_filter_map))
            .unwrap_or(true);

        let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);

        return execute_relation_select_plan(
            wal,
            table,
            schema,
            runtime_indexes,
            subquery,
            &access_plan,
            &mut |function| evaluate_inbuilt_sql_function(function),
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
        &mut |function| evaluate_inbuilt_sql_function(function),
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

pub use super::commands::{
    execute_joined_select_plan, execute_projection_only_select_plan,
    execute_relation_select_plan, explain_joined_select_plan_result,
    explain_select_plan_result,
};


#[cfg(test)]
#[path = "select_test.rs"]
mod tests;
