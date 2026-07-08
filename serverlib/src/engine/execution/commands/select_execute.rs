use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, NamedWindowDefinition,
    NamedWindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowSpec, WindowType,
};

use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseTable, FieldDef, FieldIndex, FieldType,
    RelationAccessPlan, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectProjectionItem, SelectReadPlan, SelectRelation, TableSchema,
    primary_key_index, render_stored_field_value,
};
use crate::engine::sql::{
    evaluate_sql_function_with_lookup, expression_references_column,
    sql_function_references_column, SelectCaseWhen, SqlFunctionEvaluationStrategy,
};

use super::super::{
    build_joined_row_tuples, load_live_row_count, materialize_relation_rows,
    relation_qualifier,
    JoinedRowTuple,
};

use super::super::select::SelectExecutionResult;
use super::control_flow::evaluate_case_projection;
use super::select_explain::explain_joined_select_plan_result;

pub fn execute_projection_only_select_plan<E>(
    read_plan: &SelectReadPlan,
    evaluate_function: &mut E,
) -> Result<SelectExecutionResult, String>
where
    E: SqlFunctionEvaluationStrategy,
{

    let mut columns = Vec::with_capacity(read_plan.projection_items.len());
    let mut row = Vec::with_capacity(read_plan.projection_items.len());

    for (seq, projection_item) in read_plan.projection_items.iter().enumerate() {
        
        match projection_item {

            SelectProjectionItem::InbuiltFunction {
                output_name,
                function,
            } => {

                let value = evaluate_function.evaluate(function, &mut |_| None).map_err(|err| {
                    format!("select failed: inbuilt projection evaluation failed: {err}")
                })?;

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(value.unwrap_or_else(|| b"NULL".to_vec()));

            },

            SelectProjectionItem::Column { .. } => {
                return Err("select without FROM only supports inbuilt projection functions".to_string());
            },

            SelectProjectionItem::WindowFunction { output_name, .. } => {
                
                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(Vec::new());

            },

            SelectProjectionItem::Case { .. } => {

                let SelectProjectionItem::Case {
                    output_name,
                    operand,
                    branches,
                    else_value,
                } = projection_item
                else {
                    unreachable!("case projection match arm should only receive CASE projections")
                };

                if case_projection_requires_row_context(operand.as_ref(), branches, else_value.as_ref()) {
                    return Err(
                        "select without FROM CASE projections support only row-independent expressions"
                            .to_string(),
                    );
                }

                let row_provider = HashMap::<String, Vec<u8>>::new();
                let value = evaluate_case_projection(
                    &row_provider,
                    operand.as_ref(),
                    branches,
                    else_value.as_ref(),
                    evaluate_function,
                )?;

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(value.unwrap_or_else(|| b"NULL".to_vec()));

            },

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select without FROM does not support wildcard projections".to_string());
            }
        
        }

    }

    let rows = apply_select_post_processing(vec![row], &columns, read_plan, &read_plan.projection_items)?;

    Ok(SelectExecutionResult {
        columns,
        rows,
    })

}

#[expect(clippy::too_many_arguments, reason="Necessary for the complex logic of executing SELECT plans across multiple relations, conditions, and projection types")]
pub fn execute_relation_select_plan<E, R>(
    wal: &ConcurrentWalManager,
    table: &DatabaseTable,
    schema: &TableSchema,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
    access_plan: &RelationAccessPlan,
    evaluate_function: &mut E,
    row_matches: &mut R,
) -> Result<SelectExecutionResult, String>
where
    E: SqlFunctionEvaluationStrategy,
    R: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> Result<bool, String>,
{

    let count_star_projection = count_star_projection(read_plan);

    if let Some(output_name) = &count_star_projection {

        // Always materialize count(*) rows for correctness. The strict full-table
        // fast path currently overcounts in grouped WAL histories.

        let mut matched_rows = 0usize;

        for (_, row_map) in materialize_relation_rows(wal, table, schema, runtime_indexes, access_plan) {
            
            if !row_matches(&row_map, read_plan.where_condition.as_ref())? {
                continue;
            }

            matched_rows += 1;
        }

        return Ok(SelectExecutionResult {
            columns: vec![FieldDef {
                seqno: 1,
                field_name: output_name.clone(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }],
            rows: vec![vec![matched_rows.to_string().into_bytes()]],
        });

    }

    let projection_items = expand_relation_projection_items(schema, &read_plan.projection_items);
    let visible_projection_len = projection_items.len();
    let projection_items = ensure_order_by_projection_items(projection_items, read_plan);

    let mut columns = Vec::with_capacity(projection_items.len());

    for (seq, projection_item) in projection_items.iter().enumerate() {
        
        let is_hidden_sort_key = seq >= visible_projection_len;
        
        match projection_item {

            SelectProjectionItem::Column {
                field_name,
                output_name,
            } => {

                let Some(field) = schema.field(field_name) else {
                    return Err(format!("select failed: unknown column '{}'", field_name));
                };

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: field.field_type.clone(),
                    nullable: field.nullable,
                    indexed: field.indexed,
                    default_value: field.default_value.clone(),
                    metadata: column_metadata_with_visibility(field.metadata.clone(), is_hidden_sort_key),
                });
                
            },

            SelectProjectionItem::InbuiltFunction { output_name, .. } => {
                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });
            },

            SelectProjectionItem::Case { output_name, .. } => {
                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });
            },

            SelectProjectionItem::WindowFunction { output_name, .. } => {
                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });
            },

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select failed: wildcard expansion should have been resolved before column building".to_string());
            },

        }

    }

    let mut static_projection_values = Vec::with_capacity(projection_items.len());

    for projection_item in &projection_items {

        match projection_item {
            
            SelectProjectionItem::InbuiltFunction { function, .. } => {
                if sql_function_references_column(function) {
                    static_projection_values.push(None);
                } else {
                    let value = evaluate_function.evaluate(function, &mut |_| None)
                        .map_err(|err| format!("select failed: inbuilt projection evaluation failed: {err}"))?;
                    static_projection_values.push(Some(value));
                }
            },

            SelectProjectionItem::Column { .. } => static_projection_values.push(None),

            SelectProjectionItem::Case { .. } => static_projection_values.push(None),

            SelectProjectionItem::WindowFunction { .. } => static_projection_values.push(None),

            SelectProjectionItem::Wildcard { .. } => static_projection_values.push(None),

        }

    }

    let mut rows = Vec::new();
    
    for (_, row_map) in materialize_relation_rows(wal, table, schema, runtime_indexes, access_plan) {

        if !row_matches(&row_map, read_plan.where_condition.as_ref())? {
            continue;
        }

        let projected_row = projection_items
            .iter()
            .enumerate()
            .map(|(projection_idx, projection_item)| -> Result<Vec<u8>, String> {

                match projection_item {

                    SelectProjectionItem::Column { field_name, .. } => Ok(match row_map.get(field_name) {
                        Some(value) => render_stored_field_value(value),
                        None if columns[projection_idx].nullable => b"NULL".to_vec(),
                        None => Vec::new(),
                    }),

                    SelectProjectionItem::InbuiltFunction { function, .. } => {
                        let static_value = static_projection_values
                            .get(projection_idx)
                            .and_then(|entry| entry.as_ref())
                            .cloned()
                            .flatten();

                        if let Some(value) = static_value {
                            Ok(value)
                        } else {
                            evaluate_function.evaluate(
                                function,
                                &mut |field_name| row_map
                                    .get(field_name)
                                    .map(|value| render_stored_field_value(value)),
                            )
                            .map(|value| value.unwrap_or_else(|| b"NULL".to_vec()))
                            .map_err(|err| {
                                format!("select failed: inbuilt projection evaluation failed: {err}")
                            })
                        }
                    },

                    SelectProjectionItem::Case {
                        operand,
                        branches,
                        else_value,
                        ..
                    } => Ok(
                        render_stored_field_value(&evaluate_case_projection(
                            &row_map,
                            operand.as_ref(),
                            branches,
                            else_value.as_ref(),
                            evaluate_function,
                        )?
                            .unwrap_or_else(|| b"NULL".to_vec())),
                    ),

                    SelectProjectionItem::WindowFunction { .. } => Ok(Vec::new()),

                    SelectProjectionItem::Wildcard { .. } => Ok(Vec::new()),

                }

            })
            .collect::<Result<Vec<_>, _>>()?;

        rows.push(projected_row);

    }

    let rows = apply_select_post_processing(rows, &columns, read_plan, &projection_items)?;
    let (columns, rows) = strip_hidden_output_columns(columns, rows);

    Ok(SelectExecutionResult {
        columns,
        rows,
    })
    
}

pub fn execute_joined_select_plan<E, RM, RJ>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
    evaluate_function: &mut E,
    row_matches_relation: &mut RM,
    row_matches_joined: &mut RJ,
) -> Result<SelectExecutionResult, String>
where
    E: SqlFunctionEvaluationStrategy,
    RM: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> Result<bool, String>,
    RJ: FnMut(&JoinedRowTuple, Option<&SelectCondition>) -> Result<bool, String>,
{

    if read_plan.is_explain {
        return Ok(explain_joined_select_plan_result(read_plan));
    }

    let count_star_projection = count_star_projection(read_plan);
    
    if let Some(output_name) = &count_star_projection {

        let row_tuples = build_joined_row_tuples(
            catalog,
            wal,
            runtime_indexes,
            &read_plan.relations,
            &read_plan.pushdown_conditions,
            &read_plan.joins,
            row_matches_relation,
        )?;

        let mut matched_rows = 0usize;

        for row_tuple in row_tuples {
            if !row_matches_joined(&row_tuple, read_plan.where_condition.as_ref())? {
                continue;
            }

            matched_rows += 1;
        }

        return Ok(SelectExecutionResult {
            columns: vec![FieldDef {
                seqno: 1,
                field_name: output_name.clone(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }],
            rows: vec![vec![matched_rows.to_string().into_bytes()]],
        });

    }

    let projection_items = expand_join_projection_items(catalog, &read_plan.relations, &read_plan.projection_items)?;
    let visible_projection_len = projection_items.len();
    let projection_items = ensure_order_by_projection_items(projection_items, read_plan);

    let mut columns = Vec::with_capacity(projection_items.len());
    let mut static_projection_values = Vec::with_capacity(projection_items.len());

    for (seq, projection_item) in projection_items.iter().enumerate() {
        
        let is_hidden_sort_key = seq >= visible_projection_len;

        match projection_item {

            SelectProjectionItem::Column {
                field_name,
                output_name,
            } => {

                let Some(field) = resolve_join_field(catalog, &read_plan.relations, field_name) else {
                    return Err(format!("select join failed: unknown column '{}'", field_name));
                };

                let is_nullable = field.nullable
                    || join_field_can_be_null_extended(
                        &read_plan.relations,
                        &read_plan.joins,
                        field_name,
                    );

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: field.field_type.clone(),
                    nullable: is_nullable,
                    indexed: field.indexed,
                    default_value: field.default_value.clone(),
                    metadata: column_metadata_with_visibility(field.metadata.clone(), is_hidden_sort_key),
                });

                static_projection_values.push(None);

            },

            SelectProjectionItem::InbuiltFunction { function, .. } => {

                let value = if sql_function_references_column(function) {
                    None
                } else {
                    Some(
                        evaluate_function.evaluate(function, &mut |_| None)
                            .map_err(|err| format!(
                                "select join failed: inbuilt projection evaluation failed: {err}"
                            ))?,
                    )
                };

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: projection_output_name(projection_item),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });

                static_projection_values.push(value);

            },

            SelectProjectionItem::Case { output_name, .. } => {

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });

                static_projection_values.push(None);

            },

            SelectProjectionItem::WindowFunction { output_name, .. } => {

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: column_metadata_with_visibility(None, is_hidden_sort_key),
                });

                static_projection_values.push(None);

            },

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select join failed: wildcard expansion should have been resolved before projection building".to_string());
            },

        }

    }

    let row_tuples = build_joined_row_tuples(
        catalog,
        wal,
        runtime_indexes,
        &read_plan.relations,
        &read_plan.pushdown_conditions,
        &read_plan.joins,
        row_matches_relation,
    )?;

    let mut rows = Vec::new();
    
    for row_tuple in row_tuples {

        if !row_matches_joined(&row_tuple, read_plan.where_condition.as_ref())? {
            continue;
        }

        let projected_row = projection_items
            .iter()
            .enumerate()
            .map(|(projection_idx, projection_item)| -> Result<Vec<u8>, String> {

                match projection_item {

                    SelectProjectionItem::Column { field_name, .. } => Ok(match row_tuple.value(field_name) {
                        Some(value) => render_stored_field_value(value),
                        None if columns[projection_idx].nullable => b"NULL".to_vec(),
                        None => Vec::new(),
                    }),

                    SelectProjectionItem::InbuiltFunction { function, .. } => {

                        let static_value = static_projection_values
                            .get(projection_idx)
                            .and_then(|entry| entry.as_ref())
                            .cloned()
                            .flatten();

                        if let Some(value) = static_value {
                            Ok(value)
                        } else {
                            evaluate_function.evaluate(
                                function,
                                &mut |field_name| row_tuple
                                    .value(field_name)
                                    .map(|value| render_stored_field_value(value)),
                            )
                            .map(|value| value.unwrap_or_else(|| b"NULL".to_vec()))
                            .map_err(|err| {
                                format!("select join failed: inbuilt projection evaluation failed: {err}")
                            })
                        }

                    },

                    SelectProjectionItem::Case {
                        operand,
                        branches,
                        else_value,
                        ..
                    } => Ok(

                        render_stored_field_value(&evaluate_case_projection(
                            &row_tuple,
                            operand.as_ref(),
                            branches,
                            else_value.as_ref(),
                            evaluate_function,
                        )?
                            .unwrap_or_else(|| b"NULL".to_vec())),

                    ),

                    SelectProjectionItem::WindowFunction { .. } => Ok(Vec::new()),

                    SelectProjectionItem::Wildcard { .. } => Ok(Vec::new()),

                }

            })
            .collect::<Result<Vec<_>, _>>()?;

        rows.push(projected_row);

    }

    let rows = apply_select_post_processing(rows, &columns, read_plan, &projection_items)?;
    let (columns, rows) = strip_hidden_output_columns(columns, rows);

    Ok(SelectExecutionResult {
        columns,
        rows,
    })

}

fn apply_row_window(
    rows: Vec<Vec<Vec<u8>>>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {

    let start = offset.unwrap_or(0).min(rows.len());

    let end = limit
        .map(|limit| start.saturating_add(limit).min(rows.len()))
        .unwrap_or(rows.len());

    rows.into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()

}

fn apply_select_post_processing(
    mut rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    read_plan: &SelectReadPlan,
    projection_items: &[SelectProjectionItem],
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    let visible_indexes = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let hidden = column
                .metadata
                .as_ref()
                .map(|metadata| metadata.is_hidden())
                .unwrap_or(false);
            if hidden { None } else { Some(index) }
        })
        .collect::<Vec<_>>();

    if read_plan.distinct {

        let mut unique_rows = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();

        for row in rows {

            let key = if visible_indexes.len() == columns.len() {
                row.clone()
            } else {
                visible_indexes
                    .iter()
                    .filter_map(|index| row.get(*index).cloned())
                    .collect::<Vec<_>>()
            };

            if seen.insert(key) {
                unique_rows.push(row);
            }

        }

        rows = unique_rows;

    }

    if !read_plan.order_by.is_empty() {

        let mut order_indexes = Vec::with_capacity(read_plan.order_by.len());

        for item in &read_plan.order_by {
            if let Some(index) = columns.iter().position(|column| column.field_name == item.field_name) {
                order_indexes.push((index, item.descending));
            }
        }

        if !order_indexes.is_empty() {

            rows.sort_by(|left, right| {
                for (index, descending) in &order_indexes {
                    let ordering = left
                        .get(*index)
                        .cmp(&right.get(*index));

                    if ordering != Ordering::Equal {
                        return if *descending { ordering.reverse() } else { ordering };
                    }
                }

                Ordering::Equal
            });

        }

    }

    apply_window_projection_values(&mut rows, columns, projection_items, &read_plan.named_windows)?;

    Ok(apply_row_window(rows, read_plan.limit, read_plan.offset))

}

fn apply_window_projection_values(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    projection_items: &[SelectProjectionItem],
    named_windows: &[NamedWindowDefinition],
) -> Result<(), String> {

    let window_indexes = projection_items
        .iter()
        .enumerate()
        .filter_map(|(index, projection_item)| match projection_item {
            SelectProjectionItem::WindowFunction { function, .. } => Some((index, function)),
            _ => None,
        })
        .collect::<Vec<_>>();

    if window_indexes.is_empty() {
        return Ok(());
    }

    for (column_index, function) in window_indexes {
        let window_spec = resolve_row_number_window_spec(function, named_windows)?;

        match function.name.to_string().to_ascii_lowercase().as_str() {
            "row_number" => {
                let (partition_indexes, order_indexes) =
                    window_row_number_order_indexes(&window_spec, columns)?;

                let mut partitioned_row_indexes: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::new();

                for (row_index, row) in rows.iter().enumerate() {
                    let partition_key = partition_indexes
                        .iter()
                        .filter_map(|index| row.get(*index).cloned())
                        .collect::<Vec<_>>();

                    partitioned_row_indexes
                        .entry(partition_key)
                        .or_default()
                        .push(row_index);
                }

                for mut partition_row_indexes in partitioned_row_indexes.into_values() {

                    if !order_indexes.is_empty() {
                        partition_row_indexes.sort_by(|left, right| {
                            for (order_index, descending) in &order_indexes {
                                let ordering = rows[*left].get(*order_index).cmp(&rows[*right].get(*order_index));

                                if ordering != Ordering::Equal {
                                    return if *descending { ordering.reverse() } else { ordering };
                                }
                            }

                            left.cmp(right)
                        });
                    }

                    for (row_number, row_index) in partition_row_indexes.iter().enumerate() {
                        if let Some(cell) = rows[*row_index].get_mut(column_index) {
                            *cell = (row_number + 1).to_string().into_bytes();
                        }
                    }
                }
            }

            "sum" => {
                apply_sum_window_projection(rows, columns, column_index, function, &window_spec)?;
            }

            _ => {
                return Err(format!(
                    "SELECT window function '{}' is not supported yet",
                    function.name
                ));
            }
        }
    }

    Ok(())

}

fn window_row_number_order_indexes(
    window_spec: &WindowSpec,
    columns: &[FieldDef],
) -> Result<(Vec<usize>, Vec<(usize, bool)>), String> {

    let mut partition_indexes = Vec::with_capacity(window_spec.partition_by.len());

    for expression in &window_spec.partition_by {

        let field_name = match expression {

            Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
            
            Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
                .iter()
                .map(|part| part.value.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("."),
            
            Expr::Value(sqlparser::ast::Value::Number(position, _)) => {
                let position = position.parse::<usize>().map_err(|_| {
                    "window ROW_NUMBER PARTITION BY ordinal must be an unsigned numeric literal"
                        .to_string()
                })?;

                if position == 0 {
                    return Err("window ROW_NUMBER PARTITION BY ordinal must start at 1".to_string());
                }

                let Some(column) = columns.get(position - 1) else {
                    return Err(format!("window ROW_NUMBER PARTITION BY ordinal {} is out of range", position));
                };

                column.field_name.clone()
            },

            _ => {
                return Err("window ROW_NUMBER PARTITION BY currently supports only direct column references or ordinals".to_string());
            }

        };

        let Some(column_index) = columns
            .iter()
            .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
        else {
            return Err(format!("window ROW_NUMBER PARTITION BY references unknown output field '{}'", field_name));
        };

        partition_indexes.push(column_index);

    }

    let mut order_indexes = Vec::with_capacity(window_spec.order_by.len());

    for expression in &window_spec.order_by {

        let field_name = match &expression.expr {

            Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
            
            Expr::CompoundIdentifier(parts) if parts.len() == 1 => parts[0].value.to_ascii_lowercase(),
            
            Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

                let position = position.parse::<usize>().map_err(|_| {
                    "window ROW_NUMBER ORDER BY ordinal must be an unsigned numeric literal".to_string()
                })?;

                if position == 0 {
                    return Err("window ROW_NUMBER ORDER BY ordinal must start at 1".to_string());
                }

                let Some(column) = columns.get(position - 1) else {
                    return Err(format!("window ROW_NUMBER ORDER BY ordinal {} is out of range", position));
                };

                column.field_name.clone()

            },

            _ => {
                return Err("window ROW_NUMBER ORDER BY currently supports only direct column references or ordinals".to_string());
            }

        };

        if expression.nulls_first.is_some() || expression.with_fill.is_some() {
            return Err("window ROW_NUMBER ORDER BY does not support NULLS FIRST/LAST or WITH FILL yet".to_string());
        }

        let Some(column_index) = columns
            .iter()
            .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
        else {
            return Err(format!("window ROW_NUMBER ORDER BY references unknown output field '{}'", field_name));
        };

        order_indexes.push((column_index, expression.asc == Some(false)));
    }

    Ok((partition_indexes, order_indexes))

}

fn resolve_row_number_window_spec(
    function: &sqlparser::ast::Function,
    named_windows: &[NamedWindowDefinition],
) -> Result<WindowSpec, String> {

    let Some(window_type) = function.over.as_ref() else {
        return Err("window projection requires an OVER clause".to_string());
    };

    resolve_window_spec(window_type, named_windows, &mut Vec::new())

}

fn resolve_window_spec(
    window_type: &WindowType,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    match window_type {
        WindowType::NamedWindow(name) => resolve_named_window_spec(name, named_windows, visiting),
        WindowType::WindowSpec(window_spec) => resolve_window_spec_from_spec(window_spec, named_windows, visiting),
    }

}

fn resolve_window_spec_from_spec(
    window_spec: &WindowSpec,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    if let Some(window_name) = &window_spec.window_name {
        let base_spec = resolve_named_window_spec(window_name, named_windows, visiting)?;
        return merge_window_specs(base_spec, window_spec);
    }

    Ok(window_spec.clone())

}

fn resolve_named_window_spec(
    window_name: &sqlparser::ast::Ident,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    let normalized_name = window_name.value.to_ascii_lowercase();

    if visiting.iter().any(|name| name == &normalized_name) {
        return Err(format!("window reference '{}' is recursive", window_name.value));
    }

    let Some(definition) = named_windows.iter().find(|definition| {
        definition.0.value.eq_ignore_ascii_case(&window_name.value)
    }) else {
        return Err(format!("named window '{}' is not defined", window_name.value));
    };

    visiting.push(normalized_name);

    let resolved = match &definition.1 {
        NamedWindowExpr::NamedWindow(reference) => resolve_named_window_spec(reference, named_windows, visiting),
        NamedWindowExpr::WindowSpec(window_spec) => resolve_window_spec_from_spec(window_spec, named_windows, visiting),
    };

    visiting.pop();

    resolved

}

fn merge_window_specs(base_spec: WindowSpec, override_spec: &WindowSpec) -> Result<WindowSpec, String> {

    if !base_spec.partition_by.is_empty() && !override_spec.partition_by.is_empty() {
        return Err("named window PARTITION BY cannot be overridden yet".to_string());
    }

    if !base_spec.order_by.is_empty() && !override_spec.order_by.is_empty() {
        return Err("named window ORDER BY cannot be overridden yet".to_string());
    }

    if base_spec.window_frame.is_some() && override_spec.window_frame.is_some() {
        return Err("named window frame cannot be overridden yet".to_string());
    }

    Ok(WindowSpec {
        window_name: None,
        partition_by: if override_spec.partition_by.is_empty() {
            base_spec.partition_by
        } else {
            override_spec.partition_by.clone()
        },
        order_by: if override_spec.order_by.is_empty() {
            base_spec.order_by
        } else {
            override_spec.order_by.clone()
        },
        window_frame: override_spec
            .window_frame
            .clone()
            .or(base_spec.window_frame),
    })

}

fn apply_sum_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
) -> Result<(), String> {

    let source_column_index = resolve_window_sum_source_column(function, columns)?;
    let (partition_indexes, order_indexes) = window_row_number_order_indexes(window_spec, columns)?;

    let mut partitioned_row_indexes: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::new();

    for (row_index, row) in rows.iter().enumerate() {
        let partition_key = partition_indexes
            .iter()
            .filter_map(|index| row.get(*index).cloned())
            .collect::<Vec<_>>();

        partitioned_row_indexes
            .entry(partition_key)
            .or_default()
            .push(row_index);
    }

    for mut partition_row_indexes in partitioned_row_indexes.into_values() {

        if !order_indexes.is_empty() {
            partition_row_indexes.sort_by(|left, right| {
                for (order_index, descending) in &order_indexes {
                    let ordering = rows[*left].get(*order_index).cmp(&rows[*right].get(*order_index));

                    if ordering != Ordering::Equal {
                        return if *descending { ordering.reverse() } else { ordering };
                    }
                }

                left.cmp(right)
            });
        }

        let mut partition_values = Vec::with_capacity(partition_row_indexes.len());
        for row_index in &partition_row_indexes {
            let value = rows[*row_index]
                .get(source_column_index)
                .ok_or_else(|| format!("window SUM source column index {} is out of range", source_column_index))?;
            partition_values.push(parse_window_numeric_value(value)?);
        }

        let mut prefix_sums = Vec::with_capacity(partition_values.len() + 1);
        prefix_sums.push(0.0f64);

        for value in &partition_values {
            prefix_sums.push(prefix_sums.last().copied().unwrap_or(0.0) + value.unwrap_or(0.0));
        }

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds(window_spec.window_frame.as_ref(), row_position, partition_row_indexes.len())?;
            let sum_value = match frame_bounds {
                Some((start, end)) => prefix_sums[end + 1] - prefix_sums[start],
                None => 0.0,
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = render_window_numeric_value(sum_value);
            }
            
        }

    }

    Ok(())

}

fn resolve_window_sum_source_column(
    function: &Function,
    columns: &[FieldDef],
) -> Result<usize, String> {

    let FunctionArguments::List(list) = &function.args else {
        return Err("SUM window function currently requires exactly one argument".to_string());
    };

    let Some(argument) = list.args.first() else {
        return Err("SUM window function currently requires exactly one argument".to_string());
    };

    let expression = match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,
        _ => {
            return Err("SUM window function currently supports only a direct column argument".to_string());
        }
    };

    resolve_window_field_index(
        expression,
        columns,
        "window SUM source",
        "SUM window function currently supports only a direct column argument",
        "window SUM source ordinal must be an unsigned numeric literal",
        "window SUM source ordinal must start at 1",
    )

}

fn resolve_window_field_index(
    expression: &Expr,
    columns: &[FieldDef],
    context: &str,
    unsupported_message: &str,
    ordinal_parse_message: &str,
    ordinal_start_message: &str,
) -> Result<usize, String> {

    let field_name = match expression {

        Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
        
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
            .iter()
            .map(|part| part.value.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join("."),
        
        Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

            let position = position
                .parse::<usize>()
                .map_err(|_| ordinal_parse_message.to_string())?;

            if position == 0 {
                return Err(ordinal_start_message.to_string());
            }

            let Some(column) = columns.get(position - 1) else {
                return Err(format!("{context} ordinal {} is out of range", position));
            };

            column.field_name.clone()

        },

        _ => {
            return Err(unsupported_message.to_string());
        }

    };

    let Some(column_index) = columns
        .iter()
        .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
    else {
        return Err(format!("{context} references unknown output field '{}'", field_name));
    };

    Ok(column_index)

}

fn parse_window_numeric_value(value: &[u8]) -> Result<Option<f64>, String> {

    if value.is_empty() || value == b"NULL" {
        return Ok(None);
    }

    let text = std::str::from_utf8(value)
        .map_err(|_| "window aggregate value is not valid UTF-8".to_string())?
        .trim();

    if text.is_empty() || text.eq_ignore_ascii_case("null") {
        return Ok(None);
    }

    text.parse::<f64>()
        .map(Some)
        .map_err(|_| format!("window aggregate value '{}' is not numeric", text))

}

fn render_window_numeric_value(value: f64) -> Vec<u8> {

    if value.fract() == 0.0 {
        (value as i128).to_string().into_bytes()
    } else {
        value.to_string().into_bytes()
    }

}

fn window_frame_bounds(
    frame: Option<&WindowFrame>,
    row_position: usize,
    partition_len: usize,
) -> Result<Option<(usize, usize)>, String> {

    let Some(frame) = frame else {
        return Ok(Some((0, partition_len.saturating_sub(1))));
    };

    if frame.units != WindowFrameUnits::Rows {
        return Err("only ROWS window frames are supported yet".to_string());
    }

    if partition_len == 0 {
        return Ok(None);
    }

    let start = window_frame_bound_index(&frame.start_bound, row_position, partition_len)?;
    let end_bound = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
    let end = window_frame_bound_index(end_bound, row_position, partition_len)?;

    if start > end {
        return Ok(None);
    }

    Ok(Some((start, end)))

}

fn window_frame_bound_index(
    bound: &WindowFrameBound,
    row_position: usize,
    partition_len: usize,
) -> Result<usize, String> {

    let last_index = partition_len.saturating_sub(1);

    match bound {
        
        WindowFrameBound::CurrentRow => Ok(row_position.min(last_index)),
        
        WindowFrameBound::Preceding(None) => Ok(0),
        
        WindowFrameBound::Following(None) => Ok(last_index),

        WindowFrameBound::Preceding(Some(expr)) => Ok(row_position
            .saturating_sub(parse_window_frame_offset(expr)?)
            .min(last_index)),

        WindowFrameBound::Following(Some(expr)) => Ok(row_position
            .saturating_add(parse_window_frame_offset(expr)?)
            .min(last_index)),

    }

}

fn parse_window_frame_offset(expr: &Expr) -> Result<usize, String> {

    match expr {
        Expr::Value(sqlparser::ast::Value::Number(value, _)) => value
            .parse::<usize>()
            .map_err(|_| "window frame offset must be an unsigned numeric literal".to_string()),
        _ => Err("window frame offset currently supports only unsigned numeric literals".to_string()),
    }

}

fn ensure_order_by_projection_items(
    mut projection_items: Vec<SelectProjectionItem>,
    read_plan: &SelectReadPlan,
) -> Vec<SelectProjectionItem> {

    for order_by in &read_plan.order_by {
        
        let covered = projection_items.iter().any(|item| match item {

            SelectProjectionItem::Column {
                field_name,
                output_name,
            } => field_name == &order_by.field_name || output_name == &order_by.field_name,
            
            SelectProjectionItem::Case { output_name, .. } |
            SelectProjectionItem::InbuiltFunction { output_name, .. } |
            SelectProjectionItem::WindowFunction { output_name, .. } => {
                output_name == &order_by.field_name
            },
            
            SelectProjectionItem::Wildcard { .. } => false,

        });

        if !covered {
            projection_items.push(SelectProjectionItem::Column {
                field_name: order_by.field_name.clone(),
                output_name: order_by.field_name.clone(),
            });
        }
        
    }

    projection_items

}

fn column_metadata_with_visibility(
    metadata: Option<common::schema::FieldMetadata>,
    hidden: bool,
) -> Option<common::schema::FieldMetadata> {

    if !hidden {
        return metadata;
    }

    let mut metadata = metadata.unwrap_or_default();
    metadata.system_visibility = common::schema::SystemFieldVisibility::Hidden;
    Some(metadata)

}

fn strip_hidden_output_columns(
    columns: Vec<FieldDef>,
    rows: Vec<Vec<Vec<u8>>>,
) -> (Vec<FieldDef>, Vec<Vec<Vec<u8>>>) {

    let visible_indexes = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {

            let hidden = column
                .metadata
                .as_ref()
                .map(|metadata| metadata.is_hidden())
                .unwrap_or(false);
            
            if hidden { None } else { Some(index) }

        })
        .collect::<Vec<_>>();

    if visible_indexes.len() == columns.len() {
        return (columns, rows);
    }

    let visible_columns = visible_indexes
        .iter()
        .enumerate()
        .filter_map(|(visible_seq, index)| {
            
            columns.get(*index).cloned().map(|mut column| {
                column.seqno = (visible_seq + 1) as u32;
                column
            })

        })
        .collect::<Vec<_>>();

    let visible_rows = rows
        .into_iter()
        .map(|row| {
            visible_indexes
                .iter()
                .filter_map(|index| row.get(*index).cloned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    (visible_columns, visible_rows)
    
}

fn join_field_can_be_null_extended(
    relations: &[SelectRelation],
    joins: &[SelectJoin],
    field_name: &str,
) -> bool {

    let Some((qualifier, _)) = field_name.split_once('.') else {
        return false;
    };

    for (join_index, join) in joins.iter().enumerate() {

        let left_relations = &relations[..=join_index];

        if matches!(join.kind, SelectJoinKind::Right | SelectJoinKind::Full)
            && left_relations
                .iter()
                .any(|relation| relation_qualifier(relation) == qualifier)
        {
            return true;
        }

        if matches!(join.kind, SelectJoinKind::Left | SelectJoinKind::Full)
            && relation_qualifier(&join.relation) == qualifier
        {
            return true;
        }

    }

    false

}

fn resolve_join_field<'a>(
    catalog: &'a DatabaseCatalog,
    relations: &[SelectRelation],
    field_name: &str,
) -> Option<&'a crate::FieldDef> {

    let (qualifier, column_name) = field_name.split_once('.')?;

    let relation = relations
        .iter()
        .find(|relation| relation.table_id == qualifier || relation.alias.as_deref() == Some(qualifier))?;

    catalog.table_schema(&relation.table_id)?.field(column_name)

}

fn projection_output_name(projection_item: &SelectProjectionItem) -> String {

    match projection_item {

        SelectProjectionItem::Column { output_name, .. } |
        SelectProjectionItem::Case { output_name, .. } |
        SelectProjectionItem::InbuiltFunction { output_name, .. } |
        SelectProjectionItem::WindowFunction { output_name, .. } => output_name.clone(),

        SelectProjectionItem::Wildcard { relation } => relation.clone().unwrap_or_default(),

    }

}

fn count_star_projection(read_plan: &SelectReadPlan) -> Option<String> {

    if read_plan.projection_items.len() != 1 {
        return None;
    }

    if !read_plan.group_by.is_empty() {
        return None;
    }

    let SelectProjectionItem::InbuiltFunction {
        output_name,
        function,
    } = read_plan.projection_items.first()?
    else {
        return None;
    };

    if !function.name.to_string().eq_ignore_ascii_case("count") {
        return None;
    }

    if !function_is_count_star(function) {
        return None;
    }

    Some(output_name.clone())

}

fn count_star_is_strict_full_table(read_plan: &SelectReadPlan) -> bool {
    read_plan.where_condition.is_none()
        && read_plan.joins.is_empty()
        && read_plan.group_by.is_empty()
        && read_plan.having_condition.is_none()
        && !read_plan.distinct
        && read_plan.order_by.is_empty()
        && read_plan.limit.is_none()
        && read_plan.offset.is_none()
}

fn count_star_primary_key_cardinality(
    table: &DatabaseTable,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
) -> Option<usize> {

    let _ = table;
    let _ = runtime_indexes;
    let _ = read_plan;

    // Runtime index cardinality can be stale across cross-catalog hot paths;
    // prefer WAL-backed row counting for strict correctness.
    None

}

fn _count_star_primary_key_cardinality_disabled_reference(
    table: &DatabaseTable,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
) -> Option<usize> {

    if !count_star_is_strict_full_table(read_plan) {
        return None;
    }

    let pk_index = primary_key_index(table)?;
    let table_scope_id = if table.entity_id.is_empty() {
        table.table_id.as_str()
    } else {
        table.entity_id.as_str()
    };

    runtime_indexes.cardinality_for_table(table_scope_id, &pk_index.index_id.0)

}

fn function_is_count_star(function: &Function) -> bool {

    let FunctionArguments::List(list) = &function.args else {
        return false;
    };

    if list.args.len() != 1 {
        return false;
    }

    matches!(
        list.args.first(),
        Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) |
        Some(FunctionArg::Named {
            arg: FunctionArgExpr::Wildcard,
            ..
        })
    )

}

fn expand_relation_projection_items(
    schema: &TableSchema,
    projection_items: &[SelectProjectionItem],
) -> Vec<SelectProjectionItem> {

    let mut expanded = Vec::new();

    for projection_item in projection_items {

        match projection_item {

            SelectProjectionItem::Wildcard { .. } => {
                expanded.extend(schema.fields.iter().map(|field| SelectProjectionItem::Column {
                    field_name: field.field_name.clone(),
                    output_name: field.field_name.clone(),
                }));
            },
            
            _ => expanded.push(projection_item.clone()),

        }

    }

    expanded
}

fn expand_join_projection_items(
    catalog: &DatabaseCatalog,
    relations: &[SelectRelation],
    projection_items: &[SelectProjectionItem],
) -> Result<Vec<SelectProjectionItem>, String> {
    
    let mut expanded = Vec::new();

    for projection_item in projection_items {

        match projection_item {

            SelectProjectionItem::Wildcard { relation } => {

                let target_relations: Vec<&SelectRelation> = match relation {
                    
                    Some(qualifier) => relations
                        .iter()
                        .filter(|candidate| relation_qualifier(candidate) == qualifier)
                        .collect(),
                    
                    None => relations.iter().collect(),

                };

                if target_relations.is_empty() {
                    return Err(format!("select join failed: unknown wildcard relation '{:?}'", relation));
                }

                for target_relation in target_relations {

                    let Some(schema) = catalog.table_schema(&target_relation.table_id) else {
                        return Err(format!(
                            "select join failed: unknown table schema '{}'",
                            target_relation.table_id
                        ));
                    };

                    let qualifier = relation_qualifier(target_relation).to_string();

                    expanded.extend(schema.fields.iter().map(|field| SelectProjectionItem::Column {
                        field_name: format!("{qualifier}.{}", field.field_name),
                        output_name: field.field_name.clone(),
                    }));

                }

            },

            _ => expanded.push(projection_item.clone()),

        }

    }

    Ok(expanded)

}

fn case_projection_requires_row_context(
    operand: Option<&crate::engine::sql::SelectExpression>,
    branches: &[(SelectCaseWhen, crate::engine::sql::SelectExpression)],
    else_value: Option<&crate::engine::sql::SelectExpression>,
) -> bool {

    if operand.is_some_and(expression_references_column) {
        return true;
    }

    if else_value.is_some_and(expression_references_column) {
        return true;
    }

    branches.iter().any(|(branch_when, value)| {
        let branch_references_row = match branch_when {
            SelectCaseWhen::Condition(_) => true,
            SelectCaseWhen::Equals(expected) => expression_references_column(expected),
        };

        branch_references_row || expression_references_column(value)
    })
    
}
