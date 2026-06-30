use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseTable, FieldDef, FieldIndex, FieldType,
    RelationAccessPlan, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectProjectionItem, SelectReadPlan, SelectRelation, TableSchema,
    primary_key_index, render_stored_field_value,
};
use crate::engine::sql::{
    evaluate_sql_function_with_lookup, expression_references_column,
    sql_function_references_column, SelectCaseWhen,
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
    evaluate_inbuilt: &mut E,
) -> Result<SelectExecutionResult, String>
where
    E: FnMut(&Function) -> Result<Option<Vec<u8>>, String>,
{

    let mut columns = Vec::with_capacity(read_plan.projection_items.len());
    let mut row = Vec::with_capacity(read_plan.projection_items.len());

    for (seq, projection_item) in read_plan.projection_items.iter().enumerate() {
        
        match projection_item {

            SelectProjectionItem::InbuiltFunction {
                output_name,
                function,
            } => {

                let value = evaluate_inbuilt(function).map_err(|err| {
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

    let rows = apply_select_post_processing(vec![row], &columns, read_plan);

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
    evaluate_inbuilt: &mut E,
    row_matches: &mut R,
) -> Result<SelectExecutionResult, String>
where
    E: FnMut(&Function) -> Result<Option<Vec<u8>>, String>,
    R: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> Result<bool, String>,
{

    let count_star_projection = count_star_projection(read_plan);

    if let Some(output_name) = &count_star_projection {

        if count_star_is_strict_full_table(read_plan) {

            if let Some(pk_count) = count_star_primary_key_cardinality(table, runtime_indexes, read_plan)
                && pk_count > 0 {
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
                    rows: vec![vec![pk_count.to_string().into_bytes()]],
                });
            }

            let live_row_count = load_live_row_count(wal, &table.table_id);
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
                rows: vec![vec![live_row_count.to_string().into_bytes()]],
            });

        }

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
                    let value = evaluate_inbuilt(function)
                        .map_err(|err| format!("select failed: inbuilt projection evaluation failed: {err}"))?;
                    static_projection_values.push(Some(value));
                }
            },

            SelectProjectionItem::Column { .. } => static_projection_values.push(None),

            SelectProjectionItem::Case { .. } => static_projection_values.push(None),

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
                            evaluate_sql_function_with_lookup(
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
                        )?
                            .unwrap_or_else(|| b"NULL".to_vec())),
                    ),

                    SelectProjectionItem::Wildcard { .. } => Ok(Vec::new()),

                }

            })
            .collect::<Result<Vec<_>, _>>()?;

        rows.push(projected_row);

    }

    let rows = apply_select_post_processing(rows, &columns, read_plan);
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
    evaluate_inbuilt: &mut E,
    row_matches_relation: &mut RM,
    row_matches_joined: &mut RJ,
) -> Result<SelectExecutionResult, String>
where
    E: FnMut(&Function) -> Result<Option<Vec<u8>>, String>,
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
                        evaluate_inbuilt(function)
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
                            evaluate_sql_function_with_lookup(
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
                        )?
                            .unwrap_or_else(|| b"NULL".to_vec())),

                    ),

                    SelectProjectionItem::Wildcard { .. } => Ok(Vec::new()),

                }

            })
            .collect::<Result<Vec<_>, _>>()?;

        rows.push(projected_row);

    }

    let rows = apply_select_post_processing(rows, &columns, read_plan);
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
) -> Vec<Vec<Vec<u8>>> {

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

    apply_row_window(rows, read_plan.limit, read_plan.offset)

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
            
            SelectProjectionItem::Case { output_name, .. }
            | SelectProjectionItem::InbuiltFunction { output_name, .. } => {
                output_name == &order_by.field_name
            }
            
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

        SelectProjectionItem::Column { output_name, .. }
        | SelectProjectionItem::Case { output_name, .. }
        | SelectProjectionItem::InbuiltFunction { output_name, .. } => output_name.clone(),

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

    if !count_star_is_strict_full_table(read_plan) {
        return None;
    }

    let pk_index = primary_key_index(table)?;
    runtime_indexes.cardinality(&pk_index.index_id.0)

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
        Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
            | Some(FunctionArg::Named {
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
