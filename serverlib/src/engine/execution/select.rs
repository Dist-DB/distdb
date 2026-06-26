use std::collections::{HashMap, HashSet};

use sqlparser::ast::Function;

use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseTable, FieldDef, FieldIndex,
    FieldType, RelationAccessPlan, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectProjectionItem, SelectReadPlan, SelectRelation, TableSchema,
};

use super::{
    build_joined_row_tuples, collect_indexable_equality_filters, materialize_relation_rows,
    plan_relation_access, relation_qualifier, row_matches_condition_with,
    ConditionValueProvider, JoinedRowTuple,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectExecutionResult {
    pub columns: Vec<FieldDef>,
    pub rows: Vec<Vec<Vec<u8>>>,
}

pub fn row_matches_select_condition(
    provider: &impl ConditionValueProvider,
    condition: Option<&SelectCondition>,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
) -> bool {
    row_matches_condition_with(provider, condition, &mut |actual, subquery| {
        let values = collect_subquery_projection_values(catalog, wal, runtime_indexes, subquery);
        values.contains(actual)
    })
}

fn collect_subquery_projection_values(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    subquery: &SelectReadPlan,
) -> HashSet<Vec<u8>> {
    if subquery.is_explain
        || subquery
            .projection_items
            .iter()
            .any(|item| matches!(item, SelectProjectionItem::Wildcard { .. }))
    {
        return HashSet::new();
    }

    let Some(first_projection) = subquery.projection_items.first() else {
        return HashSet::new();
    };

    if !matches!(first_projection, SelectProjectionItem::Column { .. }) {
        return HashSet::new();
    }

    if subquery.table_id.is_empty() {
        return HashSet::new();
    }

    if subquery.joins.is_empty() {
        let Some(schema) = catalog.table_schema(&subquery.table_id) else {
            return HashSet::new();
        };

        let Some(table) = catalog.table(&subquery.table_id) else {
            return HashSet::new();
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
            &mut |_function| {
                Err("select subquery projection does not support inbuilt functions".to_string())
            },
            &mut |row_map, nested_condition| {
                row_matches_select_condition(
                    row_map,
                    nested_condition,
                    catalog,
                    wal,
                    runtime_indexes,
                )
            },
        )
        .ok()
        .map(first_column_values)
        .unwrap_or_default();
    }

    execute_joined_select_plan(
        catalog,
        wal,
        runtime_indexes,
        subquery,
        &mut |_function| {
            Err("select subquery projection does not support inbuilt functions".to_string())
        },
        &mut |row_map, nested_condition| {
            row_matches_select_condition(row_map, nested_condition, catalog, wal, runtime_indexes)
        },
        &mut |row_tuple, nested_condition| {
            row_matches_select_condition(
                row_tuple,
                nested_condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
    )
    .ok()
    .map(first_column_values)
    .unwrap_or_default()
}

fn first_column_values(result: SelectExecutionResult) -> HashSet<Vec<u8>> {
    result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect()
}

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
                let value = evaluate_inbuilt(function)
                    .map_err(|err| format!("select failed: inbuilt projection evaluation failed: {err}"))?;

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
            }

            SelectProjectionItem::Column { .. } => {
                return Err(
                    "select without FROM only supports inbuilt projection functions".to_string(),
                );
            }

            SelectProjectionItem::Wildcard { .. } => {
                return Err(
                    "select without FROM does not support wildcard projections".to_string(),
                );
            }
        }
    }

    Ok(SelectExecutionResult {
        columns,
        rows: vec![row],
    })
}

pub fn explain_select_plan_result(
    table_id: &str,
    filter_count: usize,
    index_lookup: Option<(&DatabaseIndex, Vec<Vec<u8>>)>,
    runtime_indexes: &RuntimeIndexStore,
) -> SelectExecutionResult {
    let columns = vec![
        FieldDef {
            seqno: 1,
            field_name: "table".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 2,
            field_name: "access_path".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 3,
            field_name: "index_id".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 4,
            field_name: "lookup_key".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 5,
            field_name: "index_cardinality".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 6,
            field_name: "lookup_hit".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 7,
            field_name: "filters".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ];

    let (access_path, index_id, lookup_key, cardinality, lookup_hit) = if let Some((index, key)) = index_lookup {
        let state = runtime_indexes.index(&index.index_id.0);

        let hit = state.map(|s| s.contains(&key)).unwrap_or(false);
        let card = state.map(|s| s.cardinality()).unwrap_or(0);

        let key_text = key
            .iter()
            .map(|part| String::from_utf8_lossy(part).to_string())
            .collect::<Vec<_>>()
            .join(",");

        let path = if state.is_none() || card == 0 || hit {
            "index_lookup_then_scan"
        } else {
            "index_lookup_empty"
        };

        (
            path.to_string(),
            index.index_id.0.clone(),
            key_text,
            card.to_string(),
            if hit { "true" } else { "false" }.to_string(),
        )
    } else {
        (
            "full_scan".to_string(),
            "".to_string(),
            "".to_string(),
            "0".to_string(),
            "".to_string(),
        )
    };

    let rows = vec![vec![
        table_id.as_bytes().to_vec(),
        access_path.into_bytes(),
        index_id.into_bytes(),
        lookup_key.into_bytes(),
        cardinality.into_bytes(),
        lookup_hit.into_bytes(),
        filter_count.to_string().into_bytes(),
    ]];

    SelectExecutionResult { columns, rows }
}

pub fn explain_joined_select_plan_result(read_plan: &SelectReadPlan) -> SelectExecutionResult {
    let columns = vec![
        FieldDef {
            seqno: 1,
            field_name: "step".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 2,
            field_name: "join_kind".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 3,
            field_name: "relation".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 4,
            field_name: "on".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 5,
            field_name: "pushdown_filters".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ];

    let mut rows = Vec::new();

    if let Some(primary_relation) = read_plan.relations.first() {
        rows.push(vec![
            b"0".to_vec(),
            b"base".to_vec(),
            relation_label(primary_relation).into_bytes(),
            Vec::new(),
            pushdown_filter_text(read_plan.pushdown_conditions.first()).into_bytes(),
        ]);
    }

    for (join_index, join) in read_plan.joins.iter().enumerate() {
        let on_text = if let Some((left_field_name, right_field_name)) =
            super::join_condition_field_names(join)
        {
            format!("{} = {}", left_field_name, right_field_name)
        } else {
            format!("{:?}", join.on_condition)
        };

        rows.push(vec![
            (join_index + 1).to_string().into_bytes(),
            join_kind_label(&join.kind).as_bytes().to_vec(),
            relation_label(&join.relation).into_bytes(),
            on_text.into_bytes(),
            pushdown_filter_text(read_plan.pushdown_conditions.get(join_index + 1)).into_bytes(),
        ]);
    }

    SelectExecutionResult { columns, rows }
}

fn relation_label(relation: &SelectRelation) -> String {
    match relation.alias.as_deref() {
        Some(alias) if alias != relation.table_id => {
            format!("{} {}", relation.table_id, alias)
        }
        _ => relation.table_id.clone(),
    }
}

fn join_kind_label(kind: &SelectJoinKind) -> &'static str {
    match kind {
        SelectJoinKind::Inner => "inner",
        SelectJoinKind::Left => "left",
        SelectJoinKind::Right => "right",
        SelectJoinKind::Full => "full",
    }
}

fn pushdown_filter_text(condition: Option<&Option<SelectCondition>>) -> String {
    match condition.and_then(|entry| entry.as_ref()) {
        Some(condition) => format!("{:?}", condition),
        None => String::new(),
    }
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
    R: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> bool,
{
    let projection_items = expand_relation_projection_items(schema, &read_plan.projection_items);

    let mut columns = Vec::with_capacity(projection_items.len());

    for (seq, projection_item) in projection_items.iter().enumerate() {
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
                    metadata: field.metadata.clone(),
                });
            }

            SelectProjectionItem::InbuiltFunction { output_name, .. } => {
                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });
            }

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select failed: wildcard expansion should have been resolved before column building".to_string());
            }
        }
    }

    let mut static_projection_values = Vec::with_capacity(projection_items.len());

    for projection_item in &projection_items {
        match projection_item {
            SelectProjectionItem::InbuiltFunction { function, .. } => {
                let value = evaluate_inbuilt(function)
                    .map_err(|err| format!("select failed: inbuilt projection evaluation failed: {err}"))?;
                static_projection_values.push(Some(value));
            }
            SelectProjectionItem::Column { .. } => static_projection_values.push(None),
            SelectProjectionItem::Wildcard { .. } => static_projection_values.push(None),
        }
    }

    let rows = materialize_relation_rows(wal, table, schema, runtime_indexes, access_plan)
        .into_iter()
        .filter(|(_, row_map)| row_matches(row_map, read_plan.where_condition.as_ref()))
        .map(|(_, row_map)| {
            projection_items
                .iter()
                .enumerate()
                .map(|(projection_idx, projection_item)| match projection_item {
                    SelectProjectionItem::Column { field_name, .. } => match row_map.get(field_name) {
                        Some(value) => value.clone(),
                        None if columns[projection_idx].nullable => b"NULL".to_vec(),
                        None => Vec::new(),
                    },
                    SelectProjectionItem::InbuiltFunction { .. } => {
                        let static_value = static_projection_values
                            .get(projection_idx)
                            .and_then(|entry| entry.as_ref())
                            .cloned()
                            .flatten();

                        static_value.unwrap_or_else(|| b"NULL".to_vec())
                    }
                    SelectProjectionItem::Wildcard { .. } => Vec::new(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(SelectExecutionResult { columns, rows })
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
    RM: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> bool,
    RJ: FnMut(&JoinedRowTuple, Option<&SelectCondition>) -> bool,
{
    if read_plan.is_explain {
        return Err("select join failed: EXPLAIN for JOIN is not supported yet".to_string());
    }

    let projection_items = expand_join_projection_items(catalog, &read_plan.relations, &read_plan.projection_items)?;

    let mut columns = Vec::with_capacity(projection_items.len());
    let mut static_projection_values = Vec::with_capacity(projection_items.len());

    for (seq, projection_item) in projection_items.iter().enumerate() {
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
                    metadata: field.metadata.clone(),
                });

                static_projection_values.push(None);
            }

            SelectProjectionItem::InbuiltFunction { function, .. } => {
                let value = evaluate_inbuilt(function)
                    .map_err(|err| format!("select join failed: inbuilt projection evaluation failed: {err}"))?;

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: projection_output_name(projection_item),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                static_projection_values.push(Some(value));
            }

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select join failed: wildcard expansion should have been resolved before projection building".to_string());
            }
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

    let rows = row_tuples
        .into_iter()
        .filter(|row_tuple| row_matches_joined(row_tuple, read_plan.where_condition.as_ref()))
        .map(|row_tuple| {
            projection_items
                .iter()
                .enumerate()
                .map(|(projection_idx, projection_item)| match projection_item {
                    SelectProjectionItem::Column { field_name, .. } => {
                        match row_tuple.value(field_name) {
                            Some(value) => value.clone(),
                            None if columns[projection_idx].nullable => b"NULL".to_vec(),
                            None => Vec::new(),
                        }
                    }
                    SelectProjectionItem::InbuiltFunction { .. } => {
                        let static_value = static_projection_values
                            .get(projection_idx)
                            .and_then(|entry| entry.as_ref())
                            .cloned()
                            .flatten();

                        static_value.unwrap_or_else(|| b"NULL".to_vec())
                    }
                    SelectProjectionItem::Wildcard { .. } => Vec::new(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(SelectExecutionResult { columns, rows })
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

    let relation = relations.iter().find(|relation| {
        relation.table_id == qualifier || relation.alias.as_deref() == Some(qualifier)
    })?;

    catalog.table_schema(&relation.table_id)?.field(column_name)
}

fn projection_output_name(projection_item: &SelectProjectionItem) -> String {
    match projection_item {
        SelectProjectionItem::Column { output_name, .. }
        | SelectProjectionItem::InbuiltFunction { output_name, .. } => output_name.clone(),
        SelectProjectionItem::Wildcard { relation } => relation.clone().unwrap_or_default(),
    }
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
            }
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
                        return Err(format!("select join failed: unknown table schema '{}'", target_relation.table_id));
                    };

                    let qualifier = relation_qualifier(target_relation).to_string();

                    expanded.extend(schema.fields.iter().map(|field| SelectProjectionItem::Column {
                        field_name: format!("{qualifier}.{}", field.field_name),
                        output_name: field.field_name.clone(),
                    }));
                }
            }
            _ => expanded.push(projection_item.clone()),
        }
    }

    Ok(expanded)
}


#[cfg(test)]
#[path = "select_test.rs"]
mod tests;
