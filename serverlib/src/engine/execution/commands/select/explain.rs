use crate::{
    DatabaseIndex, DatabaseTable, FieldDef, FieldIndex, FieldType, RelationAccessPlan,
    RelationAccessStrategy, RuntimeIndexStore, SelectCondition,
    SelectJoinKind, SelectPredicate, SelectProjectionItem, SelectReadPlan, SelectRelation,
    collect_indexable_equality_filters_for_schema,
    collect_indexable_in_list_filter_for_schema,
    collect_indexable_like_filter_for_schema,
    collect_indexable_range_filters_for_schema,
    relation_access_plan_diagnostics,
};
use crate::engine::database::schema::migration::render_stored_field_value;

use crate::engine::execution::{
    relation_qualifier, SelectExecutionResult, join_condition_field_names
};

fn explain_table_scope_id<'a>(table_id: &'a str, table: Option<&'a DatabaseTable>) -> &'a str {
    table
        .and_then(|table| {
            if table.entity_id.is_empty() {
                None
            } else {
                Some(table.entity_id.as_str())
            }
        })
        .unwrap_or(table_id)
}

fn format_lookup_key_part(part: &[u8]) -> String {
    if part.len() == 8 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(part);
        let decoded = i64::from_le_bytes(bytes);
        if decoded >= 0 {
            return decoded.to_string();
        }
    }

    if let Ok(text) = std::str::from_utf8(part)
        && text.chars().all(|ch| !ch.is_control())
    {
        return text.to_string();
    }

    let mut hex = String::with_capacity(part.len() * 2 + 2);
    hex.push_str("0x");
    for byte in part {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{:02x}", byte);
    }

    hex
}

fn single_field_index_id(table: &DatabaseTable, field_name: &str) -> Option<String> {
    table
        .indexes
        .values()
        .find(|index| {
            (index.field_names.len() == 1 && index.field_names[0] == field_name)
                || (index.field_names.is_empty() && index.field_name == field_name)
        })
        .map(|index| index.index_id.0.clone())
}

fn explain_row_ref_hydration_hint(
    table_scope_id: &str,
    table: Option<&DatabaseTable>,
    access_plan: Option<&RelationAccessPlan>,
    runtime_indexes: &RuntimeIndexStore,
) -> String {

    let Some(table) = table else {
        return "n/a".to_string();
    };

    let Some(plan) = access_plan else {
        return "n/a".to_string();
    };

    match &plan.strategy {
        RelationAccessStrategy::RuntimeIndexLookup { index_id, lookup_key } => {
            let Some(index) = table
                .indexes
                .values()
                .find(|index| index.index_id.0 == *index_id)
            else {
                return "fallback_missing_index_metadata".to_string();
            };

            let Some(state) = runtime_indexes
                .index_for_table(table_scope_id, index_id)
                .or_else(|| {
                    runtime_indexes
                        .find_scoped_index_state_for_lookup(index_id, lookup_key)
                        .map(|(_, state)| state)
                })
            else {
                if runtime_indexes.has_scoped_index_state(index_id) {
                    return "fallback_key_not_present".to_string();
                }
                return "fallback_missing_runtime_state".to_string();
            };

            if !state.contains(lookup_key) {
                return "fallback_key_not_present".to_string();
            }

            if !index.is_unique_key() {
                return "non_unique_key_present".to_string();
            }

            if state.row_ref(lookup_key).is_some() {
                "eligible_direct_row_ref".to_string()
            } else {
                "fallback_missing_row_ref".to_string()
            }
        }

        RelationAccessStrategy::EqualityProbe {
            field_name,
            lookup_value,
            source,
            equality_filters,
        } => {
            if !matches!(source, crate::EqualityProbeSource::ExistingIndex) {
                return "fallback_temporary_index".to_string();
            }

            if equality_filters.len() != 1 {
                return "fallback_multi_filter_probe".to_string();
            }

            let Some(index_id) = single_field_index_id(table, field_name) else {
                return "fallback_missing_index_metadata".to_string();
            };

            let Some(index) = table
                .indexes
                .values()
                .find(|index| index.index_id.0 == index_id)
            else {
                return "fallback_missing_index_metadata".to_string();
            };

            let key = vec![lookup_value.clone()];
            let key_variants = runtime_lookup_key_variants(&key);

            let Some(state) = runtime_indexes
                .index_for_table(table_scope_id, &index_id)
                .or_else(|| {
                    key_variants
                        .iter()
                        .find_map(|key_variant| {
                            runtime_indexes
                                .find_scoped_index_state_for_lookup(&index_id, key_variant)
                                .map(|(_, state)| state)
                        })
                })
            else {
                if runtime_indexes.has_scoped_index_state(&index_id) {
                    return "fallback_key_not_present".to_string();
                }
                return "fallback_missing_runtime_state".to_string();
            };

            let matched_key = key_variants
                .iter()
                .find(|key_variant| state.contains(key_variant));

            if matched_key.is_none() {
                return "fallback_key_not_present".to_string();
            }

            if !index.is_unique_key() {
                return "non_unique_key_present".to_string();
            }

            if let Some(matched_key) = matched_key
                && state.row_ref(matched_key).is_some()
            {
                "eligible_direct_row_ref".to_string()
            } else {
                "fallback_missing_row_ref".to_string()
            }
        }

        _ => "n/a".to_string(),
    }
}

fn runtime_lookup_key_variants(lookup_key: &[Vec<u8>]) -> Vec<Vec<Vec<u8>>> {

    let mut variants = Vec::with_capacity(2);
    variants.push(lookup_key.to_vec());

    if lookup_key.len() == 1 {
        let rendered = render_stored_field_value(&lookup_key[0]);
        if rendered != lookup_key[0] {
            variants.push(vec![rendered]);
        }
    }

    variants

}

pub fn explain_select_plan_result(
    table_id: &str,
    filter_count: usize,
    access_plan: Option<&RelationAccessPlan>,
    index_lookup: Option<(&DatabaseIndex, &[Vec<u8>])>,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
    table: Option<&DatabaseTable>,
) -> SelectExecutionResult {

    let table_scope_id = explain_table_scope_id(table_id, table);
    
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
        FieldDef {
            seqno: 8,
            field_name: "complexity_score".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 9,
            field_name: "execution_mode".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 10,
            field_name: "complexity_reasons".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 11,
            field_name: "planner_score".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 12,
            field_name: "index_prioritization".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 13,
            field_name: "row_ref_hydration".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ];

    let advice = advise_select_execution(read_plan);

    let (access_path, mut index_id, lookup_key, cardinality, lookup_hit) =

        if let Some((index, key)) = index_lookup {

            let key_variants = runtime_lookup_key_variants(key);

            let state = runtime_indexes
                .index_for_table(table_scope_id, &index.index_id.0)
                .or_else(|| {
                    key_variants
                        .iter()
                        .find_map(|key_variant| {
                            runtime_indexes
                                .find_scoped_index_state_for_lookup(&index.index_id.0, key_variant)
                                .map(|(_, state)| state)
                        })
                });

            let hit = state
                .map(|s| key_variants.iter().any(|key_variant| s.contains(key_variant)))
                .unwrap_or(false);
            let card = state.map(|s| s.cardinality()).unwrap_or(0);

            let key_text = key
                .iter()
                .map(|part| format_lookup_key_part(part))
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

            let path = match access_plan.map(|plan| &plan.strategy) {

                Some(RelationAccessStrategy::FullScan)                  => "full_scan",

                Some(RelationAccessStrategy::EqualityProbe { .. })      => "equality_probe",

                Some(RelationAccessStrategy::InListProbe { .. })        => "in_list_probe",

                Some(RelationAccessStrategy::PrefixLikeProbe { .. })    => "prefix_like_probe",

                Some(RelationAccessStrategy::StringLikeProbe { .. })    => "string_like_probe",

                Some(RelationAccessStrategy::RangeProbe { .. })         => "range_probe",

                Some(RelationAccessStrategy::RangeIntersectionProbe { .. }) => "range_intersection_probe",

                Some(RelationAccessStrategy::RuntimeIndexLookup { .. }) => "index_lookup_then_scan",

                None => "full_scan",
                
            };

            (
                path.to_string(),
                "".to_string(),
                "".to_string(),
                "n/a".to_string(),
                "".to_string(),
            )

        };

    if index_id.is_empty()
        && matches!(
            access_plan.map(|plan| &plan.strategy),
            Some(RelationAccessStrategy::EqualityProbe { .. })
                | Some(RelationAccessStrategy::InListProbe { .. })
        )
        && let (Some(table), Some(condition)) = (table, read_plan.where_condition.as_ref())
    {
        let indexed_ids = explain_index_ids_for_equality_filters(table, condition);
        if !indexed_ids.is_empty() {
            index_id = indexed_ids.join(",");
        }
    }

    let (planner_score, index_prioritization, chosen_index_hint) = if let Some(table) = table {

        let schema = &table.schema;
        let mut index_filter_map = std::collections::HashMap::new();
        let range_filters = read_plan
            .where_condition
            .as_ref()
            .map(|condition| collect_indexable_range_filters_for_schema(schema, condition))
            .unwrap_or_default();
        let in_list_filter = read_plan
            .where_condition
            .as_ref()
            .and_then(|condition| collect_indexable_in_list_filter_for_schema(schema, condition));
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

        let diagnostics = relation_access_plan_diagnostics(
            table,
            allow_index_short_circuit,
            index_filter_map,
            in_list_filter,
            range_filters,
            like_filter,
        );

        let prioritization = diagnostics
            .candidates
            .iter()
            .enumerate()
            .map(|(position, candidate)| {
                let index_text = if candidate.index_hint.is_empty() {
                    "-".to_string()
                } else {
                    candidate.index_hint.clone()
                };

                format!(
                    "{}.{}(score={},index={},reason={})",
                    position + 1,
                    candidate.access_path,
                    candidate.score,
                    index_text,
                    candidate.reason,
                )
            })
            .collect::<Vec<_>>()
            .join(" > ");

        let chosen_index_hint = diagnostics
            .candidates
            .first()
            .map(|candidate| candidate.index_hint.clone())
            .unwrap_or_default();

        (
            diagnostics.chosen_score.to_string(),
            prioritization,
            chosen_index_hint,
        )

    } else {

        ("n/a".to_string(), "n/a".to_string(), String::new())

    };

    if index_id.is_empty() && !chosen_index_hint.is_empty() {
        index_id = chosen_index_hint;
    }

    let row_ref_hydration = explain_row_ref_hydration_hint(
        table_scope_id,
        table,
        access_plan,
        runtime_indexes,
    );

    let rows = vec![vec![
        table_id.as_bytes().to_vec(),
        access_path.into_bytes(),
        index_id.into_bytes(),
        lookup_key.into_bytes(),
        cardinality.into_bytes(),
        lookup_hit.into_bytes(),
        filter_count.to_string().into_bytes(),
        advice.score.to_string().into_bytes(),
        advice.execution_mode.as_bytes().to_vec(),
        advice.reasons.into_bytes(),
        planner_score.into_bytes(),
        index_prioritization.into_bytes(),
        row_ref_hydration.into_bytes(),
    ]];

    SelectExecutionResult { columns, rows }

}

fn explain_index_ids_for_equality_filters(
    table: &DatabaseTable,
    condition: &SelectCondition,
) -> Vec<String> {

    let mut fields = std::collections::BTreeSet::new();
    collect_equality_filter_fields(condition, &mut fields);

    let mut index_ids = fields
        .into_iter()
        .filter_map(|field_name| {
            table
                .indexes
                .values()
                .filter(|index| {
                    (!index.field_names.is_empty()
                        && index.field_names.len() == 1
                        && index.field_names[0] == field_name)
                        || (index.field_names.is_empty() && index.field_name == field_name)
                })
                .map(|index| index.index_id.0.clone())
                .min()
        })
        .collect::<Vec<_>>();

    index_ids.sort();
    index_ids

}

fn collect_equality_filter_fields(
    condition: &SelectCondition,
    fields: &mut std::collections::BTreeSet<String>,
) {

    match condition {
        SelectCondition::And(children) | SelectCondition::Or(children) => {
            for child in children {
                collect_equality_filter_fields(child, fields);
            }
        }
        SelectCondition::Not(child) => {
            collect_equality_filter_fields(child, fields);
        }
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op: crate::SelectComparisonOp::Eq,
            ..
        }) => {
            fields.insert(field_name.clone());
        }
        SelectCondition::Predicate(_) => {}
    }

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
        FieldDef {
            seqno: 6,
            field_name: "complexity_score".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 7,
            field_name: "execution_mode".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 8,
            field_name: "complexity_reasons".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ];

    let advice = advise_select_execution(read_plan);

    let mut rows = Vec::new();

    if let Some(primary_relation) = read_plan.relations.first() {
        rows.push(vec![
            b"0".to_vec(),
            b"base".to_vec(),
            relation_label(primary_relation).into_bytes(),
            Vec::new(),
            pushdown_filter_text(read_plan.pushdown_conditions.first()).into_bytes(),
            advice.score.to_string().into_bytes(),
            advice.execution_mode.as_bytes().to_vec(),
            advice.reasons.as_bytes().to_vec(),
        ]);
    }

    for (join_index, join) in read_plan.joins.iter().enumerate() {
        
        let on_text = if let Some((left_field_name, right_field_name)) =
            join_condition_field_names(join)
        {
            format!("{} = {}", left_field_name, right_field_name)
        } else {
            format!("{:?}", join.on_condition)
        };

        rows.push(vec![
            (join_index + 1).to_string().into_bytes(),
            join.kind.to_string().into_bytes(),
            relation_label(&join.relation).into_bytes(),
            on_text.into_bytes(),
            pushdown_filter_text(read_plan.pushdown_conditions.get(join_index + 1)).into_bytes(),
            advice.score.to_string().into_bytes(),
            advice.execution_mode.as_bytes().to_vec(),
            advice.reasons.as_bytes().to_vec(),
        ]);

    }

    SelectExecutionResult { columns, rows }

}

fn relation_label(relation: &SelectRelation) -> String {

    match relation.alias.as_deref() {
        
        Some(alias) if alias != relation.table_id => {
            format!("{} {}", relation.table_id, alias)
        },
        
        _ => relation.table_id.clone(),

    }

}

fn pushdown_filter_text(condition: Option<&Option<SelectCondition>>) -> String {

    match condition.and_then(|entry| entry.as_ref()) {
        Some(condition) => format!("{:?}", condition),
        None => String::new(),
    }
    
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectExecutionAdvice {
    score: usize,
    execution_mode: String,
    reasons: String,
}

impl SelectExecutionAdvice {

    pub fn score(&self) -> usize {
        self.score
    }

    pub fn execution_mode(&self) -> &str {
        &self.execution_mode
    }

    pub fn reasons(&self) -> &str {
        &self.reasons
    }

}

pub fn advise_select_execution(read_plan: &SelectReadPlan) -> SelectExecutionAdvice {

    let mut score = 0usize;
    let mut reasons = Vec::new();

    if !read_plan.joins.is_empty() {
        score += read_plan.joins.len() * 3;
        reasons.push("joins");
    }

    let non_inner_joins = read_plan
        .joins
        .iter()
        .filter(|join| !matches!(join.kind, SelectJoinKind::Inner))
        .count();
    
    if non_inner_joins > 0 {
        score += non_inner_joins * 2;
        reasons.push("outer_or_cross_join");
    }

    let projection_function_count = read_plan
        .projection_items
        .iter()
        .filter(|item| matches!(item, SelectProjectionItem::InbuiltFunction { .. }))
        .count();
    
    if projection_function_count > 0 {
        score += projection_function_count;
        reasons.push("projection_functions");
    }

    let case_count = read_plan
        .projection_items
        .iter()
        .filter(|item| matches!(item, SelectProjectionItem::Case { .. }))
        .count();
    
    if case_count > 0 {
        score += case_count * 2;
        reasons.push("case_expressions");
    }

    if read_plan.projection_items.len() > 4 {
        score += 1;
        reasons.push("wide_projection");
    }

    let subquery_count = read_plan
        .where_condition
        .as_ref()
        .map(count_subquery_predicates)
        .unwrap_or(0);

    if subquery_count > 0 {
        score += subquery_count * 3;
        reasons.push("subquery_predicates");
    }

    if read_plan.limit.is_some() || read_plan.offset.is_some() {
        score += 1;
        reasons.push("row_window");
    }

    if read_plan.relations.len() > 2 {
        score += 2;
        reasons.push("multi_relation");
    }

    let execution_mode = if score <= 2 {
        "inline"
    } else if score <= 7 {
        "adaptive_materialize"
    } else {
        "scoped_ephemeral"
    };

    SelectExecutionAdvice {
        score,
        execution_mode: execution_mode.to_string(),
        reasons: if reasons.is_empty() {
            "none".to_string()
        } else {
            reasons.join("|")
        },
    }

}

fn count_subquery_predicates(condition: &SelectCondition) -> usize {

    match condition {

        SelectCondition::And(children) | SelectCondition::Or(children) => {
            children.iter().map(count_subquery_predicates).sum()
        },

        SelectCondition::Not(child) => count_subquery_predicates(child),

        SelectCondition::Predicate(predicate) => match predicate {
            
            SelectPredicate::InSubquery { .. } |
            SelectPredicate::ScalarSubqueryComparison { .. } |
            SelectPredicate::AnySubqueryComparison { .. } |
            SelectPredicate::AllSubqueryComparison { .. } |
            SelectPredicate::Exists { .. } => 1,
            
            _ => 0,

        },

    }

}
