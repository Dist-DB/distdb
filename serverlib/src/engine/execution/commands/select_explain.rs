use crate::{
    DatabaseIndex, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, SelectCondition,
    SelectJoinKind, SelectPredicate, SelectProjectionItem, SelectReadPlan, SelectRelation,
};

use super::super::relation_qualifier;
use super::super::select::SelectExecutionResult;

pub fn explain_select_plan_result(
    table_id: &str,
    filter_count: usize,
    index_lookup: Option<(&DatabaseIndex, &[Vec<u8>])>,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &SelectReadPlan,
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
    ];

    let advice = advise_select_execution(read_plan);

    let (access_path, index_id, lookup_key, cardinality, lookup_hit) =

        if let Some((index, key)) = index_lookup {

            let state = runtime_indexes.index(&index.index_id.0);

            let hit = state.map(|s| s.contains(key)).unwrap_or(false);
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
                "n/a".to_string(),
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
        advice.score.to_string().into_bytes(),
        advice.execution_mode.as_bytes().to_vec(),
        advice.reasons.into_bytes(),
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
            super::super::join_condition_field_names(join)
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
        SelectJoinKind::Cross => "cross",
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
            SelectPredicate::InSubquery { .. }
            | SelectPredicate::ScalarSubqueryComparison { .. }
            | SelectPredicate::AnySubqueryComparison { .. }
            | SelectPredicate::AllSubqueryComparison { .. }
            | SelectPredicate::Exists { .. } => 1,
            _ => 0,
        },

    }

}
