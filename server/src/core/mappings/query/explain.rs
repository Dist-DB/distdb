use connector::{ConnectorResponse, ConnectorResult, FieldDef, QueryResult};
use serverlib::{SelectCondition, SelectJoin, SelectRelation};

use super::timings::empty_query_timings;

pub(super) fn connector_field_defs(fields: Vec<serverlib::FieldDef>) -> Vec<FieldDef> {
    fields
        .into_iter()
        .map(|field| FieldDef {
            seqno: field.seqno,
            field_name: field.field_name,
            field_type: field.field_type,
            nullable: field.nullable,
            indexed: field.indexed,
            default_value: field.default_value,
            metadata: field.metadata,
        })
        .collect()
}

pub(super) fn explain_select_plan(
    request_id: &str,
    result: serverlib::SelectExecutionResult,
) -> ConnectorResponse {
    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(result.columns),
            rows: result.rows,
            timings: empty_query_timings(),
        }),
    )
}

pub(super) fn explain_inner_statement(statement_sql: &str) -> (&str, bool) {
    let trimmed = statement_sql.trim();
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.starts_with("explain ") {
        (trimmed["explain".len()..].trim(), true)
    } else {
        (trimmed, false)
    }
}

pub(super) fn explain_mutation_plan(request_id: &str, rows: Vec<Vec<String>>) -> ConnectorResponse {
    let columns = vec![
        serverlib::FieldDef {
            seqno: 1,
            field_name: "attribute".to_string(),
            field_type: serverlib::FieldType::Text,
            nullable: false,
            indexed: serverlib::FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        serverlib::FieldDef {
            seqno: 2,
            field_name: "value".to_string(),
            field_type: serverlib::FieldType::Text,
            nullable: false,
            indexed: serverlib::FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ];

    let result_rows = rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.into_bytes()).collect::<Vec<_>>())
        .collect::<Vec<_>>();

    explain_select_plan(
        request_id,
        serverlib::SelectExecutionResult {
            columns,
            rows: result_rows,
        },
    )
}

pub(super) fn explain_join_mutation_plan(
    request_id: &str,
    operation: &str,
    table_id: &str,
    relations: &[SelectRelation],
    joins: &[SelectJoin],
    pushdown_conditions: &[Option<SelectCondition>],
    assignment_count: usize,
    has_where_condition: bool,
) -> ConnectorResponse {
    let mut rows = vec![
        vec!["operation".to_string(), operation.to_string()],
        vec!["table".to_string(), table_id.to_string()],
        vec!["relation_count".to_string(), relations.len().to_string()],
        vec!["join_count".to_string(), joins.len().to_string()],
    ];

    if operation == "update" {
        rows.push(vec![
            "assignment_count".to_string(),
            assignment_count.to_string(),
        ]);
    }

    rows.push(vec![
        "where_present".to_string(),
        if has_where_condition { "true" } else { "false" }.to_string(),
    ]);

    for (join_index, join) in joins.iter().enumerate() {
        let kind = match &join.kind {
            serverlib::SelectJoinKind::Inner => "inner",
            serverlib::SelectJoinKind::Left => "left",
            serverlib::SelectJoinKind::Right => "right",
            serverlib::SelectJoinKind::Full => "full",
            serverlib::SelectJoinKind::Cross => "cross",
        };

        let on = if let Some((left_field_name, right_field_name)) =
            serverlib::join_condition_field_names(join)
        {
            format!("{} = {}", left_field_name, right_field_name)
        } else {
            format!("{:?}", join.on_condition)
        };

        rows.push(vec![format!("join[{}].kind", join_index), kind.to_string()]);
        rows.push(vec![
            format!("join[{}].relation", join_index),
            join.relation.table_id.clone(),
        ]);
        rows.push(vec![format!("join[{}].on", join_index), on]);

        if let Some(condition) = pushdown_conditions
            .get(join_index + 1)
            .and_then(|c| c.as_ref())
        {
            rows.push(vec![
                format!("join[{}].pushdown", join_index),
                format!("{:?}", condition),
            ]);
        }
    }

    explain_mutation_plan(request_id, rows)
}
