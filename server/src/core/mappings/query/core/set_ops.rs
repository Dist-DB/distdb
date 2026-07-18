use super::*;
use super::variables::SessionVariableOverrides;
use std::cmp::Ordering;
use std::collections::HashSet;

pub(super) fn execute_union_query_impl(
    request_id: &str,
    database_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    statement: &SqlRequest,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> ConnectorResponse {

    let (
        steps,
        order_by,
        limit_by,
        fetch_with_ties_limit,
        fetch_percent,
        fetch_percent_with_ties,
        limit,
        offset,
    ) =
        match serverlib::parse_union_select_read_plans_from_statement(&statement.sql) {

            Ok(parsed) => parsed,

            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("union query execution failed: {err}"),
                )
            }

        };

    let Some(catalog) = resolve_catalog_mut(catalogs, database_id) else {

        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", database_id),
        );

    };

    let mut set_stack = Vec::<serverlib::SelectExecutionResult>::new();

    for step in steps {

        match step {

            serverlib::SelectSetQueryStep::Branch(plan) => {

                let result = if !plan.ctes.is_empty() {
                    
                    match execute_select_with_ctes(
                        catalog,
                        wal,
                        runtime_indexes,
                        &plan,
                        session_variable_overrides,
                    ) {
                        Ok(result) => result,
                        Err(message) => {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("set query execution failed: {message}"),
                            )
                        }
                    }

                } else {

                    match execute_select_plan_result(catalog, wal, runtime_indexes, &plan) {

                        Ok(result) => result,

                        Err(message) => {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("set query execution failed: {message}"),
                            )
                        }

                    }

                };

                set_stack.push(result);

            },

            serverlib::SelectSetQueryStep::BoundaryOperation(boundary_operation) => {

                let Some(right_result) = set_stack.pop() else {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: missing right branch for set operation"
                            .to_string(),
                    );
                };

                let Some(mut left_result) = set_stack.pop() else {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: missing left branch for set operation"
                            .to_string(),
                    );
                };

                if left_result.columns.len() != right_result.columns.len() {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: all set-operation branches must return the same number of columns"
                            .to_string(),
                    );
                }

                if let Err(message) =
                    reconcile_union_column_types(&mut left_result.columns, &right_result.columns)
                {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("set query execution failed: {message}"),
                    );
                }

                let rows = apply_set_boundary_operation(
                    left_result.rows,
                    right_result.rows,
                    boundary_operation,
                    &left_result.columns,
                );

                left_result.rows = rows;
                set_stack.push(left_result);

            }

        }

    }

    let Some(set_result) = set_stack.pop() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "set query execution failed: no branch results were produced".to_string(),
        );
    };

    if !set_stack.is_empty() {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "set query execution failed: invalid set-operation evaluation state".to_string(),
        );
    }

    let columns = set_result.columns;
    let mut rows = set_result.rows;

    if !order_by.is_empty() {

        let mut order_indexes = Vec::with_capacity(order_by.len());
        const UNION_ORDER_BY_ORDINAL_PREFIX: &str = "__union_order_by_ordinal__";

        for item in &order_by {

            let index = if let Some(raw_ordinal) =
                item.field_name.strip_prefix(UNION_ORDER_BY_ORDINAL_PREFIX)
            {

                let ordinal = match raw_ordinal.parse::<usize>() {
                    
                    Ok(value) => value,

                    Err(_) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!(
                                "union query execution failed: invalid ORDER BY ordinal '{}'",
                                raw_ordinal
                            ),
                        )
                    }

                };

                if ordinal == 0 || ordinal > columns.len() {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!(
                            "union query execution failed: ORDER BY ordinal {} is out of range for {} output columns",
                            ordinal,
                            columns.len()
                        ),
                    );
                }

                ordinal - 1
            
            } else {

                let Some(index) = columns
                    .iter()
                    .position(|column| column.field_name == item.field_name)
                else {

                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!(
                            "union query execution failed: ORDER BY column '{}' is not present in UNION output",
                            item.field_name
                        ),
                    );

                };

                index
            };

            order_indexes.push((index, item.descending));

        }

        rows.sort_by(|left, right| {

            for (index, descending) in &order_indexes {

                let ordering = compare_union_cell_values(
                    left.get(*index),
                    right.get(*index),
                    columns.get(*index),
                );

                if ordering != Ordering::Equal {
                    return if *descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }

            Ordering::Equal
        });

    }

    rows = match apply_union_limit_by(rows, &columns, limit_by.as_ref()) {

        Ok(rows) => rows,

        Err(message) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("union query execution failed: {message}"),
            );
        }

    };

    if let Some(percent) = fetch_percent_with_ties {

        rows = match apply_union_percent_with_ties(rows, &columns, &order_by, percent) {

            Ok(rows) => rows,

            Err(message) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("union query execution failed: {message}"),
                );
            }

        };

    } else {

        rows = apply_union_percent(rows, fetch_percent);

        rows = match apply_union_with_ties(rows, &columns, &order_by, fetch_with_ties_limit) {

            Ok(rows) => rows,

            Err(message) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("union query execution failed: {message}"),
                );
            }

        };

    }

    rows = apply_union_row_window(rows, limit, offset);

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(columns),
            rows,
            timings: empty_query_timings(),
        }),
    )

}

fn apply_union_percent(
    rows: Vec<Vec<Vec<u8>>>,
    fetch_percent: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {

    serverlib::apply_percent_rows(rows, fetch_percent)

}

fn apply_union_percent_with_ties(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[serverlib::FieldDef],
    order_by: &[serverlib::SelectOrderByItem],
    percent: usize,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    if rows.is_empty() || percent == 0 {
        return Ok(Vec::new());
    }

    let capped_percent = percent.min(100);
    let total_rows = rows.len();
    let bounded_rows = total_rows
        .saturating_mul(capped_percent)
        .saturating_add(99)
        / 100;

    apply_union_with_ties(rows, columns, order_by, Some(bounded_rows))

}

fn apply_union_limit_by(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[serverlib::FieldDef],
    limit_by: Option<&serverlib::SelectLimitByPlan>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    serverlib::apply_limit_by_rows(
        rows,
        columns,
        limit_by,
        "LIMIT BY column",
    )

}

fn apply_union_with_ties(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[serverlib::FieldDef],
    order_by: &[serverlib::SelectOrderByItem],
    with_ties_limit: Option<usize>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    let mut order_indexes = Vec::with_capacity(order_by.len());
    const UNION_ORDER_BY_ORDINAL_PREFIX: &str = "__union_order_by_ordinal__";

    for item in order_by {

        let index = if let Some(raw_ordinal) = item.field_name.strip_prefix(UNION_ORDER_BY_ORDINAL_PREFIX) {

            let ordinal = raw_ordinal
                .parse::<usize>()
                .map_err(|_| format!("invalid ORDER BY ordinal '{}'", raw_ordinal))?;

            if ordinal == 0 || ordinal > columns.len() {
                return Err(format!(
                    "ORDER BY ordinal {} is out of range for {} output columns",
                    ordinal,
                    columns.len()
                ));
            }

            ordinal - 1

        } else {

            columns
                .iter()
                .position(|column| column.field_name == item.field_name)
                .ok_or_else(|| {
                    format!(
                        "ORDER BY column '{}' is not present in UNION output",
                        item.field_name
                    )
                })?

        };

        order_indexes.push(index);

    }

    Ok(serverlib::apply_with_ties_rows(rows, &order_indexes, with_ties_limit))

}

pub(super) fn apply_set_boundary_operation(
    mut left_rows: Vec<Vec<Vec<u8>>>,
    right_rows: Vec<Vec<Vec<u8>>>,
    operation: serverlib::SelectSetBoundaryOp,
    comparison_columns: &[serverlib::FieldDef],
) -> Vec<Vec<Vec<u8>>> {

    match operation {

        serverlib::SelectSetBoundaryOp::UnionAll => {
            left_rows.extend(right_rows);
            left_rows
        },

        serverlib::SelectSetBoundaryOp::UnionDistinct => {
            left_rows.extend(right_rows);
            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);
            left_rows
        },

        serverlib::SelectSetBoundaryOp::ExceptDistinct => {
            
            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);

            let mut right_seen = HashSet::<Vec<Vec<u8>>>::new();
            for row in right_rows {
                right_seen.insert(union_row_comparison_key(&row, comparison_columns));
            }

            left_rows.retain(|row| {
                let key = union_row_comparison_key(row, comparison_columns);
                !right_seen.contains(&key)
            });

            left_rows

        },

        serverlib::SelectSetBoundaryOp::IntersectDistinct => {

            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);

            let mut right_seen = HashSet::<Vec<Vec<u8>>>::new();
            for row in right_rows {
                right_seen.insert(union_row_comparison_key(&row, comparison_columns));
            }

            left_rows.retain(|row| {
                let key = union_row_comparison_key(row, comparison_columns);
                right_seen.contains(&key)
            });

            left_rows

        },

    }

}

pub(super) fn reconcile_union_column_types(
    base_columns: &mut [serverlib::FieldDef],
    branch_columns: &[serverlib::FieldDef],
) -> Result<(), String> {

    for (index, (base_column, branch_column)) in base_columns
        .iter_mut()
        .zip(branch_columns.iter())
        .enumerate()
    {

        let resolved = resolve_union_column_type(&base_column.field_type, &branch_column.field_type)
            .ok_or_else(|| {
                format!(
                    "UNION column {} type mismatch: '{}' is not compatible with '{}'",
                    index + 1,
                    base_column.field_type.sql_variant_display_name(),
                    branch_column.field_type.sql_variant_display_name(),
                )
            })?;

        base_column.field_type = resolved;
        base_column.nullable = base_column.nullable || branch_column.nullable;

        reconcile_union_column_metadata(base_column, branch_column, index + 1)?;

    }

    Ok(())

}

pub(super) fn compare_union_cell_values(
    left: Option<&Vec<u8>>,
    right: Option<&Vec<u8>>,
    column: Option<&serverlib::FieldDef>,
) -> Ordering {
    
    match (left, right) {

        (Some(left), Some(right)) => {
            let left_key = union_cell_compare_key(left, column);
            let right_key = union_cell_compare_key(right, column);
            left_key.cmp(&right_key)
        }

        (None, Some(_)) => Ordering::Less,

        (Some(_), None) => Ordering::Greater,

        (None, None) => Ordering::Equal,

    }

}

pub(super) fn apply_union_row_window(
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

fn dedupe_union_rows_with_columns(
    rows: &mut Vec<Vec<Vec<u8>>>,
    columns: &[serverlib::FieldDef],
) {
    let mut seen = HashSet::<Vec<Vec<u8>>>::new();
    rows.retain(|row| seen.insert(union_row_comparison_key(row, columns)));
}

fn resolve_union_column_type(
    left: &serverlib::FieldType,
    right: &serverlib::FieldType,
) -> Option<serverlib::FieldType> {
    
    use serverlib::FieldType;

    if left == right {
        return Some(left.clone());
    }

    match (left, right) {

        (FieldType::Float(left_bits), FieldType::Float(right_bits)) => {
            Some(FieldType::Float((*left_bits).max(*right_bits)))
        },

        (FieldType::Int(left_bits), FieldType::Int(right_bits)) => {
            Some(FieldType::Int((*left_bits).max(*right_bits)))
        },

        (FieldType::UInt(left_bits), FieldType::UInt(right_bits)) => {
            Some(FieldType::UInt((*left_bits).max(*right_bits)))
        },

        (FieldType::Int(left_bits), FieldType::UInt(right_bits)) |
        (FieldType::UInt(right_bits), FieldType::Int(left_bits)) => {
            Some(resolve_mixed_signed_unsigned_int(*left_bits, *right_bits))
        },

        (FieldType::Float(left_bits), FieldType::Int(_)) |
        (FieldType::Int(_), FieldType::Float(left_bits)) |
        (FieldType::Float(left_bits), FieldType::UInt(_)) |
        (FieldType::UInt(_), FieldType::Float(left_bits)) => {
            Some(FieldType::Float((*left_bits).max(64)))
        },

        (FieldType::Date, FieldType::Date) |
        (FieldType::DateTime, FieldType::DateTime) |
        (FieldType::Timestamp, FieldType::Timestamp) => Some(left.clone()),

        (FieldType::Date, FieldType::DateTime) |
        (FieldType::DateTime, FieldType::Date) |
        (FieldType::Date, FieldType::Timestamp) |
        (FieldType::Timestamp, FieldType::Date) |
        (FieldType::DateTime, FieldType::Timestamp) |
        (FieldType::Timestamp, FieldType::DateTime) => Some(FieldType::DateTime),

        (FieldType::StringFixed(left_len), FieldType::StringFixed(right_len)) => {
            Some(FieldType::StringFixed((*left_len).max(*right_len)))
        },

        (FieldType::StringFixed(_), FieldType::Text) |
        (FieldType::Text, FieldType::StringFixed(_)) |
        (FieldType::Text, FieldType::Text) => Some(FieldType::Text),

        (FieldType::Enum(left_variants), FieldType::Enum(right_variants)) => {
            let left_max = max_enum_variant_len(left_variants);
            let right_max = max_enum_variant_len(right_variants);
            Some(FieldType::StringFixed(left_max.max(right_max).max(1)))
        },

        (FieldType::Enum(variants), FieldType::StringFixed(len)) |
        (FieldType::StringFixed(len), FieldType::Enum(variants)) => {
            let enum_max = max_enum_variant_len(variants);
            Some(FieldType::StringFixed(enum_max.max(*len).max(1)))
        },

        (FieldType::Enum(_), FieldType::Text) |
        (FieldType::Text, FieldType::Enum(_)) => Some(FieldType::Text),

        (FieldType::Blob, FieldType::Blob) => Some(FieldType::Blob),
        (FieldType::Spatial, FieldType::Spatial) => Some(FieldType::Spatial),

        // First-pass MySQL-like coercion: mixing scalar/date/string-like families
        // yields textual result typing in UNION metadata.
        (FieldType::Int(_), FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::Int(_)) |
        (FieldType::Int(_), FieldType::Text) |
        (FieldType::Text, FieldType::Int(_)) |
        (FieldType::UInt(_), FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::UInt(_)) |
        (FieldType::UInt(_), FieldType::Text) |
        (FieldType::Text, FieldType::UInt(_)) |
        (FieldType::Float(_), FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::Float(_)) |
        (FieldType::Float(_), FieldType::Text) |
        (FieldType::Text, FieldType::Float(_)) |
        (FieldType::Date, FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::Date) |
        (FieldType::Date, FieldType::Text) |
        (FieldType::Text, FieldType::Date) |
        (FieldType::DateTime, FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::DateTime) |
        (FieldType::DateTime, FieldType::Text) |
        (FieldType::Text, FieldType::DateTime) |
        (FieldType::Timestamp, FieldType::StringFixed(_)) |
        (FieldType::StringFixed(_), FieldType::Timestamp) |
        (FieldType::Timestamp, FieldType::Text) |
        (FieldType::Text, FieldType::Timestamp) |
        (FieldType::Enum(_), FieldType::Int(_)) |
        (FieldType::Int(_), FieldType::Enum(_)) |
        (FieldType::Enum(_), FieldType::UInt(_)) |
        (FieldType::UInt(_), FieldType::Enum(_)) |
        (FieldType::Enum(_), FieldType::Float(_)) |
        (FieldType::Float(_), FieldType::Enum(_)) |
        (FieldType::Enum(_), FieldType::Date) |
        (FieldType::Date, FieldType::Enum(_)) |
        (FieldType::Enum(_), FieldType::DateTime) |
        (FieldType::DateTime, FieldType::Enum(_)) |
        (FieldType::Enum(_), FieldType::Timestamp) |
        (FieldType::Timestamp, FieldType::Enum(_)) => Some(FieldType::Text),

        _ => None,

    }

}

fn reconcile_union_column_metadata(
    base_column: &mut serverlib::FieldDef,
    branch_column: &serverlib::FieldDef,
    column_index: usize,
) -> Result<(), String> {

    let base_metadata = base_column.metadata.clone().unwrap_or_default();
    let branch_metadata = branch_column.metadata.clone().unwrap_or_default();

    let resolved_character_set = reconcile_union_metadata_value(
        base_metadata.character_set.as_deref(),
        branch_metadata.character_set.as_deref(),
        column_index,
        "character set",
    )?;

    let resolved_collation = reconcile_union_metadata_value(
        base_metadata.collation.as_deref(),
        branch_metadata.collation.as_deref(),
        column_index,
        "collation",
    )?;

    let resolved_visibility = if base_metadata.is_hidden() || branch_metadata.is_hidden() {
        common::schema::SystemFieldVisibility::Hidden
    } else {
        common::schema::SystemFieldVisibility::Visible
    };

    let resolved_metadata = common::schema::FieldMetadata {
        comment: base_metadata.comment.or(branch_metadata.comment),
        auto_increment: base_metadata.auto_increment || branch_metadata.auto_increment,
        unique: base_metadata.unique || branch_metadata.unique,
        original_sql_type: base_metadata.original_sql_type.or(branch_metadata.original_sql_type),
        character_set: resolved_character_set,
        collation: resolved_collation,
        system_visibility: resolved_visibility,
    };

    if resolved_metadata == common::schema::FieldMetadata::default() {
        base_column.metadata = None;
    } else {
        base_column.metadata = Some(resolved_metadata);
    }

    Ok(())

}

fn reconcile_union_metadata_value(
    base_value: Option<&str>,
    branch_value: Option<&str>,
    column_index: usize,
    label: &str,
) -> Result<Option<String>, String> {

    match (base_value, branch_value) {

        (Some(base), Some(branch)) if base.eq_ignore_ascii_case(branch) => {
            Ok(Some(base.to_string()))
        },

        (Some(base), None) => Ok(Some(base.to_string())),
        
        (None, Some(branch)) => Ok(Some(branch.to_string())),
        
        (None, None) => Ok(None),
        
        (Some(base), Some(branch)) => Err(format!(
            "UNION column {} {} mismatch: '{}' is not compatible with '{}'",
            column_index, label, base, branch
        )),

    }

}

fn union_row_comparison_key(
    row: &[Vec<u8>],
    columns: &[serverlib::FieldDef],
) -> Vec<Vec<u8>> {
    
    row.iter()
        .enumerate()
        .map(|(index, cell)| union_cell_compare_key(cell, columns.get(index)))
        .collect()

}

fn union_cell_compare_key(cell: &[u8], column: Option<&serverlib::FieldDef>) -> Vec<u8> {

    let Some(column) = column else {
        return cell.to_vec();
    };

    if union_column_uses_case_insensitive_collation(column) {
        return String::from_utf8_lossy(cell).to_lowercase().into_bytes();
    }

    cell.to_vec()

}

fn union_column_uses_case_insensitive_collation(column: &serverlib::FieldDef) -> bool {

    let Some(collation) = column.metadata.as_ref().and_then(|metadata| metadata.collation.as_deref()) else {
        return false;
    };

    let normalized = collation.trim().to_ascii_lowercase();
    normalized.ends_with("_ci") || normalized.contains("_ci_")

}

fn resolve_mixed_signed_unsigned_int(left_signed_bits: u8, right_unsigned_bits: u8) -> serverlib::FieldType {

    // Keep UNION integer results in integer family instead of widening to float.
    // We conservatively promote mixed signed/unsigned values to signed 64-bit.
    
    if left_signed_bits >= right_unsigned_bits {
        serverlib::FieldType::Int(left_signed_bits.max(64))
    } else {
        serverlib::FieldType::Int(64)
    }

}

fn max_enum_variant_len(variants: &[String]) -> usize {

    variants
        .iter()
        .map(|variant| variant.len())
        .max()
        .unwrap_or(1)

}
