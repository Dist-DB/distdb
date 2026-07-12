use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::{FieldDef, SelectCondition, SelectOrderByItem, SelectProjectionItem, SelectReadPlan};
use crate::engine::sql::SelectLimitByPlan;
use crate::engine::execution::row_matches_condition_with_result;
use crate::engine::execution::{apply_limit_by_rows, apply_percent_rows, apply_with_ties_rows};

use super::window::apply_window_projection_values;

pub fn apply_row_window(
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

pub fn apply_select_post_processing(
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

    rows = apply_qualify_post_filter(rows, columns, read_plan.qualify_condition.as_ref())?;

    rows = apply_limit_by_post_filter(rows, columns, read_plan.limit_by.as_ref())?;

    rows = apply_top_percent_post_filter(rows, read_plan.top_percent);

    rows = apply_fetch_percent_post_filter(
        rows,
        columns,
        &read_plan.order_by,
        None,
        read_plan.top_percent_with_ties,
    )?;

    rows = apply_fetch_percent_post_filter(
        rows,
        columns,
        &read_plan.order_by,
        read_plan.fetch_percent,
        read_plan.fetch_percent_with_ties,
    )?;

    rows = apply_top_with_ties_post_filter(
        rows,
        columns,
        &read_plan.order_by,
        read_plan.top_with_ties_limit,
    )?;

    rows = apply_top_with_ties_post_filter(
        rows,
        columns,
        &read_plan.order_by,
        read_plan.fetch_with_ties_limit,
    )?;

    Ok(apply_row_window(rows, read_plan.limit, read_plan.offset))

}

fn apply_top_percent_post_filter(
    rows: Vec<Vec<Vec<u8>>>,
    top_percent: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {

    apply_percent_rows(rows, top_percent)

}

fn apply_fetch_percent_post_filter(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    order_by: &[SelectOrderByItem],
    fetch_percent: Option<usize>,
    fetch_percent_with_ties: Option<usize>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    if let Some(percent) = fetch_percent_with_ties {
        let rows_len = rows.len();

        if rows_len == 0 || percent == 0 {
            return Ok(Vec::new());
        }

        let capped_percent = percent.min(100);
        let bounded_rows = rows_len
            .saturating_mul(capped_percent)
            .saturating_add(99)
            / 100;

        return apply_top_with_ties_post_filter(rows, columns, order_by, Some(bounded_rows));
    }

    Ok(apply_top_percent_post_filter(rows, fetch_percent))

}

fn apply_top_with_ties_post_filter(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    order_by: &[SelectOrderByItem],
    top_with_ties_limit: Option<usize>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    let mut order_indexes = Vec::with_capacity(order_by.len());
    for item in order_by {
        let Some(index) = columns.iter().position(|column| column.field_name == item.field_name) else {
            return Err(format!(
                "select failed: TOP WITH TIES ORDER BY column '{}' is not present in result projection",
                item.field_name
            ));
        };
        order_indexes.push(index);
    }

    Ok(apply_with_ties_rows(rows, &order_indexes, top_with_ties_limit))

}

fn apply_limit_by_post_filter(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    limit_by: Option<&SelectLimitByPlan>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    apply_limit_by_rows(
        rows,
        columns,
        limit_by,
        "select failed: LIMIT BY column",
    )

}

fn apply_qualify_post_filter(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    qualify_condition: Option<&SelectCondition>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    if qualify_condition.is_none() {
        return Ok(rows);
    }

    let mut filtered = Vec::with_capacity(rows.len());

    for row in rows {
        let mut row_map = HashMap::with_capacity(columns.len());

        for (index, column) in columns.iter().enumerate() {
            row_map.insert(
                column.field_name.clone(),
                row.get(index).cloned().unwrap_or_else(|| b"NULL".to_vec()),
            );
        }

        let matched = row_matches_condition_with_result(
            &row_map,
            qualify_condition,
            &mut |_, _| {
                Err("QUALIFY subquery predicates are not supported in post-window evaluation".to_string())
            },
            &mut |_, _| {
                Err("QUALIFY subquery predicates are not supported in post-window evaluation".to_string())
            },
            &mut |_, _| {
                Err("QUALIFY subquery predicates are not supported in post-window evaluation".to_string())
            },
        )?;

        if matched {
            filtered.push(row);
        }
        
    }

    Ok(filtered)

}

pub fn column_metadata_with_visibility(
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

pub fn strip_hidden_output_columns(
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