use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::{FieldDef, SelectCondition, SelectOrderByItem, SelectProjectionItem, SelectReadPlan};
use crate::engine::sql::SelectLimitByPlan;
use crate::engine::execution::{ConditionValueProvider, row_matches_condition_with_result};
use crate::engine::execution::{apply_limit_by_rows, apply_percent_rows, apply_with_ties_rows};

use super::window::apply_window_projection_values;

struct QualifyRowProvider<'a> {
    field_indexes: &'a HashMap<&'a str, usize>,
    row: &'a [Vec<u8>],
}

impl ConditionValueProvider for QualifyRowProvider<'_> {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        let column_index = *self.field_indexes.get(field_name)?;

        self.row.get(column_index)

    }

}

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

    let column_indexes = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.field_name.clone(), index))
        .collect::<HashMap<_, _>>();

    if read_plan.distinct {

        let mut unique_rows = Vec::with_capacity(rows.len());
        let mut seen = HashSet::new();
        let all_visible = visible_indexes.len() == columns.len();

        for row in rows {

            if all_visible {
                if seen.contains(&row) {
                    continue;
                }

                seen.insert(row.clone());
                unique_rows.push(row);
                continue;
            }

            let key = {
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
            let index = column_indexes
                .get(&item.field_name)
                .copied()
                .or_else(|| {
                    let (_, unqualified) = item.field_name.rsplit_once('.')?;
                    let matches = columns
                        .iter()
                        .enumerate()
                        .filter(|(_, column)| {
                            column.field_name.eq_ignore_ascii_case(unqualified)
                        })
                        .map(|(index, _)| index)
                        .collect::<Vec<_>>();
                    (matches.len() == 1).then_some(matches[0])
                });

            if let Some(index) = index {
                order_indexes.push((index, item.descending));
            }
        }

        if !order_indexes.is_empty() {

            rows.sort_by(|left, right| {

                for (index, descending) in &order_indexes {

                    let ordering = compare_order_by_values(
                        left.get(*index).map(Vec::as_slice),
                        right.get(*index).map(Vec::as_slice),
                    );

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
        let Some(index) = resolve_order_by_projection_index(columns, item.field_name.as_str()) else {
            return Err(format!(
                "select failed: TOP WITH TIES ORDER BY column '{}' is not present in result projection",
                item.field_name
            ));
        };
        order_indexes.push(index);
    }

    Ok(apply_with_ties_rows(rows, &order_indexes, top_with_ties_limit))

}

/// Rendered values are text, so numeric columns must not fall back to lexicographic order.
fn compare_order_by_values(left: Option<&[u8]>, right: Option<&[u8]>) -> Ordering {

    let (Some(left), Some(right)) = (left, right) else {
        return left.cmp(&right);
    };

    if let (Ok(left_text), Ok(right_text)) = (
        std::str::from_utf8(left),
        std::str::from_utf8(right),
    ) && let (Ok(left_number), Ok(right_number)) = (
        left_text.trim().parse::<f64>(),
        right_text.trim().parse::<f64>(),
    ) {
        return left_number
            .partial_cmp(&right_number)
            .unwrap_or(Ordering::Equal);
    }

    left.cmp(right)

}

fn resolve_order_by_projection_index(columns: &[FieldDef], field_name: &str) -> Option<usize> {
    let direct_index = columns
        .iter()
        .enumerate()
        .find(|(_, column)| column.field_name.eq_ignore_ascii_case(field_name))
        .map(|(index, _)| index);

    if direct_index.is_some() {
        return direct_index;
    }

    let (_, unqualified) = field_name.split_once('.')?;

    let mut matches = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.field_name.eq_ignore_ascii_case(unqualified))
        .map(|(index, _)| index);

    let first = matches.next()?;
    if matches.next().is_some() {
        return None;
    }

    Some(first)

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

    let field_indexes = columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.field_name.as_str(), index))
        .collect::<HashMap<_, _>>();

    let mut filtered = Vec::with_capacity(rows.len());

    for row in rows {

        let row_provider = QualifyRowProvider {
            field_indexes: &field_indexes,
            row: &row,
        };

        let matched = row_matches_condition_with_result(
            &row_provider,
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

    let mut visible_flags = vec![false; columns.len()];
    for index in &visible_indexes {
        visible_flags[*index] = true;
    }

    let mut visible_columns = Vec::with_capacity(visible_indexes.len());
    for (index, mut column) in columns.into_iter().enumerate() {
        if !visible_flags[index] {
            continue;
        }

        column.seqno = (visible_columns.len() + 1) as u32;
        visible_columns.push(column);
    }

    let visible_rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .filter_map(|(index, value)| visible_flags[index].then_some(value))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    (visible_columns, visible_rows)
    
}