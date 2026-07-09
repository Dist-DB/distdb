use std::cmp::Ordering;
use std::collections::HashSet;

use crate::{FieldDef, SelectProjectionItem, SelectReadPlan};

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

    Ok(apply_row_window(rows, read_plan.limit, read_plan.offset))

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