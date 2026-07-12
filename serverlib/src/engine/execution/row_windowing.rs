use std::collections::HashMap;

use crate::{FieldDef, SelectLimitByPlan};

pub fn apply_percent_rows(
    rows: Vec<Vec<Vec<u8>>>,
    percent: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {

    let Some(percent) = percent else {
        return rows;
    };

    if rows.is_empty() || percent == 0 {
        return Vec::new();
    }

    let capped_percent = percent.min(100);
    let total_rows = rows.len();
    let bounded_rows = total_rows
        .saturating_mul(capped_percent)
        .saturating_add(99)
        / 100;

    rows.into_iter().take(bounded_rows).collect()

}

pub fn apply_with_ties_rows(
    rows: Vec<Vec<Vec<u8>>>,
    order_indexes: &[usize],
    with_ties_limit: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {

    let Some(limit) = with_ties_limit else {
        return rows;
    };

    if limit == 0 || rows.is_empty() {
        return Vec::new();
    }

    if limit >= rows.len() {
        return rows;
    }

    if order_indexes.is_empty() {
        return rows.into_iter().take(limit).collect();
    }

    let boundary_index = limit - 1;
    let mut end = limit;

    while end < rows.len()
        && order_indexes
            .iter()
            .all(|index| rows[end].get(*index) == rows[boundary_index].get(*index))
    {
        end += 1;
    }

    rows.into_iter().take(end).collect()

}

pub fn apply_limit_by_rows(
    rows: Vec<Vec<Vec<u8>>>,
    columns: &[FieldDef],
    limit_by: Option<&SelectLimitByPlan>,
    missing_column_error_prefix: &str,
) -> Result<Vec<Vec<Vec<u8>>>, String> {

    let Some(limit_by) = limit_by else {
        return Ok(rows);
    };

    let mut key_indexes = Vec::with_capacity(limit_by.fields.len());
    for field_name in &limit_by.fields {
        let Some(index) = columns.iter().position(|column| column.field_name == *field_name) else {
            return Err(format!("{missing_column_error_prefix} '{}'", field_name));
        };
        key_indexes.push(index);
    }

    let mut per_key_counts = HashMap::<Vec<Vec<u8>>, usize>::new();
    let mut limited_rows = Vec::with_capacity(rows.len());

    for row in rows {
        let key = key_indexes
            .iter()
            .map(|index| row.get(*index).cloned().unwrap_or_default())
            .collect::<Vec<_>>();

        let count = per_key_counts.entry(key).or_insert(0);
        if *count < limit_by.per_key_limit {
            *count += 1;
            limited_rows.push(row);
        }
    }

    Ok(limited_rows)

}