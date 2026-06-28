use std::cmp::Ordering;
use std::collections::HashSet;

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

        (FieldType::Int(left_bits), FieldType::UInt(right_bits))
        | (FieldType::UInt(right_bits), FieldType::Int(left_bits)) => {
            Some(resolve_mixed_signed_unsigned_int(*left_bits, *right_bits))
        },

        (FieldType::Float(left_bits), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Float(left_bits))
        | (FieldType::Float(left_bits), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Float(left_bits)) => {
            Some(FieldType::Float((*left_bits).max(64)))
        },

        (FieldType::Date, FieldType::Date)
        | (FieldType::DateTime, FieldType::DateTime)
        | (FieldType::Timestamp, FieldType::Timestamp) => Some(left.clone()),

        (FieldType::Date, FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Date)
        | (FieldType::Date, FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Date)
        | (FieldType::DateTime, FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::DateTime) => Some(FieldType::DateTime),

        (FieldType::StringFixed(left_len), FieldType::StringFixed(right_len)) => {
            Some(FieldType::StringFixed((*left_len).max(*right_len)))
        },

        (FieldType::StringFixed(_), FieldType::Text)
        | (FieldType::Text, FieldType::StringFixed(_))
        | (FieldType::Text, FieldType::Text) => Some(FieldType::Text),

        (FieldType::Enum(left_variants), FieldType::Enum(right_variants)) => {
            let left_max = max_enum_variant_len(left_variants);
            let right_max = max_enum_variant_len(right_variants);
            Some(FieldType::StringFixed(left_max.max(right_max).max(1)))
        },

        (FieldType::Enum(variants), FieldType::StringFixed(len))
        | (FieldType::StringFixed(len), FieldType::Enum(variants)) => {
            let enum_max = max_enum_variant_len(variants);
            Some(FieldType::StringFixed(enum_max.max(*len).max(1)))
        },

        (FieldType::Enum(_), FieldType::Text)
        | (FieldType::Text, FieldType::Enum(_)) => Some(FieldType::Text),

        (FieldType::Blob, FieldType::Blob) => Some(FieldType::Blob),
        (FieldType::Spatial, FieldType::Spatial) => Some(FieldType::Spatial),

        // First-pass MySQL-like coercion: mixing scalar/date/string-like families
        // yields textual result typing in UNION metadata.
        (FieldType::Int(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Text)
        | (FieldType::Text, FieldType::Int(_))
        | (FieldType::UInt(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Text)
        | (FieldType::Text, FieldType::UInt(_))
        | (FieldType::Float(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Float(_))
        | (FieldType::Float(_), FieldType::Text)
        | (FieldType::Text, FieldType::Float(_))
        | (FieldType::Date, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Date)
        | (FieldType::Date, FieldType::Text)
        | (FieldType::Text, FieldType::Date)
        | (FieldType::DateTime, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Text)
        | (FieldType::Text, FieldType::DateTime)
        | (FieldType::Timestamp, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Text)
        | (FieldType::Text, FieldType::Timestamp)
        | (FieldType::Enum(_), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Float(_))
        | (FieldType::Float(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Date)
        | (FieldType::Date, FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Enum(_)) => Some(FieldType::Text),

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
