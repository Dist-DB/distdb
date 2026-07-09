use std::cmp::Ordering;
use std::collections::HashMap;

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, NamedWindowDefinition,
    NamedWindowExpr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowSpec, WindowType,
};

use crate::{FieldDef, SelectProjectionItem};

pub fn apply_window_projection_values(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    projection_items: &[SelectProjectionItem],
    named_windows: &[NamedWindowDefinition],
) -> Result<(), String> {

    let window_indexes = projection_items
        .iter()
        .enumerate()
        .filter_map(|(index, projection_item)| match projection_item {
            SelectProjectionItem::WindowFunction { function, .. } => Some((index, function)),
            _ => None,
        })
        .collect::<Vec<_>>();

    if window_indexes.is_empty() {
        return Ok(());
    }

    for (column_index, function) in window_indexes {

        let window_spec = resolve_window_spec(function, named_windows)?;

        match function.name.to_string().to_ascii_lowercase().as_str() {

            "row_number" => {
                
                let (partition_indexes, order_indexes) =
                    window_partition_and_order_indexes(&window_spec, columns)?;

                let mut partitioned_row_indexes: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::new();

                for (row_index, row) in rows.iter().enumerate() {

                    let partition_key = partition_indexes
                        .iter()
                        .filter_map(|index| row.get(*index).cloned())
                        .collect::<Vec<_>>();

                    partitioned_row_indexes
                        .entry(partition_key)
                        .or_default()
                        .push(row_index);

                }

                for mut partition_row_indexes in partitioned_row_indexes.into_values() {

                    if !order_indexes.is_empty() {

                        partition_row_indexes.sort_by(|left, right| {

                            for (order_index, descending) in &order_indexes {

                                let ordering = rows[*left].get(*order_index).cmp(&rows[*right].get(*order_index));

                                if ordering != Ordering::Equal {
                                    return if *descending { ordering.reverse() } else { ordering };
                                }

                            }

                            left.cmp(right)

                        });

                    }

                    for (row_number, row_index) in partition_row_indexes.iter().enumerate() {

                        if let Some(cell) = rows[*row_index].get_mut(column_index) {
                            *cell = (row_number + 1).to_string().into_bytes();
                        }

                    }

                }
            
            },

            "sum" => {
                apply_sum_window_projection(rows, columns, column_index, function, &window_spec)?;
            },

            _ => {
                return Err(format!(
                    "SELECT window function '{}' is not supported yet",
                    function.name
                ));
            }

        }

    }

    Ok(())

}

fn window_partition_and_order_indexes(
    window_spec: &WindowSpec,
    columns: &[FieldDef],
) -> Result<(Vec<usize>, Vec<(usize, bool)>), String> {

    let mut partition_indexes = Vec::with_capacity(window_spec.partition_by.len());

    for expression in &window_spec.partition_by {

        let field_name = match expression {

            Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
            
            Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
                .iter()
                .map(|part| part.value.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("."),
            
            Expr::Value(sqlparser::ast::Value::Number(position, _)) => {
                let position = position.parse::<usize>().map_err(|_| {
                    "window ROW_NUMBER PARTITION BY ordinal must be an unsigned numeric literal"
                        .to_string()
                })?;

                if position == 0 {
                    return Err("window ROW_NUMBER PARTITION BY ordinal must start at 1".to_string());
                }

                let Some(column) = columns.get(position - 1) else {
                    return Err(format!("window ROW_NUMBER PARTITION BY ordinal {} is out of range", position));
                };

                column.field_name.clone()
            },

            _ => {
                return Err("window ROW_NUMBER PARTITION BY currently supports only direct column references or ordinals".to_string());
            }

        };

        let Some(column_index) = columns
            .iter()
            .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
        else {
            return Err(format!("window ROW_NUMBER PARTITION BY references unknown output field '{}'", field_name));
        };

        partition_indexes.push(column_index);

    }

    let mut order_indexes = Vec::with_capacity(window_spec.order_by.len());

    for expression in &window_spec.order_by {

        let field_name = match &expression.expr {

            Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
            
            Expr::CompoundIdentifier(parts) if parts.len() == 1 => parts[0].value.to_ascii_lowercase(),
            
            Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

                let position = position.parse::<usize>().map_err(|_| {
                    "window ROW_NUMBER ORDER BY ordinal must be an unsigned numeric literal".to_string()
                })?;

                if position == 0 {
                    return Err("window ROW_NUMBER ORDER BY ordinal must start at 1".to_string());
                }

                let Some(column) = columns.get(position - 1) else {
                    return Err(format!("window ROW_NUMBER ORDER BY ordinal {} is out of range", position));
                };

                column.field_name.clone()

            },

            _ => {
                return Err("window ROW_NUMBER ORDER BY currently supports only direct column references or ordinals".to_string());
            }

        };

        if expression.nulls_first.is_some() || expression.with_fill.is_some() {
            return Err("window ROW_NUMBER ORDER BY does not support NULLS FIRST/LAST or WITH FILL yet".to_string());
        }

        let Some(column_index) = columns
            .iter()
            .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
        else {
            return Err(format!("window ROW_NUMBER ORDER BY references unknown output field '{}'", field_name));
        };

        order_indexes.push((column_index, expression.asc == Some(false)));

    }

    Ok((partition_indexes, order_indexes))

}

fn resolve_window_spec(
    function: &Function,
    named_windows: &[NamedWindowDefinition],
) -> Result<WindowSpec, String> {

    let Some(window_type) = function.over.as_ref() else {
        return Err("window projection requires an OVER clause".to_string());
    };

    resolve_named_or_inline_window_spec(window_type, named_windows, &mut Vec::new())

}

fn resolve_named_or_inline_window_spec(
    window_type: &WindowType,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    match window_type {
        
        WindowType::NamedWindow(name) => resolve_named_window_spec(name, named_windows, visiting),

        WindowType::WindowSpec(window_spec) => resolve_window_spec_from_spec(window_spec, named_windows, visiting),

    }

}

fn resolve_window_spec_from_spec(
    window_spec: &WindowSpec,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    if let Some(window_name) = &window_spec.window_name {
        let base_spec = resolve_named_window_spec(window_name, named_windows, visiting)?;
        merge_window_specs(base_spec, window_spec)
    } else {
        Ok(window_spec.clone())
    }

}

fn resolve_named_window_spec(
    window_name: &sqlparser::ast::Ident,
    named_windows: &[NamedWindowDefinition],
    visiting: &mut Vec<String>,
) -> Result<WindowSpec, String> {

    let normalized_name = window_name.value.to_ascii_lowercase();

    if visiting.iter().any(|name| name == &normalized_name) {
        return Err(format!("window reference '{}' is recursive", window_name.value));
    }

    let Some(definition) = named_windows.iter().find(|definition| {
        definition.0.value.eq_ignore_ascii_case(&window_name.value)
    }) else {
        return Err(format!("named window '{}' is not defined", window_name.value));
    };

    visiting.push(normalized_name);

    let resolved = match &definition.1 {
        NamedWindowExpr::NamedWindow(reference) => resolve_named_window_spec(reference, named_windows, visiting),
        NamedWindowExpr::WindowSpec(window_spec) => resolve_window_spec_from_spec(window_spec, named_windows, visiting),
    };

    visiting.pop();

    resolved

}

fn merge_window_specs(base_spec: WindowSpec, override_spec: &WindowSpec) -> Result<WindowSpec, String> {

    if !base_spec.partition_by.is_empty() && !override_spec.partition_by.is_empty() {
        return Err("named window PARTITION BY cannot be overridden yet".to_string());
    }

    if !base_spec.order_by.is_empty() && !override_spec.order_by.is_empty() {
        return Err("named window ORDER BY cannot be overridden yet".to_string());
    }

    if base_spec.window_frame.is_some() && override_spec.window_frame.is_some() {
        return Err("named window frame cannot be overridden yet".to_string());
    }

    Ok(WindowSpec {
        window_name: None,
        partition_by: if override_spec.partition_by.is_empty() {
            base_spec.partition_by
        } else {
            override_spec.partition_by.clone()
        },
        order_by: if override_spec.order_by.is_empty() {
            base_spec.order_by
        } else {
            override_spec.order_by.clone()
        },
        window_frame: override_spec
            .window_frame
            .clone()
            .or(base_spec.window_frame),
    })

}

fn apply_sum_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
) -> Result<(), String> {

    let source_column_index = resolve_window_sum_source_column(function, columns)?;
    let (partition_indexes, order_indexes) = window_partition_and_order_indexes(window_spec, columns)?;

    let mut partitioned_row_indexes: HashMap<Vec<Vec<u8>>, Vec<usize>> = HashMap::new();

    for (row_index, row) in rows.iter().enumerate() {
        let partition_key = partition_indexes
            .iter()
            .filter_map(|index| row.get(*index).cloned())
            .collect::<Vec<_>>();

        partitioned_row_indexes
            .entry(partition_key)
            .or_default()
            .push(row_index);
    }

    for mut partition_row_indexes in partitioned_row_indexes.into_values() {

        if !order_indexes.is_empty() {
            partition_row_indexes.sort_by(|left, right| {
                for (order_index, descending) in &order_indexes {
                    let ordering = rows[*left].get(*order_index).cmp(&rows[*right].get(*order_index));

                    if ordering != Ordering::Equal {
                        return if *descending { ordering.reverse() } else { ordering };
                    }
                }

                left.cmp(right)
            });
        }

        let mut partition_values = Vec::with_capacity(partition_row_indexes.len());
        for row_index in &partition_row_indexes {
            let value = rows[*row_index]
                .get(source_column_index)
                .ok_or_else(|| format!("window SUM source column index {} is out of range", source_column_index))?;
            partition_values.push(parse_window_numeric_value(value)?);
        }

        let mut prefix_sums = Vec::with_capacity(partition_values.len() + 1);
        prefix_sums.push(0.0f64);

        for value in &partition_values {
            prefix_sums.push(prefix_sums.last().copied().unwrap_or(0.0) + value.unwrap_or(0.0));
        }

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds(window_spec.window_frame.as_ref(), row_position, partition_row_indexes.len())?;
            let sum_value = match frame_bounds {
                Some((start, end)) => prefix_sums[end + 1] - prefix_sums[start],
                None => 0.0,
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = render_window_numeric_value(sum_value);
            }

        }

    }

    Ok(())

}

fn resolve_window_sum_source_column(
    function: &Function,
    columns: &[FieldDef],
) -> Result<usize, String> {

    let FunctionArguments::List(list) = &function.args else {
        return Err("SUM window function currently requires exactly one argument".to_string());
    };

    let Some(argument) = list.args.first() else {
        return Err("SUM window function currently requires exactly one argument".to_string());
    };

    let expression = match argument {

        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,

        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,

        _ => {
            return Err("SUM window function currently supports only a direct column argument".to_string());
        }
        
    };

    resolve_window_field_index(
        expression,
        columns,
        "window SUM source",
        "SUM window function currently supports only a direct column argument",
        "window SUM source ordinal must be an unsigned numeric literal",
        "window SUM source ordinal must start at 1",
    )

}

fn resolve_window_field_index(
    expression: &Expr,
    columns: &[FieldDef],
    context: &str,
    unsupported_message: &str,
    ordinal_parse_message: &str,
    ordinal_start_message: &str,
) -> Result<usize, String> {

    let field_name = match expression {

        Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),
        
        Expr::CompoundIdentifier(parts) if !parts.is_empty() => parts
            .iter()
            .map(|part| part.value.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join("."),
        
        Expr::Value(sqlparser::ast::Value::Number(position, _)) => {

            let position = position
                .parse::<usize>()
                .map_err(|_| ordinal_parse_message.to_string())?;

            if position == 0 {
                return Err(ordinal_start_message.to_string());
            }

            let Some(column) = columns.get(position - 1) else {
                return Err(format!("{context} ordinal {} is out of range", position));
            };

            column.field_name.clone()

        },

        _ => {
            return Err(unsupported_message.to_string());
        }

    };

    let Some(column_index) = columns
        .iter()
        .position(|column| column.field_name.eq_ignore_ascii_case(&field_name))
    else {
        return Err(format!("{context} references unknown output field '{}'", field_name));
    };

    Ok(column_index)

}

fn parse_window_numeric_value(value: &[u8]) -> Result<Option<f64>, String> {

    if value.is_empty() || value == b"NULL" {
        return Ok(None);
    }

    let text = std::str::from_utf8(value)
        .map_err(|_| "window aggregate value is not valid UTF-8".to_string())?
        .trim();

    if text.is_empty() || text.eq_ignore_ascii_case("null") {
        return Ok(None);
    }

    text.parse::<f64>()
        .map(Some)
        .map_err(|_| format!("window aggregate value '{}' is not numeric", text))

}

fn render_window_numeric_value(value: f64) -> Vec<u8> {

    if value.fract() == 0.0 {
        (value as i128).to_string().into_bytes()
    } else {
        value.to_string().into_bytes()
    }

}

fn window_frame_bounds(
    frame: Option<&WindowFrame>,
    row_position: usize,
    partition_len: usize,
) -> Result<Option<(usize, usize)>, String> {

    let Some(frame) = frame else {
        return Ok(Some((0, partition_len.saturating_sub(1))));
    };

    if frame.units != WindowFrameUnits::Rows {
        return Err("only ROWS window frames are supported yet".to_string());
    }

    if partition_len == 0 {
        return Ok(None);
    }

    let start = window_frame_bound_index(&frame.start_bound, row_position, partition_len)?;
    let end_bound = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
    let end = window_frame_bound_index(end_bound, row_position, partition_len)?;

    if start > end {
        return Ok(None);
    }

    Ok(Some((start, end)))

}

fn window_frame_bound_index(
    bound: &WindowFrameBound,
    row_position: usize,
    partition_len: usize,
) -> Result<usize, String> {

    let last_index = partition_len.saturating_sub(1);

    match bound {
        
        WindowFrameBound::CurrentRow => Ok(row_position.min(last_index)),
        
        WindowFrameBound::Preceding(None) => Ok(0),
        
        WindowFrameBound::Following(None) => Ok(last_index),

        WindowFrameBound::Preceding(Some(expr)) => Ok(row_position
            .saturating_sub(parse_window_frame_offset(expr)?)
            .min(last_index)),

        WindowFrameBound::Following(Some(expr)) => Ok(row_position
            .saturating_add(parse_window_frame_offset(expr)?)
            .min(last_index)),

    }

}

fn parse_window_frame_offset(expr: &Expr) -> Result<usize, String> {

    match expr {
        Expr::Value(sqlparser::ast::Value::Number(value, _)) => value
            .parse::<usize>()
            .map_err(|_| "window frame offset must be an unsigned numeric literal".to_string()),
        _ => Err("window frame offset currently supports only unsigned numeric literals".to_string()),
    }

}