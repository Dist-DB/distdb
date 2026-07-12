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
        let (partition_indexes, order_indexes) =
            window_partition_and_order_indexes(&window_spec, columns)?;

        match function.name.to_string().to_ascii_lowercase().as_str() {

            "row_number" => {
                apply_row_number_window_projection(
                    rows,
                    column_index,
                    &partition_indexes,
                    &order_indexes,
                );
            },

            "rank" => {
                apply_rank_window_projection(
                    rows,
                    column_index,
                    &partition_indexes,
                    &order_indexes,
                );
            },

            "dense_rank" => {
                apply_dense_rank_window_projection(
                    rows,
                    column_index,
                    &partition_indexes,
                    &order_indexes,
                );
            },

            "percent_rank" => {
                apply_percent_rank_window_projection(
                    rows,
                    column_index,
                    &partition_indexes,
                    &order_indexes,
                );
            },

            "cume_dist" => {
                apply_cume_dist_window_projection(
                    rows,
                    column_index,
                    &partition_indexes,
                    &order_indexes,
                );
            },

            "ntile" => {
                apply_ntile_window_projection(
                    rows,
                    column_index,
                    function,
                    &partition_indexes,
                    &order_indexes,
                )?;
            },

            "lag" => {
                apply_lag_lead_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &partition_indexes,
                    &order_indexes,
                    false,
                )?;
            },

            "lead" => {
                apply_lag_lead_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &partition_indexes,
                    &order_indexes,
                    true,
                )?;
            },

            "sum" => {
                apply_sum_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                )?;
            },

            "count" => {
                apply_count_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                )?;
            },

            "avg" => {
                apply_avg_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                )?;
            },

            "min" => {
                apply_min_max_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                    false,
                )?;
            },

            "max" => {
                apply_min_max_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                    true,
                )?;
            },

            "first_value" => {
                apply_first_last_value_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                    false,
                )?;
            },

            "last_value" => {
                apply_first_last_value_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                    true,
                )?;
            },

            "nth_value" => {
                apply_nth_value_window_projection(
                    rows,
                    columns,
                    column_index,
                    function,
                    &window_spec,
                    &partition_indexes,
                    &order_indexes,
                )?;
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

fn partitioned_row_indexes(
    rows: &[Vec<Vec<u8>>],
    partition_indexes: &[usize],
) -> HashMap<Vec<Vec<u8>>, Vec<usize>> {

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

    partitioned_row_indexes

}

fn sort_partition_row_indexes(
    rows: &[Vec<Vec<u8>>],
    partition_row_indexes: &mut [usize],
    order_indexes: &[(usize, bool)],
) {

    if order_indexes.is_empty() {
        return;
    }

    partition_row_indexes.sort_by(|left, right| {
        compare_rows_by_order(rows, *left, *right, order_indexes)
    });

}

fn compare_rows_by_order(
    rows: &[Vec<Vec<u8>>],
    left: usize,
    right: usize,
    order_indexes: &[(usize, bool)],
) -> Ordering {

    for (order_index, descending) in order_indexes {

        let ordering = rows[left].get(*order_index).cmp(&rows[right].get(*order_index));

        if ordering != Ordering::Equal {
            return if *descending { ordering.reverse() } else { ordering };
        }

    }

    left.cmp(&right)

}

fn rows_are_window_peers(
    rows: &[Vec<Vec<u8>>],
    left: usize,
    right: usize,
    order_indexes: &[(usize, bool)],
) -> bool {

    if order_indexes.is_empty() {
        return true;
    }

    order_indexes.iter().all(|(order_index, _)| {
        rows[left].get(*order_index) == rows[right].get(*order_index)
    })

}

fn apply_row_number_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) {

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        for (row_number, row_index) in partition_row_indexes.iter().enumerate() {

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = (row_number + 1).to_string().into_bytes();
            }

        }

    }

}

fn apply_rank_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) {

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let mut current_rank = 1usize;

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            if row_position > 0 {
                let previous_row_index = partition_row_indexes[row_position - 1];

                if !rows_are_window_peers(rows, previous_row_index, *row_index, order_indexes) {
                    current_rank = row_position + 1;
                }
            }

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = current_rank.to_string().into_bytes();
            }

        }

    }

}

fn apply_dense_rank_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) {

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let mut current_rank = 1usize;

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            if row_position > 0 {
                let previous_row_index = partition_row_indexes[row_position - 1];

                if !rows_are_window_peers(rows, previous_row_index, *row_index, order_indexes) {
                    current_rank = current_rank.saturating_add(1);
                }
            }

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = current_rank.to_string().into_bytes();
            }

        }

    }

}

fn apply_percent_rank_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) {

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let partition_len = partition_row_indexes.len();
        let mut current_rank = 1usize;

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            if row_position > 0 {
                let previous_row_index = partition_row_indexes[row_position - 1];

                if !rows_are_window_peers(rows, previous_row_index, *row_index, order_indexes) {
                    current_rank = row_position + 1;
                }
            }

            let percent_rank = if partition_len <= 1 {
                0.0
            } else {
                (current_rank.saturating_sub(1)) as f64 / (partition_len.saturating_sub(1)) as f64
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = render_window_numeric_value(percent_rank);
            }

        }

    }

}

fn apply_cume_dist_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) {

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let partition_len = partition_row_indexes.len();
        if partition_len == 0 {
            continue;
        }

        let mut group_start = 0usize;

        while group_start < partition_len {

            let mut group_end = group_start;
            while group_end + 1 < partition_len
                && rows_are_window_peers(
                    rows,
                    partition_row_indexes[group_end],
                    partition_row_indexes[group_end + 1],
                    order_indexes,
                )
            {
                group_end += 1;
            }

            let cume_dist = (group_end + 1) as f64 / partition_len as f64;

            for row_index in &partition_row_indexes[group_start..=group_end] {
                if let Some(cell) = rows[*row_index].get_mut(column_index) {
                    *cell = render_window_numeric_value(cume_dist);
                }
            }

            group_start = group_end + 1;

        }

    }

}

fn apply_ntile_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    column_index: usize,
    function: &Function,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Result<(), String> {

    let bucket_count = resolve_ntile_bucket_count(function)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let partition_len = partition_row_indexes.len();
        if partition_len == 0 {
            continue;
        }

        let base_bucket_size = partition_len / bucket_count;
        let remainder = partition_len % bucket_count;
        let large_bucket_size = base_bucket_size.saturating_add(1);
        let large_bucket_rows = large_bucket_size.saturating_mul(remainder);

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let bucket_number = if row_position < large_bucket_rows {
                row_position / large_bucket_size + 1
            } else {
                remainder + ((row_position - large_bucket_rows) / base_bucket_size) + 1
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = bucket_number.to_string().into_bytes();
            }

        }

    }

    Ok(())

}

fn resolve_ntile_bucket_count(function: &Function) -> Result<usize, String> {

    let FunctionArguments::List(list) = &function.args else {
        return Err("NTILE window function currently requires exactly one argument".to_string());
    };

    if list.args.len() != 1 {
        return Err("NTILE window function currently requires exactly one argument".to_string());
    }

    let bucket_expr = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
        _ => {
            return Err("NTILE window function bucket count must be an unsigned numeric literal".to_string());
        }
    };

    let bucket_count = match bucket_expr {
        Expr::Value(sqlparser::ast::Value::Number(number, _)) => number.parse::<usize>().map_err(|_| {
            "NTILE window function bucket count must be an unsigned numeric literal".to_string()
        })?,
        _ => {
            return Err("NTILE window function bucket count must be an unsigned numeric literal".to_string());
        }
    };

    if bucket_count == 0 {
        return Err("NTILE window function bucket count must be greater than 0".to_string());
    }

    Ok(bucket_count)

}

fn apply_lag_lead_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
    is_lead: bool,
) -> Result<(), String> {

    let (source_column_index, offset, default_value) = resolve_lag_lead_arguments(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {
        
        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let target_position = if is_lead {
                row_position.checked_add(offset)
            } else {
                row_position.checked_sub(offset)
            };

            let resolved = target_position
                .and_then(|position| partition_row_indexes.get(position).copied())
                .and_then(|source_row_index| rows[source_row_index].get(source_column_index).cloned())
                .unwrap_or_else(|| default_value.clone().unwrap_or_else(|| b"NULL".to_vec()));

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = resolved;
            }

        }

    }

    Ok(())

}

fn resolve_lag_lead_arguments(
    function: &Function,
    columns: &[FieldDef],
) -> Result<(usize, usize, Option<Vec<u8>>), String> {

    let function_name = function.name.to_string().to_ascii_uppercase();

    let FunctionArguments::List(list) = &function.args else {
        return Err(format!(
            "{} window function currently requires between 1 and 3 arguments",
            function_name
        ));
    };

    if list.args.is_empty() || list.args.len() > 3 {
        return Err(format!(
            "{} window function currently requires between 1 and 3 arguments",
            function_name
        ));
    }

    let source_expression = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
        _ => {
            return Err(format!(
                "{} window function currently supports only a direct column source argument",
                function_name
            ));
        }
    };

    let source_column_index = resolve_window_field_index(
        source_expression,
        columns,
        &format!("window {function_name} source"),
        &format!(
            "{} window function currently supports only a direct column source argument",
            function_name
        ),
        &format!("window {function_name} source ordinal must be an unsigned numeric literal"),
        &format!("window {function_name} source ordinal must start at 1"),
    )?;

    let offset = if let Some(offset_arg) = list.args.get(1) {
        let offset_expr = match offset_arg {

            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,

            FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
            
            _ => {
                return Err(format!(
                    "{} window function offset must be an unsigned numeric literal",
                    function_name
                ));
            }

        };

        match offset_expr {

            Expr::Value(sqlparser::ast::Value::Number(number, _)) => {
                number.parse::<usize>().map_err(|_| {
                    format!(
                        "{} window function offset must be an unsigned numeric literal",
                        function_name
                    )
                })?
            },

            _ => {
                return Err(format!(
                    "{} window function offset must be an unsigned numeric literal",
                    function_name
                ));
            }

        }

    } else {
        1
    };

    let default_value = if let Some(default_arg) = list.args.get(2) {
        let default_expr = match default_arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
            FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
            _ => {
                return Err(format!(
                    "{} window function default value must be a literal",
                    function_name
                ));
            }
        };

        Some(render_window_default_value(default_expr).map_err(|message| {
            format!("{} window function {message}", function_name)
        })?)
    } else {
        None
    };

    Ok((source_column_index, offset, default_value))

}

fn render_window_default_value(expression: &Expr) -> Result<Vec<u8>, String> {

    match expression {

        Expr::Value(sqlparser::ast::Value::Null) => Ok(b"NULL".to_vec()),

        Expr::Value(sqlparser::ast::Value::Number(number, _)) => Ok(number.clone().into_bytes()),

        Expr::Value(sqlparser::ast::Value::SingleQuotedString(value)) |
        Expr::Value(sqlparser::ast::Value::DoubleQuotedString(value)) => {
            Ok(value.clone().into_bytes())
        },

        Expr::Value(sqlparser::ast::Value::Boolean(value)) => Ok(value.to_string().into_bytes()),

        Expr::UnaryOp { op, expr } => {

            let Expr::Value(sqlparser::ast::Value::Number(number, _)) = expr.as_ref() else {
                return Err("default value must be a literal".to_string());
            };

            match op {
                sqlparser::ast::UnaryOperator::Plus => Ok(number.clone().into_bytes()),
                sqlparser::ast::UnaryOperator::Minus => Ok(format!("-{number}").into_bytes()),
                _ => Err("default value must be a literal".to_string()),
            }

        },

        _ => Err("default value must be a literal".to_string()),

    }

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
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Result<(), String> {

    let source_column_index = resolve_window_single_source_column(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

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

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                row_position,
            )?;
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

fn apply_count_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Result<(), String> {

    let source_column_index = resolve_window_single_source_column(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let mut prefix_counts = Vec::with_capacity(partition_row_indexes.len() + 1);
        prefix_counts.push(0usize);

        for row_index in &partition_row_indexes {
            let value = rows[*row_index]
                .get(source_column_index)
                .ok_or_else(|| format!(
                    "window COUNT source column index {} is out of range",
                    source_column_index
                ))?;

            let previous = *prefix_counts.last().unwrap_or(&0);
            let next = if is_window_null_value(value) {
                previous
            } else {
                previous.saturating_add(1)
            };
            prefix_counts.push(next);
        }

        for (position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                position,
            )?;

            let count_value = match frame_bounds {
                Some((start, end)) => prefix_counts[end + 1].saturating_sub(prefix_counts[start]),
                None => 0,
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = count_value.to_string().into_bytes();
            }

        }

    }

    Ok(())

}

fn apply_avg_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Result<(), String> {

    let source_column_index = resolve_window_single_source_column(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let mut partition_values = Vec::with_capacity(partition_row_indexes.len());
        
        for row_index in &partition_row_indexes {
            let value = rows[*row_index]
                .get(source_column_index)
                .ok_or_else(|| {
                    format!(
                        "window AVG source column index {} is out of range",
                        source_column_index
                    )
                })?;
            partition_values.push(parse_window_numeric_value(value)?);
        }

        let mut prefix_sums = Vec::with_capacity(partition_values.len() + 1);
        let mut prefix_counts = Vec::with_capacity(partition_values.len() + 1);
        prefix_sums.push(0.0f64);
        prefix_counts.push(0usize);

        for value in &partition_values {
            prefix_sums.push(prefix_sums.last().copied().unwrap_or(0.0) + value.unwrap_or(0.0));
            prefix_counts.push(
                prefix_counts
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(usize::from(value.is_some())),
            );
        }

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                row_position,
            )?;

            let avg_value = match frame_bounds {
                Some((start, end)) => {
                    let sum = prefix_sums[end + 1] - prefix_sums[start];
                    let count = prefix_counts[end + 1].saturating_sub(prefix_counts[start]);

                    if count == 0 {
                        0.0
                    } else {
                        sum / count as f64
                    }
                },
                None => 0.0,
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = render_window_numeric_value(avg_value);
            }

        }

    }

    Ok(())

}

fn apply_min_max_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
    is_max: bool,
) -> Result<(), String> {

    let source_column_index = resolve_window_single_source_column(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let mut partition_values = Vec::with_capacity(partition_row_indexes.len());
        for row_index in &partition_row_indexes {
            let value = rows[*row_index]
                .get(source_column_index)
                .ok_or_else(|| {
                    format!(
                        "window {} source column index {} is out of range",
                        if is_max { "MAX" } else { "MIN" },
                        source_column_index
                    )
                })?;
            partition_values.push(parse_window_numeric_value(value)?);
        }

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                row_position,
            )?;

            let value = match frame_bounds {
                Some((start, end)) => {
                    let mut selected: Option<f64> = None;

                    for candidate in partition_values[start..=end].iter().flatten() {
                        selected = Some(match selected {
                            Some(current) => {
                                if is_max {
                                    current.max(*candidate)
                                } else {
                                    current.min(*candidate)
                                }
                            },
                            None => *candidate,
                        });
                    }

                    selected.unwrap_or(0.0)
                },
                None => 0.0,
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = render_window_numeric_value(value);
            }

        }

    }

    Ok(())

}

fn apply_first_last_value_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
    is_last: bool,
) -> Result<(), String> {

    let source_column_index = resolve_window_single_source_column(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let partition_values = partition_row_indexes
            .iter()
            .map(|row_index| {
                rows[*row_index].get(source_column_index).cloned().ok_or_else(|| {
                    format!(
                        "window {} source column index {} is out of range",
                        if is_last { "LAST_VALUE" } else { "FIRST_VALUE" },
                        source_column_index,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                row_position,
            )?;

            let resolved = match frame_bounds {
                Some((start, end)) => {
                    let selected_index = if is_last { end } else { start };
                    partition_values
                        .get(selected_index)
                        .cloned()
                        .unwrap_or_else(|| b"NULL".to_vec())
                }
                None => b"NULL".to_vec(),
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = resolved;
            }

        }

    }

    Ok(())

}

fn apply_nth_value_window_projection(
    rows: &mut [Vec<Vec<u8>>],
    columns: &[FieldDef],
    column_index: usize,
    function: &Function,
    window_spec: &WindowSpec,
    partition_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Result<(), String> {

    let (source_column_index, nth_position) = resolve_nth_value_arguments(function, columns)?;

    for mut partition_row_indexes in partitioned_row_indexes(rows, partition_indexes).into_values() {

        sort_partition_row_indexes(rows, &mut partition_row_indexes, order_indexes);

        let partition_values = partition_row_indexes
            .iter()
            .map(|row_index| {
                rows[*row_index].get(source_column_index).cloned().ok_or_else(|| {
                    format!(
                        "window NTH_VALUE source column index {} is out of range",
                        source_column_index,
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (row_position, row_index) in partition_row_indexes.iter().enumerate() {

            let frame_bounds = window_frame_bounds_for_partition(
                rows,
                &partition_row_indexes,
                order_indexes,
                window_spec.window_frame.as_ref(),
                row_position,
            )?;

            let resolved = match frame_bounds {
                Some((start, end)) => {
                    let frame_len = end.saturating_sub(start).saturating_add(1);

                    if nth_position > frame_len {
                        b"NULL".to_vec()
                    } else {
                        partition_values
                            .get(start + (nth_position - 1))
                            .cloned()
                            .unwrap_or_else(|| b"NULL".to_vec())
                    }
                }
                None => b"NULL".to_vec(),
            };

            if let Some(cell) = rows[*row_index].get_mut(column_index) {
                *cell = resolved;
            }

        }

    }

    Ok(())

}

fn resolve_nth_value_arguments(
    function: &Function,
    columns: &[FieldDef],
) -> Result<(usize, usize), String> {

    let FunctionArguments::List(list) = &function.args else {
        return Err("NTH_VALUE window function currently requires exactly two arguments".to_string());
    };

    if list.args.len() != 2 {
        return Err("NTH_VALUE window function currently requires exactly two arguments".to_string());
    }

    let source_expression = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
        _ => {
            return Err(
                "NTH_VALUE window function currently supports only a direct column source argument"
                    .to_string(),
            );
        }
    };

    let nth_expression = match &list.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named { arg: FunctionArgExpr::Expr(expression), .. } => expression,
        _ => {
            return Err("NTH_VALUE window function ordinal must be an unsigned numeric literal".to_string());
        }
    };

    let source_column_index = resolve_window_field_index(
        source_expression,
        columns,
        "window NTH_VALUE source",
        "NTH_VALUE window function currently supports only a direct column source argument",
        "window NTH_VALUE source ordinal must be an unsigned numeric literal",
        "window NTH_VALUE source ordinal must start at 1",
    )?;

    let nth_position = match nth_expression {
        Expr::Value(sqlparser::ast::Value::Number(number, _)) => number.parse::<usize>().map_err(|_| {
            "NTH_VALUE window function ordinal must be an unsigned numeric literal".to_string()
        })?,
        _ => {
            return Err("NTH_VALUE window function ordinal must be an unsigned numeric literal".to_string());
        }
    };

    if nth_position == 0 {
        return Err("NTH_VALUE window function ordinal must start at 1".to_string());
    }

    Ok((source_column_index, nth_position))

}

fn resolve_window_single_source_column(
    function: &Function,
    columns: &[FieldDef],
) -> Result<usize, String> {

    let function_name = function.name.to_string().to_ascii_uppercase();

    let FunctionArguments::List(list) = &function.args else {
        return Err(format!(
            "{} window function currently requires exactly one argument",
            function_name
        ));
    };

    let Some(argument) = list.args.first() else {
        return Err(format!(
            "{} window function currently requires exactly one argument",
            function_name
        ));
    };

    let expression = match argument {

        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,

        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,

        _ => {
            return Err(format!(
                "{} window function currently supports only a direct column argument",
                function_name
            ));
        }
        
    };

    resolve_window_field_index(
        expression,
        columns,
        &format!("window {} source", function_name),
        &format!(
            "{} window function currently supports only a direct column argument",
            function_name
        ),
        &format!(
            "window {} source ordinal must be an unsigned numeric literal",
            function_name
        ),
        &format!("window {} source ordinal must start at 1", function_name),
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

fn is_window_null_value(value: &[u8]) -> bool {

    if value.is_empty() || value == b"NULL" {
        return true;
    }

    String::from_utf8(value.to_vec())
        .map(|text| text.trim().eq_ignore_ascii_case("null"))
        .unwrap_or(false)

}

fn render_window_numeric_value(value: f64) -> Vec<u8> {

    if value.fract() == 0.0 {
        (value as i128).to_string().into_bytes()
    } else {
        value.to_string().into_bytes()
    }

}

fn window_frame_bounds_for_partition(
    rows: &[Vec<Vec<u8>>],
    partition_row_indexes: &[usize],
    order_indexes: &[(usize, bool)],
    frame: Option<&WindowFrame>,
    row_position: usize,
) -> Result<Option<(usize, usize)>, String> {

    let partition_len = partition_row_indexes.len();

    let Some(frame) = frame else {
        return Ok(Some((0, partition_len.saturating_sub(1))));
    };

    if partition_len == 0 {
        return Ok(None);
    }

    let (start, end) = match frame.units {

        WindowFrameUnits::Rows => {
            let start = window_frame_rows_bound_index(&frame.start_bound, row_position, partition_len)?;
            let end_bound = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
            let end = window_frame_rows_bound_index(end_bound, row_position, partition_len)?;
            (start, end)
        },

        WindowFrameUnits::Groups => {
            let peer_groups = partition_peer_groups(rows, partition_row_indexes, order_indexes);
            let current_group = peer_group_index_for_row(row_position, &peer_groups);

            let start_group = window_frame_groups_bound_index(&frame.start_bound, current_group, peer_groups.len())?;
            let end_bound = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
            let end_group = window_frame_groups_bound_index(end_bound, current_group, peer_groups.len())?;

            let start = peer_groups[start_group].0;
            let end = peer_groups[end_group].1;
            (start, end)
        },

        WindowFrameUnits::Range => {
            let end_bound = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);

            window_frame_range_bounds(
                rows,
                partition_row_indexes,
                order_indexes,
                row_position,
                &frame.start_bound,
                end_bound,
            )?
        },

    };

    if start > end {
        return Ok(None);
    }

    Ok(Some((start, end)))

}

fn window_frame_rows_bound_index(
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

fn partition_peer_groups(
    rows: &[Vec<Vec<u8>>],
    partition_row_indexes: &[usize],
    order_indexes: &[(usize, bool)],
) -> Vec<(usize, usize)> {

    if partition_row_indexes.is_empty() {
        return Vec::new();
    }

    let mut groups = Vec::new();
    let mut start = 0usize;

    for position in 1..partition_row_indexes.len() {
        if !rows_are_window_peers(
            rows,
            partition_row_indexes[position - 1],
            partition_row_indexes[position],
            order_indexes,
        ) {
            groups.push((start, position - 1));
            start = position;
        }
    }

    groups.push((start, partition_row_indexes.len() - 1));
    groups

}

fn peer_group_index_for_row(row_position: usize, groups: &[(usize, usize)]) -> usize {
    groups
        .iter()
        .position(|(start, end)| row_position >= *start && row_position <= *end)
        .unwrap_or(0)
}

fn window_frame_groups_bound_index(
    bound: &WindowFrameBound,
    current_group: usize,
    total_groups: usize,
) -> Result<usize, String> {

    let last_group = total_groups.saturating_sub(1);

    match bound {
        
        WindowFrameBound::CurrentRow => Ok(current_group.min(last_group)),

        WindowFrameBound::Preceding(None) => Ok(0),

        WindowFrameBound::Following(None) => Ok(last_group),

        WindowFrameBound::Preceding(Some(expr)) => Ok(current_group
            .saturating_sub(parse_window_frame_offset(expr)?)
            .min(last_group)),

        WindowFrameBound::Following(Some(expr)) => Ok(current_group
            .saturating_add(parse_window_frame_offset(expr)?)
            .min(last_group)),

    }

}

fn window_frame_range_bounds(
    rows: &[Vec<Vec<u8>>],
    partition_row_indexes: &[usize],
    order_indexes: &[(usize, bool)],
    row_position: usize,
    start_bound: &WindowFrameBound,
    end_bound: &WindowFrameBound,
) -> Result<(usize, usize), String> {

    if order_indexes.is_empty() {
        return Ok((0, partition_row_indexes.len().saturating_sub(1)));
    }

    if order_indexes.len() != 1 {
        return Err("RANGE window frames currently require exactly one ORDER BY expression".to_string());
    }

    let (order_index, descending) = order_indexes[0];
    let current_row_index = partition_row_indexes[row_position];
    let current_value = rows
        .get(current_row_index)
        .and_then(|row| row.get(order_index))
        .ok_or_else(|| "window RANGE ORDER BY value is out of range".to_string())?;
    let current = transformed_range_order_value(current_value, descending)?;

    let start = range_bound_position(
        rows,
        partition_row_indexes,
        order_index,
        descending,
        start_bound,
        current,
        true,
    )?;

    let end = range_bound_position(
        rows,
        partition_row_indexes,
        order_index,
        descending,
        end_bound,
        current,
        false,
    )?;

    Ok((start, end))

}

fn range_bound_position(
    rows: &[Vec<Vec<u8>>],
    partition_row_indexes: &[usize],
    order_index: usize,
    descending: bool,
    bound: &WindowFrameBound,
    current: f64,
    is_start: bool,
) -> Result<usize, String> {

    let last_index = partition_row_indexes.len().saturating_sub(1);

    let search = |target: f64, use_lower_bound: bool| -> Result<usize, String> {
        if use_lower_bound {
            for (position, row_index) in partition_row_indexes.iter().enumerate() {
                let value = rows
                    .get(*row_index)
                    .and_then(|row| row.get(order_index))
                    .ok_or_else(|| "window RANGE ORDER BY value is out of range".to_string())?;
                if transformed_range_order_value(value, descending)? >= target {
                    return Ok(position);
                }
            }
            Ok(last_index)
        } else {
            for position in (0..partition_row_indexes.len()).rev() {
                let row_index = partition_row_indexes[position];
                let value = rows
                    .get(row_index)
                    .and_then(|row| row.get(order_index))
                    .ok_or_else(|| "window RANGE ORDER BY value is out of range".to_string())?;
                if transformed_range_order_value(value, descending)? <= target {
                    return Ok(position);
                }
            }
            Ok(0)
        }
    };

    match bound {

        WindowFrameBound::Preceding(None) => Ok(0),

        WindowFrameBound::Following(None) => Ok(last_index),

        WindowFrameBound::CurrentRow => {
            if is_start {
                search(current, true)
            } else {
                search(current, false)
            }
        },

        WindowFrameBound::Preceding(Some(expr)) => {
            let target = current - parse_window_frame_offset(expr)? as f64;
            if is_start {
                search(target, true)
            } else {
                search(target, false)
            }
        },

        WindowFrameBound::Following(Some(expr)) => {
            let target = current + parse_window_frame_offset(expr)? as f64;
            if is_start {
                search(target, true)
            } else {
                search(target, false)
            }
        }

    }

}

fn transformed_range_order_value(value: &[u8], descending: bool) -> Result<f64, String> {
    let numeric = parse_window_numeric_value(value)?.ok_or_else(|| {
        "RANGE window frames currently require numeric ORDER BY values".to_string()
    })?;

    Ok(if descending { -numeric } else { numeric })
}

fn parse_window_frame_offset(expr: &Expr) -> Result<usize, String> {

    match expr {
        Expr::Value(sqlparser::ast::Value::Number(value, _)) => value
            .parse::<usize>()
            .map_err(|_| "window frame offset must be an unsigned numeric literal".to_string()),
        _ => Err("window frame offset currently supports only unsigned numeric literals".to_string()),
    }

}