use sqlparser::ast::{
    Delete, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, Statement,
    TableFactor, TableWithJoins,
};

use super::{
    derive_relation_pushdown_conditions, parse_joins_from_table_with_joins,
    parse_mysql_statements, parse_relation_bindings_from_table_with_joins,
    ORDER_EXPR_ABS_PREFIX, ORDER_EXPR_CEIL_PREFIX, ORDER_EXPR_FLOOR_PREFIX,
    ORDER_EXPR_LENGTH_PREFIX, ORDER_EXPR_LOWER_PREFIX, ORDER_EXPR_LTRIM_PREFIX,
    ORDER_EXPR_REVERSE_PREFIX, ORDER_EXPR_ROUND_PREFIX, ORDER_EXPR_ROUND_SCALE_PREFIX,
    ORDER_EXPR_RTRIM_PREFIX, ORDER_EXPR_TRIM_PREFIX, ORDER_EXPR_UPPER_PREFIX,
    parse_select_condition_from_expr, DeleteRowsPlan, SelectCondition, SelectJoin,
    SelectJoinKind, SelectOrderByItem, SqlParseError,
};

pub fn parse_delete_rows_from_statement(statement: &str) -> Result<DeleteRowsPlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Delete(delete) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not DELETE".to_string(),
        ));
    };

    let table_id = parse_delete_table_id(delete)?;
    let table_with_joins = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };

    let table = &table_with_joins[0];
    let mut returning_qualifiers = vec![table_id.clone()];
    if let TableFactor::Table { alias, .. } = &table.relation
        && let Some(alias) = alias.as_ref()
    {
        returning_qualifiers.push(common::normalize_identifier!(&alias.name.value));
    }

    let returning = super::mutation_returning::parse_mutation_returning_plan(
        delete.returning.as_deref(),
        "DELETE",
        &returning_qualifiers,
    )?;

    let (relation_bindings, joins) = build_delete_relation_graph(
        table,
        delete.using.as_deref(),
        &table_id,
        statement,
    )?;

    let where_condition = parse_select_condition_from_expr(delete.selection.as_ref(), &relation_bindings)?;
    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );
    let order_by = parse_delete_order_by_items(delete, &table_id)?;
    let limit = parse_delete_limit(delete.limit.as_ref())?;

    Ok(DeleteRowsPlan {
        table_id,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        order_by,
        limit,
        where_condition,
        returning,
    })

}

fn build_delete_relation_graph(
    from_table: &TableWithJoins,
    using_tables: Option<&[TableWithJoins]>,
    target_table_id: &str,
    statement: &str,
) -> Result<(Vec<super::SelectRelation>, Vec<SelectJoin>), SqlParseError> {

    let mut relation_bindings = parse_relation_bindings_from_table_with_joins(Some(from_table), statement)?;
    let mut joins = parse_joins_from_table_with_joins(Some(from_table), statement, &relation_bindings)?;

    let Some(using_tables) = using_tables else {
        return Ok((relation_bindings, joins));
    };

    for using_table in using_tables {

        let using_relations = parse_relation_bindings_from_table_with_joins(Some(using_table), statement)?;
        if using_relations.is_empty() {
            continue;
        }

        let using_joins = parse_joins_from_table_with_joins(Some(using_table), statement, &using_relations)?;

        let using_primary = &using_relations[0];

        if using_primary.table_id == target_table_id && relation_bindings.len() == 1 && joins.is_empty() {
            relation_bindings = using_relations;
            joins = using_joins;
            continue;
        }

        joins.push(SelectJoin {
            kind: SelectJoinKind::Inner,
            relation: using_primary.clone(),
            on_condition: SelectCondition::And(Vec::new()),
        });

        joins.extend(using_joins);
        relation_bindings.extend(using_relations);

    }

    if relation_bindings
        .first()
        .is_none_or(|relation| relation.table_id != target_table_id)
    {
        return Err(SqlParseError::UnsupportedStatement(
            "DELETE USING must keep the mutation target as the first relation".to_string(),
        ));
    }

    Ok((relation_bindings, joins))

}

fn parse_delete_table_id(delete: &Delete) -> Result<String, SqlParseError> {

    let table_with_joins = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };

    if table_with_joins.len() != 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "DELETE currently supports exactly one table".to_string(),
        ));
    }

    let table = &table_with_joins[0];

    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table DELETE is currently supported".to_string(),
        ));
    };

    Ok(common::normalize_identifier!(&name.to_string()))

}

fn parse_delete_order_by_items(
    delete: &Delete,
    target_table_id: &str,
) -> Result<Vec<SelectOrderByItem>, SqlParseError> {

    if delete.order_by.is_empty() {
        return Ok(Vec::new());
    }

    let mut table_aliases = vec![target_table_id.to_string()];

    if let Some(first_table) = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables.first(),
    }
        && let TableFactor::Table { alias, .. } = &first_table.relation
        && let Some(alias) = alias.as_ref()
    {
        table_aliases.push(common::normalize_identifier!(&alias.name.value));
    }

    let mut items = Vec::with_capacity(delete.order_by.len());

    for expression in &delete.order_by {

        if expression.nulls_first.is_some() || expression.with_fill.is_some() {
            return Err(SqlParseError::UnsupportedStatement(
                "DELETE ORDER BY NULLS FIRST/LAST or WITH FILL is not supported yet".to_string(),
            ));
        }

        let field_name = parse_delete_order_expression(&expression.expr, &table_aliases)?;

        items.push(SelectOrderByItem {
            field_name,
            descending: expression.asc == Some(false),
        });
    }

    Ok(items)

}

fn parse_delete_order_expression(
    expression: &Expr,
    table_aliases: &[String],
) -> Result<String, SqlParseError> {
    
    match expression {

        Expr::Identifier(identifier) => Ok(common::normalize_identifier!(&identifier.value)),

        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = common::normalize_identifier!(&parts[0].value);
            if !table_aliases.iter().any(|candidate| candidate == &qualifier) {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY currently supports only target-table column references"
                        .to_string(),
                ));
            }

            Ok(common::normalize_identifier!(&parts[1].value))
        },

        Expr::Function(function) => {

            let function_name = common::normalize_identifier!(&function.name.to_string());
            
            if function_name == "round" {

                let FunctionArguments::List(list) = &function.args else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "DELETE ORDER BY ROUND requires ROUND(column) or ROUND(column, scale)"
                            .to_string(),
                    ));
                };

                if list.args.len() == 1 {
                    let inner_column = match &list.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            parse_delete_order_expression(inner, table_aliases)?
                        }
                        _ => {
                            return Err(SqlParseError::UnsupportedStatement(
                                "DELETE ORDER BY ROUND requires a direct column reference"
                                    .to_string(),
                            ));
                        }
                    };
                    return Ok(format!("{ORDER_EXPR_ROUND_PREFIX}{inner_column}"));
                }

                if list.args.len() == 2 {

                    let inner_column = match &list.args[0] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            parse_delete_order_expression(inner, table_aliases)?
                        }
                        _ => {
                            return Err(SqlParseError::UnsupportedStatement(
                                "DELETE ORDER BY ROUND requires a direct column reference"
                                    .to_string(),
                            ));
                        }
                    };

                    let scale = match &list.args[1] {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(sqlparser::ast::Value::Number(value, _)))) => {
                            value.parse::<i32>().map_err(|_| {
                                SqlParseError::UnsupportedStatement(
                                    "DELETE ORDER BY ROUND scale must be an integer literal"
                                        .to_string(),
                                )
                            })?
                        }
                        _ => {
                            return Err(SqlParseError::UnsupportedStatement(
                                "DELETE ORDER BY ROUND scale must be an integer literal"
                                    .to_string(),
                            ));
                        }
                    };

                    return Ok(format!("{ORDER_EXPR_ROUND_SCALE_PREFIX}{scale}:{inner_column}"));

                }

                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY ROUND requires ROUND(column) or ROUND(column, scale)"
                        .to_string(),
                ));

            }

            let prefix = match function_name.as_str() {
                "lower" => ORDER_EXPR_LOWER_PREFIX,
                "upper" => ORDER_EXPR_UPPER_PREFIX,
                "abs" => ORDER_EXPR_ABS_PREFIX,
                "length" => ORDER_EXPR_LENGTH_PREFIX,
                "char_length" => ORDER_EXPR_LENGTH_PREFIX,
                "len" => ORDER_EXPR_LENGTH_PREFIX,
                "lcase" => ORDER_EXPR_LOWER_PREFIX,
                "ucase" => ORDER_EXPR_UPPER_PREFIX,
                "reverse" => ORDER_EXPR_REVERSE_PREFIX,
                "trim" => ORDER_EXPR_TRIM_PREFIX,
                "ltrim" => ORDER_EXPR_LTRIM_PREFIX,
                "rtrim" => ORDER_EXPR_RTRIM_PREFIX,
                "ceil" => ORDER_EXPR_CEIL_PREFIX,
                "ceiling" => ORDER_EXPR_CEIL_PREFIX,
                "floor" => ORDER_EXPR_FLOOR_PREFIX,
                _ => {
                    return Err(SqlParseError::UnsupportedStatement(
                        "DELETE ORDER BY currently supports direct columns or LOWER/UPPER/ABS/LENGTH/CHAR_LENGTH/LEN/LCASE/UCASE/REVERSE/TRIM/LTRIM/RTRIM/CEIL/CEILING/FLOOR/ROUND(column)"
                            .to_string(),
                    ));
                }
            };

            let FunctionArguments::List(list) = &function.args else {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY function expressions require a single column argument"
                        .to_string(),
                ));
            };

            if list.args.len() != 1 {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY function expressions require a single column argument"
                        .to_string(),
                ));
            }

            let inner_column = match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                    parse_delete_order_expression(inner, table_aliases)?
                }
                _ => {
                    return Err(SqlParseError::UnsupportedStatement(
                        "DELETE ORDER BY function expressions require a direct column reference"
                            .to_string(),
                    ));
                }
            };

            Ok(format!("{prefix}{inner_column}"))

        },

        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY TRIM currently supports only TRIM(column)"
                        .to_string(),
                ));
            }

            let inner_column = parse_delete_order_expression(expr, table_aliases)?;
            Ok(format!("{ORDER_EXPR_TRIM_PREFIX}{inner_column}"))
        },

        Expr::Ceil { expr, field } => {

            if !matches!(
                field,
                sqlparser::ast::CeilFloorKind::DateTimeField(
                    sqlparser::ast::DateTimeField::NoDateTime
                )
            ) {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY CEIL currently supports only CEIL(column)"
                        .to_string(),
                ));
            }

            let inner_column = parse_delete_order_expression(expr, table_aliases)?;
            
            Ok(format!("{ORDER_EXPR_CEIL_PREFIX}{inner_column}"))

        },

        Expr::Floor { expr, field } => {

            if !matches!(
                field,
                sqlparser::ast::CeilFloorKind::DateTimeField(
                    sqlparser::ast::DateTimeField::NoDateTime
                )
            ) {
                return Err(SqlParseError::UnsupportedStatement(
                    "DELETE ORDER BY FLOOR currently supports only FLOOR(column)"
                        .to_string(),
                ));
            }

            let inner_column = parse_delete_order_expression(expr, table_aliases)?;
            Ok(format!("{ORDER_EXPR_FLOOR_PREFIX}{inner_column}"))
            
        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "DELETE ORDER BY currently supports direct columns or LOWER/UPPER/ABS/LENGTH/CHAR_LENGTH/LEN/LCASE/UCASE/REVERSE/TRIM/LTRIM/RTRIM/CEIL/CEILING/FLOOR/ROUND(column)"
                .to_string(),
        )),

    }

}

fn parse_delete_limit(limit: Option<&Expr>) -> Result<Option<usize>, SqlParseError> {

    let Some(limit) = limit else {
        return Ok(None);
    };

    let Expr::Value(sqlparser::ast::Value::Number(value, _)) = unwrap_nested_delete_expression(limit) else {
        return Err(SqlParseError::UnsupportedStatement(
            "DELETE LIMIT currently supports only unsigned numeric literals".to_string(),
        ));
    };

    value.parse::<usize>().map(Some).map_err(|_| {
        SqlParseError::UnsupportedStatement(format!(
            "DELETE LIMIT value '{}' is out of range",
            value
        ))
    })

}

fn unwrap_nested_delete_expression(expression: &Expr) -> &Expr {
    match expression {
        Expr::Nested(inner) => unwrap_nested_delete_expression(inner),
        _ => expression,
    }
}


#[cfg(test)]
#[path = "delete_plan_test.rs"]
mod tests;
