use sqlparser::ast::{BinaryOperator, Expr, Query, SelectItem, SetExpr, Statement};

use crate::engine::database::inbuilt::{evaluate_inbuilt_sql_function, is_inbuilt_function};

use super::literals::parse_default_value;
use super::{
    parse_mysql_statements, SelectComparisonOp, SelectCondition, SelectPredicate,
    SelectProjectionItem, SelectReadPlan, SqlParseError,
};

#[derive(Clone)]
struct SelectRelationBinding {
    table_id: String,
    alias: Option<String>,
}

pub fn parse_select_projection_from_statement(
    statement: &str,
) -> Result<Option<Vec<String>>, SqlParseError> {
    parse_select_read_plan_from_statement(statement).map(|plan| plan.projection)
}

pub fn parse_select_read_plan_from_statement(
    statement: &str,
) -> Result<SelectReadPlan, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();
    let (inner_statement, is_explain) = if lowered.starts_with("explain ") {
        (trimmed["explain".len()..].trim(), true)
    } else {
        (trimmed, false)
    };

    let parsed = parse_mysql_statements(inner_statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Query(query) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not SELECT".to_string(),
        ));
    };

    parse_select_read_plan_from_query(query, is_explain)
}

fn parse_select_read_plan_from_query(
    query: &Query,
    is_explain: bool,
) -> Result<SelectReadPlan, SqlParseError> {

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlParseError::UnsupportedStatement(
            "only simple SELECT queries are currently supported for projection parsing".to_string(),
        ));
    };

    let projection_is_wildcard = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)))
    ;

    let relation_binding = parse_select_relation_binding(select.from.first(), query)?;

    let (projection, projection_items) = if projection_is_wildcard {
        for item in &select.projection {
            if let SelectItem::QualifiedWildcard(prefix, _) = item {
                validate_qualified_prefix(prefix, relation_binding.as_ref())?;
            }
        }
        (None, Vec::new())
    } else {
        let mut fields = Vec::new();
        let mut items = Vec::new();

        for item in &select.projection {

            let projection_item = parse_select_projection_item(item, relation_binding.as_ref())?;

            if let SelectProjectionItem::Column { field_name, .. } = &projection_item {
                fields.push(field_name.clone());
            }

            items.push(projection_item);
        }

        (Some(fields), items)
    };

    let table_id = match relation_binding.as_ref() {
        Some(binding) => binding.table_id.clone(),
        None => {
            if projection_is_wildcard {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "from",
                    statement: query.to_string(),
                });
            }

            if projection_items.iter().all(is_inbuilt_projection_item) {
                String::new()
            } else {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "from",
                    statement: query.to_string(),
                });
            }
        }
    };

    let where_condition = parse_select_condition(select.selection.as_ref(), relation_binding.as_ref())?;

    Ok(SelectReadPlan {
        table_id,
        projection,
        projection_items,
        projection_is_wildcard,
        where_condition,
        is_explain,
    })
    
}

    fn parse_select_projection_item(
        item: &SelectItem,
        relation_binding: Option<&SelectRelationBinding>,
    ) -> Result<SelectProjectionItem, SqlParseError> {

    match item {

        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
            let field_name = common::normalize_identifier!(&ident.value);
            Ok(SelectProjectionItem::Column {
                field_name: field_name.clone(),
                output_name: field_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            let field_name = parse_qualified_field_name(parts, relation_binding, "SELECT projection")?;

            let field_name = common::normalize_identifier!(&field_name);
            Ok(SelectProjectionItem::Column {
                field_name: field_name.clone(),
                output_name: field_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::Function(function)) => {
            
            let function_name = function.name.to_string();
            
            if !is_inbuilt_function(&function_name) {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "SELECT projection function '{}' is not supported",
                    function_name
                )));
            }

            Ok(SelectProjectionItem::InbuiltFunction {
                output_name: common::normalize_identifier!(&function_name),
                function: function.clone(),
            })

        },

        SelectItem::ExprWithAlias { expr, alias } => match expr {

            Expr::Identifier(ident) => {
                let field_name = common::normalize_identifier!(&ident.value);
                let output_name = common::normalize_identifier!(&alias.value);
                Ok(SelectProjectionItem::Column {
                    field_name,
                    output_name,
                })
            },

            Expr::CompoundIdentifier(parts) => {
                let field_name =
                    parse_qualified_field_name(parts, relation_binding, "SELECT projection")?;

                Ok(SelectProjectionItem::Column {
                    field_name: common::normalize_identifier!(&field_name),
                    output_name: common::normalize_identifier!(&alias.value),
                })
            },

            Expr::Function(function) => {
                let function_name = function.name.to_string();
                if !is_inbuilt_function(&function_name) {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "SELECT projection function '{}' is not supported",
                        function_name
                    )));
                }

                Ok(SelectProjectionItem::InbuiltFunction {
                    output_name: common::normalize_identifier!(&alias.value),
                    function: function.clone(),
                })
            },

            _ => Err(SqlParseError::UnsupportedStatement(
                "only direct column and inbuilt function projection is currently supported"
                    .to_string(),
            )),

        },

        _ => Err(SqlParseError::UnsupportedStatement(
            "only direct column and inbuilt function projection is currently supported"
                .to_string(),
        )),

    }

}

fn is_inbuilt_projection_item(item: &SelectProjectionItem) -> bool {
    matches!(item, SelectProjectionItem::InbuiltFunction { .. })
}

fn parse_select_condition(
    selection: Option<&Expr>,
    relation_binding: Option<&SelectRelationBinding>,
) -> Result<Option<SelectCondition>, SqlParseError> {
    let Some(selection) = selection else {
        return Ok(None);
    };

    Ok(Some(parse_select_condition_expression(selection, relation_binding)?))
}

fn parse_select_condition_expression(
    expression: &Expr,
    relation_binding: Option<&SelectRelationBinding>,
) -> Result<SelectCondition, SqlParseError> {

    match expression {

        Expr::Nested(inner) => parse_select_condition_expression(inner, relation_binding),

        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(SelectCondition::And(vec![
                parse_select_condition_expression(left, relation_binding)?,
                parse_select_condition_expression(right, relation_binding)?,
            ])),
            BinaryOperator::Or => Ok(SelectCondition::Or(vec![
                parse_select_condition_expression(left, relation_binding)?,
                parse_select_condition_expression(right, relation_binding)?,
            ])),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let field_name = parse_condition_column_name(left, relation_binding)?;
                let value = parse_condition_literal_value(right)?;
                let op = match op {
                    BinaryOperator::Eq => SelectComparisonOp::Eq,
                    BinaryOperator::NotEq => SelectComparisonOp::NotEq,
                    BinaryOperator::Gt => SelectComparisonOp::Gt,
                    BinaryOperator::GtEq => SelectComparisonOp::Gte,
                    BinaryOperator::Lt => SelectComparisonOp::Lt,
                    BinaryOperator::LtEq => SelectComparisonOp::Lte,
                    _ => unreachable!("comparison operator is already constrained"),
                };
                Ok(SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name,
                    op,
                    value,
                }))
            }
            _ => Err(SqlParseError::UnsupportedStatement(
                "WHERE operator is not supported yet".to_string(),
            )),
        },

        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(SelectCondition::Not(Box::new(
            parse_select_condition_expression(expr, relation_binding)?,
        ))),

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let field_name = parse_condition_column_name(expr, relation_binding)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                values.push(parse_condition_literal_value(item)?);
            }

            Ok(SelectCondition::Predicate(SelectPredicate::InList {
                field_name,
                values,
                negated: *negated,
            }))
        },

        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let field_name = parse_condition_column_name(expr, relation_binding)?;
            let subquery_plan = parse_select_read_plan_from_query(subquery.as_ref(), false)?;

            let Some(projection) = &subquery_plan.projection else {
                return Err(SqlParseError::UnsupportedStatement(
                    "WHERE subquery membership requires selecting exactly one explicit column"
                        .to_string(),
                ));
            };

            if projection.len() != 1 {
                return Err(SqlParseError::UnsupportedStatement(
                    "WHERE subquery membership requires selecting exactly one column".to_string(),
                ));
            }

            Ok(SelectCondition::Predicate(SelectPredicate::InSubquery {
                field_name,
                subquery: Box::new(subquery_plan),
                negated: *negated,
            }))
        }

        Expr::IsNull(expr) => Ok(SelectCondition::Predicate(SelectPredicate::IsNull {
            field_name: parse_condition_column_name(expr, relation_binding)?,
            negated: false,
        })),

        Expr::IsNotNull(expr) => Ok(SelectCondition::Predicate(SelectPredicate::IsNull {
            field_name: parse_condition_column_name(expr, relation_binding)?,
            negated: true,
        })),

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let field_name = parse_condition_column_name(expr, relation_binding)?;
            let low_value = parse_condition_literal_value(low)?;
            let high_value = parse_condition_literal_value(high)?;

            let between = SelectCondition::And(vec![
                SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: field_name.clone(),
                    op: SelectComparisonOp::Gte,
                    value: low_value,
                }),
                SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name,
                    op: SelectComparisonOp::Lte,
                    value: high_value,
                }),
            ]);

            if *negated {
                Ok(SelectCondition::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        }

        _ => Err(SqlParseError::UnsupportedStatement(
            "WHERE expression is not supported yet".to_string(),
        )),

    }

}

fn parse_condition_column_name(
    expression: &Expr,
    relation_binding: Option<&SelectRelationBinding>,
) -> Result<String, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let field_name = match expression {

        Expr::Identifier(ident) => ident.value.clone(),
        
        Expr::CompoundIdentifier(parts) => parse_qualified_field_name(parts, relation_binding, "WHERE clause")?,

        _ => {
            return Err(SqlParseError::UnsupportedStatement(
                "WHERE currently supports only direct column predicates".to_string(),
            ));
        }

    };

    Ok(common::normalize_identifier!(&field_name))

}

fn parse_select_relation_binding(
    from: Option<&sqlparser::ast::TableWithJoins>,
    query: &Query,
) -> Result<Option<SelectRelationBinding>, SqlParseError> {
    let Some(table_with_joins) = from else {
        return Ok(None);
    };

    if !table_with_joins.joins.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "JOIN is not supported yet".to_string(),
        ));
    }

    let sqlparser::ast::TableFactor::Table { name, alias, .. } = &table_with_joins.relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table SELECT is currently supported".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(&name.to_string());
    if table_id.is_empty() {
        return Err(SqlParseError::MissingIdentifier {
            keyword: "from",
            statement: query.to_string(),
        });
    }

    let alias = alias
        .as_ref()
        .map(|table_alias| common::normalize_identifier!(&table_alias.name.value));

    Ok(Some(SelectRelationBinding { table_id, alias }))
}

fn parse_qualified_field_name(
    parts: &[sqlparser::ast::Ident],
    relation_binding: Option<&SelectRelationBinding>,
    location: &str,
) -> Result<String, SqlParseError> {
    if parts.len() == 1 {
        return Ok(parts[0].value.clone());
    }

    if parts.len() != 2 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "invalid compound identifier in {location}"
        )));
    }

    validate_qualifier(&parts[0].value, relation_binding, location)?;
    Ok(parts[1].value.clone())
}

fn validate_qualified_prefix(
    prefix: &sqlparser::ast::ObjectName,
    relation_binding: Option<&SelectRelationBinding>,
) -> Result<(), SqlParseError> {
    let parts = &prefix.0;

    if parts.len() != 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "qualified wildcard must use exactly one qualifier".to_string(),
        ));
    }

    validate_qualifier(&parts[0].value, relation_binding, "SELECT wildcard")
}

fn validate_qualifier(
    qualifier: &str,
    relation_binding: Option<&SelectRelationBinding>,
    location: &str,
) -> Result<(), SqlParseError> {
    let Some(binding) = relation_binding else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "qualified reference '{}' in {} requires FROM relation",
            qualifier, location
        )));
    };

    let normalized = common::normalize_identifier!(qualifier);
    let matches_table = normalized == binding.table_id;
    let matches_alias = binding
        .alias
        .as_ref()
        .is_some_and(|alias| normalized == *alias);

    if matches_table || matches_alias {
        return Ok(());
    }

    Err(SqlParseError::UnsupportedStatement(format!(
        "unknown relation qualifier '{}' in {}",
        qualifier, location
    )))
}

fn parse_condition_literal_value(expression: &Expr) -> Result<Vec<u8>, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    if let Expr::Function(function) = expression {
        let value = evaluate_inbuilt_sql_function(function).map_err(|err| {
            SqlParseError::UnsupportedStatement(format!("WHERE inbuilt function failed: {err}"))
        })?;

        return value.ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "WHERE inbuilt function returned NULL; use IS NULL where appropriate".to_string(),
            )
        });
    }

    if !matches!(expression, Expr::Value(_)) {
        return Err(SqlParseError::UnsupportedStatement(
            "WHERE currently supports only literal values".to_string(),
        ));
    }

    let Some(value) = parse_default_value(expression.to_string()) else {
        return Err(SqlParseError::UnsupportedStatement(
            "WHERE comparison against NULL is not supported; use IS NULL".to_string(),
        ));
    };

    Ok(value)
    
}

fn unwrap_nested_expression(expression: &Expr) -> &Expr {
    let mut current = expression;

    while let Expr::Nested(inner) = current {
        current = inner;
    }

    current
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_projection_returns_requested_columns() {
        let projection =
            parse_select_projection_from_statement("SELECT uid, id_person FROM __account")
                .expect("projection should parse");

        assert_eq!(
            projection,
            Some(vec!["uid".to_string(), "id_person".to_string()])
        );
    }

    #[test]
    fn select_star_projection_returns_none() {
        let projection = parse_select_projection_from_statement("SELECT * FROM __account")
            .expect("projection should parse");

        assert_eq!(projection, None);
    }

    #[test]
    fn select_read_plan_parses_or_and_is_null() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where uid = '1' or id_device is null",
        )
        .expect("select read plan should parse");

        assert_eq!(plan.table_id, "__account");
        assert!(!plan.is_explain);
        assert!(matches!(plan.where_condition, Some(SelectCondition::Or(_))));
    }

    #[test]
    fn select_read_plan_parses_in_and_not_equal() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where role in ('user','admin') and is_deleted <> 1",
        )
        .expect("select read plan should parse");

        assert!(matches!(plan.where_condition, Some(SelectCondition::And(_))));
    }

    #[test]
    fn select_read_plan_parses_parenthesized_nested_groups() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where ((uid = '1' or (role = 'admin' and id_device is null)) and ((is_deleted <> 1)))",
        )
        .expect("select read plan with nested parentheses should parse");

        assert!(matches!(plan.where_condition, Some(SelectCondition::And(_))));
    }

    #[test]
    fn select_read_plan_parses_parenthesized_operands() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where ((uid)) = (('1'))",
        )
        .expect("select read plan with parenthesized operands should parse");

        assert!(matches!(
            plan.where_condition,
            Some(SelectCondition::Predicate(SelectPredicate::Comparison { .. }))
        ));
    }

    #[test]
    fn select_read_plan_parses_between_condition() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where date_created between 10 and 20",
        )
        .expect("between condition should parse");

        assert!(matches!(plan.where_condition, Some(SelectCondition::And(_))));
    }

    #[test]
    fn select_read_plan_parses_in_subquery_condition() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where id_person in (select uid from __person where is_deleted = 0)",
        )
        .expect("in-subquery condition should parse");

        assert!(matches!(
            plan.where_condition,
            Some(SelectCondition::Predicate(SelectPredicate::InSubquery { .. }))
        ));
    }

    #[test]
    fn select_read_plan_parses_inbuilt_function_literals() {
        let plan = parse_select_read_plan_from_statement(
            "select uid from __account where uid = CONCAT('user', '001')",
        )
        .expect("where inbuilt function literal should parse");

        assert!(matches!(
            plan.where_condition,
            Some(SelectCondition::Predicate(SelectPredicate::Comparison { .. }))
        ));
    }

    #[test]
    fn select_read_plan_parses_inbuilt_function_projection_with_from() {
        let plan = parse_select_read_plan_from_statement(
            "select unixtimestamp() as time from __account",
        )
        .expect("function projection with from should parse");

        assert_eq!(plan.table_id, "__account");
        assert_eq!(plan.projection_items.len(), 1);
        assert!(matches!(
            plan.projection_items[0],
            SelectProjectionItem::InbuiltFunction { .. }
        ));
    }

    #[test]
    fn select_read_plan_parses_inbuilt_function_projection_without_from() {
        let plan = parse_select_read_plan_from_statement("select unixtimestamp() as time")
            .expect("function projection without from should parse");

        assert!(plan.table_id.is_empty());
        assert_eq!(plan.projection_items.len(), 1);
        assert!(matches!(
            plan.projection_items[0],
            SelectProjectionItem::InbuiltFunction { .. }
        ));
    }

    #[test]
    fn select_alias_qualified_projection_is_valid() {
        let plan = parse_select_read_plan_from_statement(
            "select ac.uid from __account as ac",
        )
        .expect("alias-qualified projection should parse");

        assert_eq!(plan.projection, Some(vec!["uid".to_string()]));
    }

    #[test]
    fn select_unknown_alias_in_projection_is_rejected() {
        let err = parse_select_read_plan_from_statement(
            "select zz.uid from __account as ac",
        )
        .expect_err("unknown alias should fail parsing");

        assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn select_unknown_alias_in_where_is_rejected() {
        let err = parse_select_read_plan_from_statement(
            "select uid from __account as ac where zz.uid = '1'",
        )
        .expect_err("unknown alias in where should fail parsing");

        assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn select_alias_qualified_wildcard_is_valid() {
        let plan = parse_select_read_plan_from_statement("select ac.* from __account as ac")
            .expect("alias-qualified wildcard should parse");

        assert!(plan.projection_is_wildcard);
    }

    #[test]
    fn select_unknown_alias_qualified_wildcard_is_rejected() {
        let err = parse_select_read_plan_from_statement("select zz.* from __account as ac")
            .expect_err("unknown alias-qualified wildcard should fail parsing");

        assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
    }
}
