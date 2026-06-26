use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, SelectItem, SetExpr, Statement,
    TableFactor,
};

use super::literals::parse_default_value;
use super::{
    evaluate_sql_function, is_supported_sql_function, parse_mysql_statements,
    validate_regex_pattern, SelectComparisonOp, SelectCondition, SelectJoin, SelectJoinKind,
    SelectPredicate, SelectProjectionItem, SelectReadPlan, SelectRelation, SqlParseError,
};

type SelectRelationBinding = SelectRelation;

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

    let relation_bindings = parse_relation_bindings_from_table_with_joins(
        select.from.first(),
        &query.to_string(),
    )?;
    let joins = parse_joins_from_table_with_joins(
        select.from.first(),
        &query.to_string(),
        &relation_bindings,
    )?;

    let has_wildcard_projection = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)));
    let projection_is_wildcard = select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)));

    let (projection, projection_items) = if has_wildcard_projection {
        for item in &select.projection {
            if let SelectItem::QualifiedWildcard(prefix, _) = item {
                validate_qualified_prefix(prefix, &relation_bindings)?;
            }
        }

        (None, select
            .projection
            .iter()
            .map(|item| parse_select_projection_item(item, &relation_bindings))
            .collect::<Result<Vec<_>, _>>()?)
    } else {
        let mut fields = Vec::new();
        let mut items = Vec::new();

        for item in &select.projection {
            let projection_item = parse_select_projection_item(item, &relation_bindings)?;

            if let SelectProjectionItem::Column { field_name, .. } = &projection_item {
                fields.push(field_name.clone());
            }

            items.push(projection_item);
        }

        (Some(fields), items)
    };

    let table_id = match relation_bindings.first() {
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

    let where_condition = parse_select_condition_from_expr(
        select.selection.as_ref(),
        &relation_bindings,
    )?;
    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );

    Ok(SelectReadPlan {
        table_id,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        projection,
        projection_items,
        projection_is_wildcard,
        where_condition,
        is_explain,
    })

}

fn parse_select_projection_item(
    item: &SelectItem,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectProjectionItem, SqlParseError> {

    match item {

        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
            ensure_unqualified_allowed(relation_bindings, &ident.value, "SELECT projection")?;

            let field_name = common::normalize_identifier!(&ident.value);
            Ok(SelectProjectionItem::Column {
                field_name: field_name.clone(),
                output_name: field_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
            let field_name = parse_qualified_field_name(parts, relation_bindings, "SELECT projection")?;
            let output_name = common::normalize_identifier!(&parts.last().expect("parts should exist").value);

            Ok(SelectProjectionItem::Column {
                field_name,
                output_name,
            })
        },

        SelectItem::UnnamedExpr(Expr::Function(function)) => {
            let function_name = function.name.to_string();

            if !is_supported_sql_function(&function_name) {
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

        SelectItem::Wildcard(_) => Ok(SelectProjectionItem::Wildcard { relation: None }),

        SelectItem::QualifiedWildcard(prefix, _) => {
            let relation = validate_qualified_prefix(prefix, relation_bindings)?;
            Ok(SelectProjectionItem::Wildcard { relation: Some(relation) })
        },

        SelectItem::ExprWithAlias { expr, alias } => match expr {

            Expr::Identifier(ident) => {
                ensure_unqualified_allowed(relation_bindings, &ident.value, "SELECT projection")?;

                let field_name = common::normalize_identifier!(&ident.value);
                let output_name = common::normalize_identifier!(&alias.value);

                Ok(SelectProjectionItem::Column {
                    field_name,
                    output_name,
                })
            },

            Expr::CompoundIdentifier(parts) => {
                let field_name =
                    parse_qualified_field_name(parts, relation_bindings, "SELECT projection")?;

                Ok(SelectProjectionItem::Column {
                    field_name,
                    output_name: common::normalize_identifier!(&alias.value),
                })
            },

            Expr::Function(function) => {
                let function_name = function.name.to_string();

                if !is_supported_sql_function(&function_name) {
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

pub fn parse_select_condition_from_expr(
    selection: Option<&Expr>,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Option<SelectCondition>, SqlParseError> {

    let Some(selection) = selection else {
        return Ok(None);
    };

    Ok(Some(parse_select_condition_expression(selection, relation_bindings)?))
}

pub fn derive_relation_pushdown_conditions(
    condition: Option<&SelectCondition>,
    relation_bindings: &[SelectRelationBinding],
    joins: &[SelectJoin],
) -> Vec<Option<SelectCondition>> {

    if relation_bindings.is_empty() {
        return Vec::new();
    }

    let mut per_relation = vec![Vec::new(); relation_bindings.len()];

    let Some(condition) = condition else {
        return vec![None; relation_bindings.len()];
    };

    let clauses = flatten_and_clauses(condition);

    #[expect(clippy::single_match, reason="the function may be extended in the future to support more condition types that can be pushed down, but currently only one type is supported")]
    for clause in clauses {
        match relation_index_for_condition(clause, relation_bindings) {
            Some(index) => {
                if is_safe_relation_pushdown(index, joins)
                    && let Some(localized) = localize_condition_for_relation(clause)
                {
                    per_relation[index].push(localized);
                }
            }
            None => {}
        }
    }

    per_relation
        .into_iter()
        .map(combine_conditions)
        .collect::<Vec<_>>()

}

fn flatten_and_clauses(condition: &SelectCondition) -> Vec<&SelectCondition> {

    match condition {
        SelectCondition::And(children) => children
            .iter()
            .flat_map(flatten_and_clauses)
            .collect::<Vec<_>>(),
        _ => vec![condition],
    }
    
}

fn combine_conditions(conditions: Vec<SelectCondition>) -> Option<SelectCondition> {
    match conditions.len() {
        0 => None,
        1 => conditions.into_iter().next(),
        _ => Some(SelectCondition::And(conditions)),
    }
}

fn relation_index_for_condition(
    condition: &SelectCondition,
    relation_bindings: &[SelectRelationBinding],
) -> Option<usize> {
    let field_name = match condition {
        SelectCondition::Predicate(SelectPredicate::Comparison { field_name, .. })
        | SelectCondition::Predicate(SelectPredicate::Like { field_name, .. })
        | SelectCondition::Predicate(SelectPredicate::Regex { field_name, .. })
        | SelectCondition::Predicate(SelectPredicate::InList { field_name, .. })
        | SelectCondition::Predicate(SelectPredicate::IsNull { field_name, .. })
        | SelectCondition::Predicate(SelectPredicate::InSubquery { field_name, .. }) => field_name,
        SelectCondition::Predicate(SelectPredicate::FieldComparison { .. }) => return None,
        _ => return None,
    };

    let (qualifier, _) = field_name.split_once('.')?;

    relation_bindings.iter().position(|binding| {
        binding.alias.as_deref() == Some(qualifier) || binding.table_id == qualifier
    })
}

fn is_safe_relation_pushdown(relation_index: usize, joins: &[SelectJoin]) -> bool {
    if joins.is_empty() || relation_index == 0 {
        return true;
    }

    joins.iter().all(|join| matches!(join.kind, SelectJoinKind::Inner))
}

fn localize_condition_for_relation(condition: &SelectCondition) -> Option<SelectCondition> {
    match condition {
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op,
            value,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: unqualify_field_name(field_name)?,
            op: op.clone(),
            value: value.clone(),
        })),

        SelectCondition::Predicate(SelectPredicate::Like {
            field_name,
            pattern,
            negated,
            case_insensitive,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Like {
            field_name: unqualify_field_name(field_name)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
        })),

        SelectCondition::Predicate(SelectPredicate::Regex {
            field_name,
            pattern,
            negated,
            case_insensitive,
        }) => Some(SelectCondition::Predicate(SelectPredicate::Regex {
            field_name: unqualify_field_name(field_name)?,
            pattern: pattern.clone(),
            negated: *negated,
            case_insensitive: *case_insensitive,
        })),

        SelectCondition::Predicate(SelectPredicate::InList {
            field_name,
            values,
            negated,
        }) => Some(SelectCondition::Predicate(SelectPredicate::InList {
            field_name: unqualify_field_name(field_name)?,
            values: values.clone(),
            negated: *negated,
        })),

        SelectCondition::Predicate(SelectPredicate::IsNull { field_name, negated }) => {
            Some(SelectCondition::Predicate(SelectPredicate::IsNull {
                field_name: unqualify_field_name(field_name)?,
                negated: *negated,
            }))
        }

        SelectCondition::Predicate(SelectPredicate::InSubquery {
            field_name,
            subquery,
            negated,
        }) => Some(SelectCondition::Predicate(SelectPredicate::InSubquery {
            field_name: unqualify_field_name(field_name)?,
            subquery: subquery.clone(),
            negated: *negated,
        })),

        SelectCondition::Predicate(SelectPredicate::FieldComparison { .. }) => None,

        _ => None,
    
    }

}

fn unqualify_field_name(field_name: &str) -> Option<String> {
    field_name
        .split_once('.')
        .map(|(_, field_name)| field_name.to_string())
        .or_else(|| Some(field_name.to_string()))
}

fn parse_select_condition_expression(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {

    match expression {

        Expr::Nested(inner) => parse_select_condition_expression(inner, relation_bindings),

        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => parse_like_condition_expression(
            expr,
            pattern,
            *negated,
            false,
            escape_char.as_deref(),
            relation_bindings,
        ),

        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
        } => parse_like_condition_expression(
            expr,
            pattern,
            *negated,
            true,
            escape_char.as_deref(),
            relation_bindings,
        ),

        Expr::RLike {
            negated,
            expr,
            pattern,
            regexp: _,
        } => parse_regex_condition_expression(
            expr,
            pattern,
            *negated,
            relation_bindings,
        ),

        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(SelectCondition::And(vec![
                parse_select_condition_expression(left, relation_bindings)?,
                parse_select_condition_expression(right, relation_bindings)?,
            ])),
            BinaryOperator::Or => Ok(SelectCondition::Or(vec![
                parse_select_condition_expression(left, relation_bindings)?,
                parse_select_condition_expression(right, relation_bindings)?,
            ])),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let field_name = parse_condition_column_name(left, relation_bindings)?;
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
            parse_select_condition_expression(expr, relation_bindings)?,
        ))),

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
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
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
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
            field_name: parse_condition_column_name(expr, relation_bindings)?,
            negated: false,
        })),

        Expr::IsNotNull(expr) => Ok(SelectCondition::Predicate(SelectPredicate::IsNull {
            field_name: parse_condition_column_name(expr, relation_bindings)?,
            negated: true,
        })),

        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let field_name = parse_condition_column_name(expr, relation_bindings)?;
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
    relation_bindings: &[SelectRelationBinding],
) -> Result<String, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    let field_name = match expression {

        Expr::Identifier(ident) => {
            ensure_unqualified_allowed(relation_bindings, &ident.value, "WHERE clause")?;
            common::normalize_identifier!(&ident.value)
        },

        Expr::CompoundIdentifier(parts) => {
            parse_qualified_field_name(parts, relation_bindings, "WHERE clause")?
        },

        _ => {
            return Err(SqlParseError::UnsupportedStatement(
                "WHERE currently supports only direct column predicates".to_string(),
            ));
        },

    };

    Ok(field_name)

}

fn parse_like_condition_expression(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    escape_char: Option<&str>,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {
    
    if escape_char.is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "LIKE ESCAPE is not supported yet".to_string(),
        ));
    }

    let field_name = parse_condition_column_name(expr, relation_bindings)?;
    let pattern = parse_condition_literal_value(pattern)?;

    Ok(SelectCondition::Predicate(SelectPredicate::Like {
        field_name,
        pattern,
        negated,
        case_insensitive,
    }))

}

fn parse_regex_condition_expression(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    relation_bindings: &[SelectRelationBinding],
) -> Result<SelectCondition, SqlParseError> {

    let field_name = parse_condition_column_name(expr, relation_bindings)?;
    let pattern = parse_condition_literal_value(pattern)?;

    validate_regex_pattern(&pattern).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("REGEXP pattern is invalid: {err}"))
    })?;

    Ok(SelectCondition::Predicate(SelectPredicate::Regex {
        field_name,
        pattern,
        negated,
        case_insensitive: false,
    }))

}

pub fn parse_relation_bindings_from_table_with_joins(
    from: Option<&sqlparser::ast::TableWithJoins>,
    statement: &str,
) -> Result<Vec<SelectRelationBinding>, SqlParseError> {

    let Some(table_with_joins) = from else {
        return Ok(Vec::new());
    };

    let mut relations = Vec::with_capacity(1 + table_with_joins.joins.len());
    relations.push(parse_relation_binding_from_factor(
        &table_with_joins.relation,
        statement,
    )?);

    for join in &table_with_joins.joins {
        relations.push(parse_relation_binding_from_factor(&join.relation, statement)?);
    }

    Ok(relations)

}

fn parse_relation_binding_from_factor(
    relation: &TableFactor,
    statement: &str,
) -> Result<SelectRelationBinding, SqlParseError> {

    let TableFactor::Table { name, alias, .. } = relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table SELECT is currently supported".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(&name.to_string());

    if table_id.is_empty() {
        return Err(SqlParseError::MissingIdentifier {
            keyword: "from",
            statement: statement.to_string(),
        });
    }

    Ok(SelectRelationBinding {
        table_id,
        alias: alias
            .as_ref()
            .map(|table_alias| common::normalize_identifier!(&table_alias.name.value)),
    })

}

pub fn parse_joins_from_table_with_joins(
    from: Option<&sqlparser::ast::TableWithJoins>,
    statement: &str,
    relation_bindings: &[SelectRelationBinding],
) -> Result<Vec<SelectJoin>, SqlParseError> {

    let Some(table_with_joins) = from else {
        return Ok(Vec::new());
    };

    let mut available = Vec::with_capacity(relation_bindings.len());
    if let Some(primary) = relation_bindings.first() {
        available.push(primary.clone());
    }

    let mut joins = Vec::with_capacity(table_with_joins.joins.len());

    for (idx, join) in table_with_joins.joins.iter().enumerate() {
        
        let relation = relation_bindings
            .get(idx + 1)
            .cloned()
            .ok_or_else(|| SqlParseError::UnsupportedStatement(statement.to_string()))?;

        let (kind, on) = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => (SelectJoinKind::Inner, expr),
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => (SelectJoinKind::Left, expr),
            JoinOperator::RightOuter(JoinConstraint::On(expr)) => (SelectJoinKind::Right, expr),
            JoinOperator::FullOuter(JoinConstraint::On(expr)) => (SelectJoinKind::Full, expr),
            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    "only INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL JOIN with ... ON left.col = right.col are currently supported"
                        .to_string(),
                ));
            }
        };

        let on_condition = parse_join_on_expression(on, &available, &relation)?;

        joins.push(SelectJoin {
            kind,
            relation: relation.clone(),
            on_condition,
        });

        available.push(relation);
    
    }

    Ok(joins)

}

fn parse_join_on_expression(
    expression: &Expr,
    left_relations: &[SelectRelationBinding],
    right_relation: &SelectRelationBinding,
) -> Result<SelectCondition, SqlParseError> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = unwrap_nested_expression(expression)
    else {
        return Err(SqlParseError::UnsupportedStatement(
            "JOIN ON currently supports only equality between qualified columns".to_string(),
        ));
    };

    let left_field_name = parse_join_field_name(left, left_relations, "JOIN left operand")?;
    let right_field_name = parse_join_field_name(
        right,
        std::slice::from_ref(right_relation),
        "JOIN right operand",
    )?;

    Ok(SelectCondition::Predicate(SelectPredicate::FieldComparison {
        left_field_name,
        op: SelectComparisonOp::Eq,
        right_field_name,
    }))
}

fn parse_join_field_name(
    expression: &Expr,
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {
    let expression = unwrap_nested_expression(expression);

    let Expr::CompoundIdentifier(parts) = expression else {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{} must be a qualified column reference",
            location,
        )));
    };

    parse_join_qualified_field_name(parts, relation_bindings, location)
}

fn parse_qualified_field_name(
    parts: &[sqlparser::ast::Ident],
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {
    if parts.len() == 1 {
        ensure_unqualified_allowed(relation_bindings, &parts[0].value, location)?;
        return Ok(common::normalize_identifier!(&parts[0].value));
    }

    if parts.len() != 2 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "invalid compound identifier in {location}"
        )));
    }

    if relation_bindings.len() == 1 {
        validate_qualifier(&parts[0].value, relation_bindings, location)?;
        return Ok(common::normalize_identifier!(&parts[1].value));
    }

    parse_join_qualified_field_name(parts, relation_bindings, location)
}

fn parse_join_qualified_field_name(
    parts: &[sqlparser::ast::Ident],
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {
    if parts.len() != 2 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "invalid compound identifier in {location}"
        )));
    }

    let qualifier = validate_qualifier(&parts[0].value, relation_bindings, location)?;
    Ok(format!("{}.{}", qualifier, common::normalize_identifier!(&parts[1].value)))
}

fn validate_qualified_prefix(
    prefix: &sqlparser::ast::ObjectName,
    relation_bindings: &[SelectRelationBinding],
) -> Result<String, SqlParseError> {
    let parts = &prefix.0;

    if parts.len() != 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "qualified wildcard must use exactly one qualifier".to_string(),
        ));
    }

    validate_qualifier(&parts[0].value, relation_bindings, "SELECT wildcard")
}

fn validate_qualifier(
    qualifier: &str,
    relation_bindings: &[SelectRelationBinding],
    location: &str,
) -> Result<String, SqlParseError> {
    if relation_bindings.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "qualified reference '{}' in {} requires FROM relation",
            qualifier, location
        )));
    }

    let normalized = common::normalize_identifier!(qualifier);

    for binding in relation_bindings {
        let matches_table = normalized == binding.table_id;
        let matches_alias = binding
            .alias
            .as_ref()
            .is_some_and(|alias| normalized == *alias);

        if matches_table || matches_alias {
            return Ok(relation_lookup_key(binding));
        }
    }

    Err(SqlParseError::UnsupportedStatement(format!(
        "unknown relation qualifier '{}' in {}",
        qualifier, location
    )))
}

fn relation_lookup_key(binding: &SelectRelationBinding) -> String {
    binding.alias.clone().unwrap_or_else(|| binding.table_id.clone())
}

fn ensure_unqualified_allowed(
    relation_bindings: &[SelectRelationBinding],
    field_name: &str,
    location: &str,
) -> Result<(), SqlParseError> {
    if relation_bindings.len() > 1 {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{} requires qualified column '{}' when JOIN is present",
            location, field_name
        )));
    }

    Ok(())
}

fn parse_condition_literal_value(expression: &Expr) -> Result<Vec<u8>, SqlParseError> {

    let expression = unwrap_nested_expression(expression);

    if let Expr::Function(function) = expression {
        let value = evaluate_sql_function(function)?;

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
#[path = "select_plan_test.rs"]
mod tests;
