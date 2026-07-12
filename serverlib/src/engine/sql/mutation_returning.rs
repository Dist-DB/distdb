use sqlparser::ast::{Expr, SelectItem};

use super::{MutationReturningItem, MutationReturningPlan, SqlParseError};

pub(crate) fn parse_mutation_returning_plan(
    returning: Option<&[SelectItem]>,
    statement_name: &str,
    allowed_qualifiers: &[String],
) -> Result<Option<MutationReturningPlan>, SqlParseError> {

    let Some(items) = returning else {
        return Ok(None);
    };

    if items.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "{statement_name} RETURNING must contain at least one projection item"
        )));
    }

    let mut plan = Vec::with_capacity(items.len());

    for item in items {
        match item {
            SelectItem::Wildcard(_) => plan.push(MutationReturningItem::Wildcard),

            SelectItem::QualifiedWildcard(prefix, _) => {
                let qualifier = common::normalize_identifier!(&prefix.to_string());
                if !allowed_qualifier(allowed_qualifiers, &qualifier) {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "{statement_name} RETURNING wildcard qualifier '{qualifier}' does not reference the mutation target"
                    )));
                }
                plan.push(MutationReturningItem::Wildcard);
            },

            SelectItem::UnnamedExpr(expr) => {
                let field_name = parse_returning_column_reference(expr, statement_name, allowed_qualifiers)?;
                plan.push(MutationReturningItem::Column {
                    output_name: field_name.clone(),
                    field_name,
                });
            },

            SelectItem::ExprWithAlias { expr, alias } => {
                let field_name = parse_returning_column_reference(expr, statement_name, allowed_qualifiers)?;
                plan.push(MutationReturningItem::Column {
                    field_name,
                    output_name: common::normalize_identifier!(&alias.value),
                });
            },

        }
    }

    Ok(Some(plan))

}

fn parse_returning_column_reference(
    expr: &Expr,
    statement_name: &str,
    allowed_qualifiers: &[String],
) -> Result<String, SqlParseError> {

    match expr {

        Expr::Identifier(identifier) => Ok(common::normalize_identifier!(&identifier.value)),

        Expr::CompoundIdentifier(parts) => {

            if parts.len() != 2 {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "{statement_name} RETURNING expression '{expr}' is not supported"
                )));
            }

            let qualifier = common::normalize_identifier!(&parts[0].value);
            if !allowed_qualifier(allowed_qualifiers, &qualifier) {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "{statement_name} RETURNING qualifier '{qualifier}' does not reference the mutation target"
                )));
            }

            Ok(common::normalize_identifier!(&parts[1].value))

        },

        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "{statement_name} RETURNING expression '{expr}' is not supported"
        ))),

    }

}

fn allowed_qualifier(allowed_qualifiers: &[String], qualifier: &str) -> bool {
    allowed_qualifiers.iter().any(|allowed| allowed == qualifier)
}
