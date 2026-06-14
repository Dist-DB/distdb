use sqlparser::ast::{Delete, FromTable, Statement, TableFactor};

use super::{
    derive_relation_pushdown_conditions, parse_joins_from_table_with_joins,
    parse_mysql_statements, parse_relation_bindings_from_table_with_joins,
    parse_select_condition_from_expr, DeleteRowsPlan, SqlParseError,
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
    let relation_bindings = parse_relation_bindings_from_table_with_joins(Some(table), statement)?;
    let joins = parse_joins_from_table_with_joins(Some(table), statement, &relation_bindings)?;

    let where_condition = parse_select_condition_from_expr(delete.selection.as_ref(), &relation_bindings)?;
    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );

    Ok(DeleteRowsPlan {
        table_id,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        where_condition,
    })

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


#[cfg(test)]
#[path = "delete_plan_test.rs"]
mod tests;
