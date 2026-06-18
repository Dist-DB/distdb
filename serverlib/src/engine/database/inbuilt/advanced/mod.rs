mod helpers;

pub mod bin;
pub mod binary;
pub mod case;
pub mod cast;
pub mod coalesce;
pub mod connection_id;
pub mod conv;
pub mod convert;
pub mod current_user;
pub mod database;
pub mod ifcommand;
pub mod ifnull;
pub mod isnull;
pub mod last_insert_id;
pub mod nullif;
pub mod session_user;
pub mod system_user;
pub mod user;
pub mod version;

#[cfg(test)]
mod tests {

	use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
	use sqlparser::dialect::MySqlDialect;
	use sqlparser::parser::Parser;

	use crate::engine::database::inbuilt::{
		evaluate_inbuilt_sql_function_with_context, is_inbuilt_function, InbuiltSqlRuntimeContext,
	};

	fn evaluate_expression(sql: &str, context: &InbuiltSqlRuntimeContext) -> Option<String> {
        
		let mut statements = Parser::parse_sql(&MySqlDialect {}, &format!("select {}", sql))
			.expect("expression should parse");

		let Statement::Query(query) = statements.remove(0) else {
			panic!("expected query statement");
		};

		let SetExpr::Select(select) = *query.body else {
			panic!("expected select body");
		};

		let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
			panic!("expected unnamed expression projection");
		};

		let Expr::Function(function) = expression else {
			panic!("expected function projection, got {:?}", expression);
		};

		evaluate_inbuilt_sql_function_with_context(function, context)
			.expect("function should evaluate")
			.map(|value| String::from_utf8(value).expect("result should be utf8"))

	}

	fn test_runtime_context() -> InbuiltSqlRuntimeContext {
		
        InbuiltSqlRuntimeContext {
			current_database: Some("main_db".to_string()),
			current_user: Some("alice@localhost".to_string()),
			session_user: Some("alice@localhost".to_string()),
			system_user: Some("system@localhost".to_string()),
			connection_id: Some(42),
			last_insert_id: Some(1234),
			version: Some("distdb-test".to_string()),
		}

	}

	#[test]
	fn advanced_function_registry_exposes_expected_functions() {

		for function_name in [
			"bin",
			"binary",
			"case",
			"cast",
			"coalesce",
			"connection_id",
			"conv",
			"convert",
			"current_user",
			"database",
			"if",
			"ifnull",
			"isnull",
			"last_insert_id",
			"nullif",
			"session_user",
			"system_user",
			"user",
			"version",
		] {
			assert!(is_inbuilt_function(function_name));
		}

	}

	#[test]
	fn advanced_functions_match_expected_outputs() {

		let context = test_runtime_context();

		let cases = [
			("bin(10)", Some("1010")),
			("binary('abc')", Some("abc")),
			("`case`(1, 'yes', 'no')", Some("yes")),
			("`cast`(12.9, 'signed')", Some("12.9")),
			("coalesce(null, null, 'x')", Some("x")),
			("connection_id()", Some("42")),
			("conv('A', 16, 10)", Some("10")),
			("`convert`('abc', 'binary')", Some("abc")),
			("current_user()", Some("alice@localhost")),
			("database()", Some("main_db")),
			("if(1, 't', 'f')", Some("t")),
			("ifnull(null, 'fallback')", Some("fallback")),
			("isnull(null)", Some("1")),
			("last_insert_id()", Some("1234")),
			("nullif('same', 'same')", None),
			("session_user()", Some("alice@localhost")),
			("system_user()", Some("system@localhost")),
			("user()", Some("alice@localhost")),
			("version()", Some("distdb-test")),
		];

		for (expression, expected) in cases {
			assert_eq!(
				evaluate_expression(expression, &context).as_deref(),
				expected,
				"{}",
				expression
			);
		}

	}

}