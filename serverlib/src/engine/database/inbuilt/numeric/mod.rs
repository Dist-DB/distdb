use std::time::{SystemTime, UNIX_EPOCH};

use sqlparser::ast::FunctionArg;

use super::indexer::{evaluate_argument_expression, function_argument_expr};

pub mod abs;
pub mod acos;
pub mod asin;
pub mod atan;
pub mod atan2;
pub mod avg;
pub mod ceil;
pub mod cos;
pub mod cot;
pub mod count;
pub mod degrees;
pub mod div;
pub mod exp;
pub mod floor;
pub mod greatest;
pub mod least;
pub mod ln;
pub mod log;
pub mod log10;
pub mod log2;
pub mod max;
pub mod min;
pub mod modulo;
pub mod pi;
pub mod pow;
pub mod radians;
pub mod rand;
pub mod round;
pub mod sign;
pub mod sin;
pub mod sqrt;
pub mod sum;
pub mod tan;
pub mod truncate;

pub(super) fn expect_arg_count(
	args: &[FunctionArg],
	min: usize,
	max: usize,
	function_name: &str,
) -> Result<(), String> {
	if args.len() < min || args.len() > max {
		if min == max {
			return Err(format!("{} requires {} argument(s)", function_name, min));
		}
		return Err(format!(
			"{} requires between {} and {} arguments",
			function_name, min, max
		));
	}

	Ok(())
}

pub(super) fn evaluate_bytes_arg(
	args: &[FunctionArg],
	index: usize,
) -> Result<Option<Vec<u8>>, String> {
	let expr = function_argument_expr(&args[index])?;
	evaluate_argument_expression(expr)
}

pub(super) fn evaluate_f64_arg(args: &[FunctionArg], index: usize) -> Result<Option<f64>, String> {
	let Some(value) = evaluate_bytes_arg(args, index)? else {
		return Ok(None);
	};

	let text = String::from_utf8_lossy(&value);
	text
		.trim()
		.parse::<f64>()
		.map(Some)
		.map_err(|_| format!("argument {} must be numeric", index + 1))
}

pub(super) fn evaluate_i64_arg(args: &[FunctionArg], index: usize) -> Result<Option<i64>, String> {
	let Some(value) = evaluate_f64_arg(args, index)? else {
		return Ok(None);
	};

	Ok(Some(value.trunc() as i64))
}

pub(super) fn collect_numeric_args(
	args: &[FunctionArg],
) -> Result<Vec<Option<f64>>, String> {
	let mut values = Vec::with_capacity(args.len());
	for index in 0..args.len() {
		values.push(evaluate_f64_arg(args, index)?);
	}
	Ok(values)
}

pub(super) fn number_result<T: ToString>(value: T) -> Option<Vec<u8>> {
	Some(value.to_string().into_bytes())
}

pub(super) fn float_result(value: f64) -> Option<Vec<u8>> {
	if !value.is_finite() {
		return None;
	}

	Some(normalize_float(value).into_bytes())
}

pub(super) fn normalize_float(value: f64) -> String {
	if value == 0.0 {
		return "0".to_string();
	}

	let mut text = value.to_string();
	if text.contains('.') && !text.contains('e') && !text.contains('E') {
		while text.ends_with('0') {
			text.pop();
		}
		if text.ends_with('.') {
			text.pop();
		}
	}

	text
}

pub(super) fn round_mysql(value: f64, decimals: i64) -> f64 {
	if decimals >= 0 {
		let factor = 10_f64.powi(decimals as i32);
		(value * factor).round() / factor
	} else {
		let factor = 10_f64.powi((-decimals) as i32);
		(value / factor).round() * factor
	}
}

pub(super) fn truncate_mysql(value: f64, decimals: i64) -> f64 {
	if decimals >= 0 {
		let factor = 10_f64.powi(decimals as i32);
		(value * factor).trunc() / factor
	} else {
		let factor = 10_f64.powi((-decimals) as i32);
		(value / factor).trunc() * factor
	}
}

pub(super) fn seeded_random(seed: i64) -> f64 {
	let mut state = (seed as u64) ^ 0x9E37_79B9_7F4A_7C15;
	state = state
		.wrapping_mul(6_364_136_223_846_793_005)
		.wrapping_add(1_442_695_040_888_963_407);
	((state >> 11) as f64) / ((1_u64 << 53) as f64)
}

pub(super) fn random_seed_now() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|duration| duration.as_nanos() as i64)
		.unwrap_or(0)
}

#[cfg(test)]
mod tests {

	use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
	use sqlparser::dialect::MySqlDialect;
	use sqlparser::parser::Parser;

	use crate::engine::database::inbuilt::{evaluate_inbuilt_sql_function, is_inbuilt_function};

	fn evaluate_expression(sql: &str) -> Option<String> {

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

		evaluate_inbuilt_sql_function(function)
			.expect("function should evaluate")
			.map(|value| String::from_utf8(value).expect("result should be utf8"))
            
	}

	#[test]
	fn numeric_function_registry_exposes_expected_functions() {

		for function_name in [
			"abs",
			"atan2",
			"ceil",
			"div",
			"greatest",
			"least",
			"log2",
			"pow",
			"rand",
			"truncate",
		] {
			assert!(is_inbuilt_function(function_name));
		}

	}

	#[test]
	fn evaluate_numeric_functions_matches_mysql_like_samples() {
		let cases = [
			("abs(-3.5)", Some("3.5")),
			("acos(1)", Some("0")),
			("asin(1)", Some("1.5707963267948966")),
			("atan(1)", Some("0.7853981633974483")),
			("atan2(0, -1)", Some("3.141592653589793")),
			("avg(5)", Some("5")),
			("cos(0)", Some("1")),
			("round(cot(1), 12)", Some("0.642092615934")),
			("count(1, null, 2)", Some("2")),
			("round(degrees(pi()), 0)", Some("180")),
			("div(5, 2)", Some("2")),
			("round(exp(1), 12)", Some("2.718281828459")),
			("greatest(1, 3, 2)", Some("3")),
			("least(1, 3, 2)", Some("1")),
			("round(ln(exp(1)), 0)", Some("1")),
			("round(log(10, 100), 0)", Some("2")),
			("log10(1000)", Some("3")),
			("log2(8)", Some("3")),
			("max(1, null, 3)", Some("3")),
			("min(1, null, 3)", Some("1")),
			("mod(10, 3)", Some("1")),
			("pi()", Some("3.141592653589793")),
			("pow(2, 3)", Some("8")),
			("round(radians(180), 12)", Some("3.14159265359")),
			("round(1234.56, -2)", Some("1200")),
			("sign(-12)", Some("-1")),
			("round(sin(pi() / 2), 0)", Some("1")),
			("sqrt(9)", Some("3")),
			("sum(1, null, 2)", Some("3")),
			("tan(0)", Some("0")),
			("truncate(123.456, 2)", Some("123.45")),
		];

		for (expression, expected) in cases {
			assert_eq!(evaluate_expression(expression).as_deref(), expected, "{}", expression);
		}
	}

	#[test]
	fn numeric_functions_propagate_null_or_invalid_mysql_results() {
		assert_eq!(evaluate_expression("abs(null)"), None);
		assert_eq!(evaluate_expression("acos(2)"), None);
		assert_eq!(evaluate_expression("greatest(1, null)"), None);
		assert_eq!(evaluate_expression("mod(10, 0)"), None);
		assert_eq!(evaluate_expression("sqrt(-1)"), None);
	}

	#[test]
	fn rand_is_deterministic_with_seed_and_bounded_without_seed() {
		let seeded_once = evaluate_expression("rand(7)").expect("seeded rand should evaluate");
		let seeded_twice = evaluate_expression("rand(7)").expect("seeded rand should evaluate");
		assert_eq!(seeded_once, seeded_twice);

		let random = evaluate_expression("rand()").expect("rand should evaluate");
		let parsed = random.parse::<f64>().expect("rand output should be numeric");
		assert!((0.0..1.0).contains(&parsed));
	}

}