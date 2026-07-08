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
#[path = "mod_test.rs"]
mod tests;