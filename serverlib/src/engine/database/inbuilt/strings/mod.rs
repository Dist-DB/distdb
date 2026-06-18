use sqlparser::ast::FunctionArg;

use super::indexer::{evaluate_argument_expression, function_argument_expr};

pub mod ascii;
pub mod char_length;
pub mod concat;
pub mod concat_w;
pub mod field;
pub mod find_in_set;
pub mod format;
pub mod insert;
pub mod instr;
pub mod left;
pub mod length;
pub mod locate;
pub mod lower;
pub mod lpad;
pub mod ltrim;
pub mod mid;
pub mod position;
pub mod repeat;
pub mod replace;
pub mod reverse;
pub mod right;
pub mod rpad;
pub mod rtrim;
pub mod space;
pub mod substr;
pub mod substring_index;
pub mod trim;
pub mod upper;

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

pub(super) fn evaluate_string_arg(
	args: &[FunctionArg],
	index: usize,
) -> Result<Option<String>, String> {
	Ok(evaluate_bytes_arg(args, index)?
		.map(|value| String::from_utf8_lossy(&value).into_owned()))
}

pub(super) fn evaluate_i64_arg(args: &[FunctionArg], index: usize) -> Result<Option<i64>, String> {
	let Some(value) = evaluate_string_arg(args, index)? else {
		return Ok(None);
	};

	value
		.trim()
		.parse::<i64>()
		.map(Some)
		.map_err(|_| format!("argument {} must be an integer", index + 1))
}

pub(super) fn evaluate_f64_arg(args: &[FunctionArg], index: usize) -> Result<Option<f64>, String> {
	let Some(value) = evaluate_string_arg(args, index)? else {
		return Ok(None);
	};

	value
		.trim()
		.parse::<f64>()
		.map(Some)
		.map_err(|_| format!("argument {} must be numeric", index + 1))
}

pub(super) fn string_result(value: impl Into<String>) -> Option<Vec<u8>> {
	Some(value.into().into_bytes())
}

pub(super) fn number_result<T: ToString>(value: T) -> Option<Vec<u8>> {
	Some(value.to_string().into_bytes())
}

pub(super) fn char_count(value: &str) -> usize {
	value.chars().count()
}

pub(super) fn left_chars(value: &str, count: i64) -> String {
	if count <= 0 {
		return String::new();
	}

	value.chars().take(count as usize).collect()
}

pub(super) fn right_chars(value: &str, count: i64) -> String {
	if count <= 0 {
		return String::new();
	}

	let chars = value.chars().collect::<Vec<_>>();
	let count = count as usize;
	if count >= chars.len() {
		return value.to_string();
	}

	chars[chars.len() - count..].iter().collect()
}

pub(super) fn substring_mysql(value: &str, position: i64, length: Option<i64>) -> String {
	if position == 0 {
		return String::new();
	}

	let chars = value.chars().collect::<Vec<_>>();
	let total = chars.len() as i64;

	let start = if position > 0 {
		position - 1
	} else {
		total + position
	};

	if start < 0 || start >= total {
		return String::new();
	}

	let end = match length {
		Some(length) if length <= 0 => return String::new(),
		Some(length) => (start + length).min(total),
		None => total,
	};

	chars[start as usize..end as usize].iter().collect()
}

pub(super) fn insert_mysql(value: &str, position: i64, length: i64, new_value: &str) -> String {
	if position <= 0 || length < 0 {
		return value.to_string();
	}

	let chars = value.chars().collect::<Vec<_>>();
	let total = chars.len() as i64;

	if position > total {
		return value.to_string();
	}

	let start = (position - 1) as usize;
	let end = (start as i64 + length).min(total) as usize;

	let prefix = chars[..start].iter().collect::<String>();
	let suffix = chars[end..].iter().collect::<String>();

	format!("{}{}{}", prefix, new_value, suffix)
}

pub(super) fn find_substring_position(value: &str, needle: &str, start_position: i64) -> usize {
	let total = char_count(value) as i64;
	if start_position <= 0 || start_position > total + 1 {
		return 0;
	}

	if needle.is_empty() {
		return start_position as usize;
	}

	let lowered_value = value.to_lowercase();
	let lowered_needle = needle.to_lowercase();
	let start_index = (start_position - 1) as usize;
	let start_byte = char_to_byte_index(&lowered_value, start_index).unwrap_or(lowered_value.len());

	let Some(relative_index) = lowered_value[start_byte..].find(&lowered_needle) else {
		return 0;
	};

	let absolute_index = start_byte + relative_index;
	lowered_value[..absolute_index].chars().count() + 1
}

pub(super) fn pad_mysql(value: &str, target_length: i64, pad: &str, left_pad: bool) -> Option<String> {
	if target_length < 0 {
		return None;
	}

	let target_length = target_length as usize;
	let current_length = char_count(value);

	if target_length <= current_length {
		return Some(left_chars(value, target_length as i64));
	}

	if pad.is_empty() {
		return None;
	}

	let missing = target_length - current_length;
	let filler = repeat_to_length(pad, missing);

	if left_pad {
		Some(format!("{}{}", filler, value))
	} else {
		Some(format!("{}{}", value, filler))
	}
}

pub(super) fn trim_spaces(value: &str, leading: bool, trailing: bool) -> String {
	trim_exact(value, " ", leading, trailing)
}

pub(super) fn trim_exact(value: &str, pattern: &str, leading: bool, trailing: bool) -> String {
	if pattern.is_empty() {
		return value.to_string();
	}

	let mut result = value.to_string();

	if leading {
		while result.starts_with(pattern) {
			result = result[pattern.len()..].to_string();
		}
	}

	if trailing {
		while result.ends_with(pattern) {
			let next_len = result.len() - pattern.len();
			result.truncate(next_len);
		}
	}

	result
}

pub(super) fn substring_index_mysql(value: &str, delimiter: &str, count: i64) -> String {
	if delimiter.is_empty() || count == 0 {
		return String::new();
	}

	let matches = value.match_indices(delimiter).map(|(index, _)| index).collect::<Vec<_>>();
	if matches.is_empty() {
		return value.to_string();
	}

	if count > 0 {
		let count = count as usize;
		if count > matches.len() {
			return value.to_string();
		}

		return value[..matches[count - 1]].to_string();
	}

	let count = (-count) as usize;
	if count > matches.len() {
		return value.to_string();
	}

	let start = matches[matches.len() - count] + delimiter.len();
	value[start..].to_string()
}

pub(super) fn format_mysql_number(value: f64, decimals: i64, locale: Option<&str>) -> String {
	let decimals = decimals.clamp(0, 30) as usize;
	let (thousands_separator, decimal_separator) = locale_separators(locale);

	let sign = if value.is_sign_negative() { "-" } else { "" };
	let rounded = format!("{:.*}", decimals, value.abs());
	let mut parts = rounded.split('.');
	let integer = parts.next().unwrap_or_default();
	let fraction = parts.next();

	let grouped_integer = group_integer(integer, thousands_separator);

	match fraction {
		Some(fraction) if decimals > 0 => {
			format!("{}{}{}{}", sign, grouped_integer, decimal_separator, fraction)
		}
		_ => format!("{}{}", sign, grouped_integer),
	}
}

fn repeat_to_length(pattern: &str, target_length: usize) -> String {
	let mut result = String::new();
	while result.chars().count() < target_length {
		result.push_str(pattern);
	}

	left_chars(&result, target_length as i64)
}

fn char_to_byte_index(value: &str, char_index: usize) -> Option<usize> {
	if char_index == value.chars().count() {
		return Some(value.len());
	}

	value.char_indices().nth(char_index).map(|(index, _)| index)
}

fn locale_separators(locale: Option<&str>) -> (char, char) {
	match locale.map(|value| value.trim().to_ascii_lowercase()) {
		Some(value)
			if matches!(
				value.as_str(),
				"de" | "de_de" | "de-at" | "es" | "es_es" | "fr" | "fr_fr" | "it" | "it_it"
			) =>
		{
			('.', ',')
		}
		_ => (',', '.'),
	}
}

fn group_integer(integer: &str, separator: char) -> String {
	let digits = integer.chars().collect::<Vec<_>>();
	let mut grouped = String::new();

	for (index, digit) in digits.iter().enumerate() {
		if index > 0 && (digits.len() - index) % 3 == 0 {
			grouped.push(separator);
		}
		grouped.push(*digit);
	}

	grouped
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
	fn string_function_registry_exposes_mysql_aliases() {
		for function_name in [
			"concat_ws",
			"mid",
			"position",
			"repeat",
			"replace",
			"reverse",
			"right",
		] {
			assert!(is_inbuilt_function(function_name));
		}
	}

	#[test]
	fn evaluate_string_functions_matches_mysql_like_samples() {
        
		let cases = [
			("ascii('A')", Some("65")),
			("char_length('Grüße')", Some("5")),
			("concat('sa', 'm')", Some("sam")),
			("concat_ws('-', 'sam', null, 'colak')", Some("sam-colak")),
			("field('b', 'a', 'b', 'c')", Some("2")),
			("find_in_set('b', 'a,b,c')", Some("2")),
			("format(12345.678, 2)", Some("12,345.68")),
			("insert('Quadratic', 3, 4, 'ZZ')", Some("QuZZtic")),
			("instr('Foobar', 'oba')", Some("3")),
			("left('Hello', 2)", Some("He")),
			("length('Grüße')", Some("7")),
			("locate('bar', 'Foobarbar')", Some("4")),
			("lower('GrÜße')", Some("grüße")),
			("lpad('hi', 5, 'xy')", Some("xyxhi")),
			("ltrim('  hi')", Some("hi")),
			("mid('Quadratic', 3, 4)", Some("adra")),
			("repeat('ab', 3)", Some("ababab")),
			("replace('abcabc', 'ab', 'x')", Some("xcxc")),
			("reverse('stressed')", Some("desserts")),
			("right('Hello', 2)", Some("lo")),
			("rpad('hi', 5, 'xy')", Some("hixyx")),
			("rtrim('hi  ')", Some("hi")),
			("space(3)", Some("   ")),
			("substr('Quadratic', -4, 3)", Some("ati")),
			("substring_index('www.mysql.com', '.', 2)", Some("www.mysql")),
			("upper('Grüße')", Some("GRÜSSE")),
		];

		for (expression, expected) in cases {
			assert_eq!(evaluate_expression(expression).as_deref(), expected, "{}", expression);
		}

	}

	#[test]
	fn string_functions_propagate_null_when_mysql_requires_it() {
		assert_eq!(evaluate_expression("concat('sam', null)"), None);
		assert_eq!(evaluate_expression("concat_ws(null, 'sam', 'colak')"), None);
		assert_eq!(evaluate_expression("find_in_set(null, 'a,b')"), None);
	}
}