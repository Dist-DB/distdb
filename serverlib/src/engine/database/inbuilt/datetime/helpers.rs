use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

use crate::engine::database::inbuilt::indexer::{evaluate_argument_expression, function_argument_expr};

use sqlparser::ast::FunctionArg;

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

pub(super) fn expect_zero_args(function_name: &str, args: &[FunctionArg]) -> Result<(), String> {
	expect_arg_count(args, 0, 0, function_name)
}

pub(super) fn evaluate_bytes_arg(args: &[FunctionArg], index: usize) -> Result<Option<Vec<u8>>, String> {
	let expr = function_argument_expr(&args[index])?;
	evaluate_argument_expression(expr)
}

pub(super) fn evaluate_string_arg(args: &[FunctionArg], index: usize) -> Result<Option<String>, String> {
	Ok(evaluate_bytes_arg(args, index)?
		.map(|value| String::from_utf8_lossy(&value).into_owned()))
}

pub(super) fn evaluate_i64_arg(args: &[FunctionArg], index: usize) -> Result<Option<i64>, String> {
	let Some(value) = evaluate_string_arg(args, index)? else {
		return Ok(None);
	};

	value.trim().parse::<i64>().map(Some).map_err(|_| {
		format!("argument {} must be an integer", index + 1)
	})
}

pub(super) fn evaluate_f64_arg(args: &[FunctionArg], index: usize) -> Result<Option<f64>, String> {
	let Some(value) = evaluate_string_arg(args, index)? else {
		return Ok(None);
	};

	value.trim().parse::<f64>().map(Some).map_err(|_| {
		format!("argument {} must be numeric", index + 1)
	})
}

pub(super) fn number_result<T: ToString>(value: T) -> Option<Vec<u8>> {
	Some(value.to_string().into_bytes())
}

pub(super) fn string_result(value: impl Into<String>) -> Option<Vec<u8>> {
	Some(value.into().into_bytes())
}

pub(super) fn utc_now_date_string() -> String {
	Utc::now().date_naive().format("%Y-%m-%d").to_string()
}

pub(super) fn utc_now_time_string() -> String {
	Utc::now().time().format("%H:%M:%S").to_string()
}

pub(super) fn utc_now_datetime_string() -> String {
	Utc::now().naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn parse_date(value: &str) -> Option<NaiveDate> {
	let trimmed = value.trim();
	[
		"%Y-%m-%d",
		"%Y/%m/%d",
		"%Y%m%d",
	]
	.into_iter()
	.find_map(|format| NaiveDate::parse_from_str(trimmed, format).ok())
}

pub(super) fn parse_time(value: &str) -> Option<NaiveTime> {
	let trimmed = value.trim();
	[
		"%H:%M:%S%.f",
		"%H:%M:%S",
		"%H:%M",
	]
	.into_iter()
	.find_map(|format| NaiveTime::parse_from_str(trimmed, format).ok())
}

pub(super) fn parse_datetime(value: &str) -> Option<NaiveDateTime> {
	let trimmed = value.trim();
	[
		"%Y-%m-%d %H:%M:%S%.f",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S%.f",
		"%Y-%m-%dT%H:%M:%S",
	]
	.into_iter()
	.find_map(|format| NaiveDateTime::parse_from_str(trimmed, format).ok())
	.or_else(|| parse_date(trimmed).and_then(|date| date.and_hms_opt(0, 0, 0)))
	.or_else(|| parse_time(trimmed).and_then(|time| NaiveDate::from_ymd_opt(1970, 1, 1).map(|date| date.and_time(time))))
}

pub(super) fn date_to_string(date: NaiveDate) -> String {
	date.format("%Y-%m-%d").to_string()
}

pub(super) fn time_to_string(time: NaiveTime) -> String {
	time.format("%H:%M:%S").to_string()
}

pub(super) fn datetime_to_string(datetime: NaiveDateTime) -> String {
	datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(super) fn mysql_format_to_chrono(format: &str) -> String {

	let mut result = String::new();
	let mut chars = format.chars().peekable();

	while let Some(ch) = chars.next() {

		if ch != '%' {
			result.push(ch);
			continue;
		}

		let Some(token) = chars.next() else {
			result.push('%');
			break;
		};

		match token {

			'%' => result.push('%'),
			'Y' => result.push_str("%Y"),
			'y' => result.push_str("%y"),
			'm' => result.push_str("%m"),
			'c' => result.push_str("%-m"),
			'd' => result.push_str("%d"),
			'e' => result.push_str("%-d"),
			'H' => result.push_str("%H"),
			'k' => result.push_str("%-H"),
			'h' | 'I' => result.push_str("%I"),
			'l' => result.push_str("%-I"),
			'i' => result.push_str("%M"),
			's' | 'S' => result.push_str("%S"),
			'f' => result.push_str("%f"),
			'p' => result.push_str("%p"),
			'W' => result.push_str("%A"),
			'a' => result.push_str("%a"),
			'b' => result.push_str("%b"),
			'M' => result.push_str("%B"),
			'j' => result.push_str("%j"),
			'r' => result.push_str("%I:%M:%S %p"),
			'T' => result.push_str("%H:%M:%S"),
			_ => {
				result.push('%');
				result.push(token);
			}

		}
	
    }

	result

}

pub(super) fn format_datetime_with_mysql_pattern(datetime: &NaiveDateTime, format: &str) -> String {
	datetime.format(&mysql_format_to_chrono(format)).to_string()
}

pub(super) fn format_date_with_mysql_pattern(date: &NaiveDate, format: &str) -> String {
	date.format(&mysql_format_to_chrono(format)).to_string()
}

pub(super) fn format_time_with_mysql_pattern(time: &NaiveTime, format: &str) -> String {
	time.format(&mysql_format_to_chrono(format)).to_string()
}

pub(super) fn str_to_date_with_mysql_pattern(value: &str, format: &str) -> Option<String> {

	let chrono_format = mysql_format_to_chrono(format);
	let trimmed = value.trim();

	NaiveDateTime::parse_from_str(trimmed, &chrono_format)
		.map(datetime_to_string)
		.or_else(|_| NaiveDate::parse_from_str(trimmed, &chrono_format).map(date_to_string))
		.or_else(|_| NaiveTime::parse_from_str(trimmed, &chrono_format).map(time_to_string))
		.ok()

}

pub(super) fn date_from_year_and_day(year: i64, day_of_year: i64) -> Option<String> {
	let date = NaiveDate::from_yo_opt(year as i32, day_of_year as u32)?;
	Some(date_to_string(date))

}

pub(super) fn make_time_string(hours: i64, minutes: i64, seconds: i64) -> Option<String> {
	let negative = hours.is_negative() || minutes.is_negative() || seconds.is_negative();
	let total_seconds = hours.abs() * 3600 + minutes.abs() * 60 + seconds.abs();
	time_from_seconds(if negative { -total_seconds } else { total_seconds })

}

pub(super) fn time_difference_seconds(left: &str, right: &str) -> Option<i64> {

	let left = parse_datetime(left)
		.map(|datetime| datetime.and_utc().timestamp())
		.or_else(|| parse_time(left).map(|time| time.num_seconds_from_midnight() as i64))?;

	let right = parse_datetime(right)
		.map(|datetime| datetime.and_utc().timestamp())
		.or_else(|| parse_time(right).map(|time| time.num_seconds_from_midnight() as i64))?;

	Some(left - right)

}

pub(super) fn date_difference_days(left: &str, right: &str) -> Option<i64> {

	let left = parse_datetime(left)
		.map(|datetime| datetime.date())
		.or_else(|| parse_date(left))?;

	let right = parse_datetime(right)
		.map(|datetime| datetime.date())
		.or_else(|| parse_date(right))?;

	Some(left.signed_duration_since(right).num_days())

}

pub(super) fn normalize_time_string(value: &str) -> Option<String> {

	parse_datetime(value)
		.map(|datetime| datetime.time().format("%H:%M:%S").to_string())
		.or_else(|| parse_time(value).map(time_to_string))
		.or_else(|| parse_date(value).map(|_| "00:00:00".to_string()))

}

pub(super) fn normalize_datetime_string(value: &str) -> Option<String> {

	parse_datetime(value)
		.map(datetime_to_string)
		.or_else(|| {
			parse_date(value)
				.and_then(|date| date.and_hms_opt(0, 0, 0))
				.map(datetime_to_string)
		})
		.or_else(|| {
			parse_time(value).and_then(|time| {
				NaiveDate::from_ymd_opt(1970, 1, 1).map(|date| datetime_to_string(date.and_time(time)))
			})
		})

}

pub(super) fn to_date_string(value: &str) -> Option<String> {
	parse_datetime(value)
		.map(|datetime| datetime.date().format("%Y-%m-%d").to_string())
		.or_else(|| parse_date(value).map(|date| date.format("%Y-%m-%d").to_string()))
}

pub(super) fn extract_year(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.date().year() as i64)
		.or_else(|| parse_date(value).map(|date| date.year() as i64))
}

pub(super) fn extract_month(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.date().month() as i64)
		.or_else(|| parse_date(value).map(|date| date.month() as i64))
}

pub(super) fn extract_day(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.date().day() as i64)
		.or_else(|| parse_date(value).map(|date| date.day() as i64))
}

pub(super) fn extract_quarter(value: &str) -> Option<i64> {
	extract_month(value).map(|month| ((month - 1) / 3) + 1)
}

pub(super) fn extract_hour(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.time().hour() as i64)
		.or_else(|| parse_time(value).map(|time| time.hour() as i64))
}

pub(super) fn extract_minute(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.time().minute() as i64)
		.or_else(|| parse_time(value).map(|time| time.minute() as i64))
}

pub(super) fn extract_second(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.time().second() as i64)
		.or_else(|| parse_time(value).map(|time| time.second() as i64))
}

pub(super) fn extract_microsecond(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| (datetime.and_utc().timestamp_subsec_micros()) as i64)
		.or_else(|| parse_time(value).map(|time| (time.nanosecond() / 1_000) as i64))
}

pub(super) fn extract_day_of_year(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.ordinal() as i64)
		.or_else(|| parse_date(value).map(|date| date.ordinal() as i64))
}

pub(super) fn extract_day_of_week(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.weekday().number_from_sunday() as i64)
		.or_else(|| parse_date(value).map(|date| date.weekday().number_from_sunday() as i64))
}

pub(super) fn extract_weekday(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.weekday().num_days_from_monday() as i64)
		.or_else(|| parse_date(value).map(|date| date.weekday().num_days_from_monday() as i64))
}

pub(super) fn extract_week(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| datetime.iso_week().week() as i64)
		.or_else(|| parse_date(value).map(|date| date.iso_week().week() as i64))
}

pub(super) fn extract_yearweek(value: &str) -> Option<i64> {
	parse_datetime(value)
		.map(|datetime| {
			let week = datetime.iso_week();
			(week.year() as i64) * 100 + week.week() as i64
		})
		.or_else(|| {
			parse_date(value).map(|date| {
				let week = date.iso_week();
				(week.year() as i64) * 100 + week.week() as i64
			})
		})
}

pub(super) fn day_name(value: &str) -> Option<String> {
	parse_datetime(value)
		.map(|datetime| datetime.weekday().to_string())
		.or_else(|| parse_date(value).map(|date| date.weekday().to_string()))
}

pub(super) fn last_day_of_month(value: &str) -> Option<String> {
	
    let date = parse_datetime(value).map(|datetime| datetime.date())
		.or_else(|| parse_date(value))?;
	
    let next_month = if date.month() == 12 {
		NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)?
	} else {
		NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)?
	};
	
    let last_day = next_month.pred_opt()?;
	
    Some(last_day.format("%Y-%m-%d").to_string())

}

pub(super) fn period_add(period: i64, months: i64) -> Option<i64> {
	
    let year = period / 100;
	let month = period % 100;
	
    if month < 1 || month > 12 {
		return None;
	}
	
    let total_months = year * 12 + (month - 1) + months;
	let new_year = total_months.div_euclid(12);
	let new_month = total_months.rem_euclid(12) + 1;
	
    Some(new_year * 100 + new_month)

}

pub(super) fn period_diff(end_period: i64, start_period: i64) -> Option<i64> {
	
    let end_year = end_period / 100;
	let end_month = end_period % 100;
	let start_year = start_period / 100;
	let start_month = start_period % 100;
	
    if !(1..=12).contains(&end_month) || !(1..=12).contains(&start_month) {
		return None;
	}
	
    Some((end_year - start_year) * 12 + (end_month - start_month))

}

pub(super) fn days_from_mysql_origin(date: NaiveDate) -> i64 {
	date.num_days_from_ce() as i64 + 365
}

pub(super) fn date_from_mysql_days(days: i64) -> Option<NaiveDate> {
	NaiveDate::from_num_days_from_ce_opt((days - 365) as i32)
}

pub(super) fn add_days_to_date(date: NaiveDate, days: i64) -> Option<NaiveDate> {
	date.checked_add_signed(chrono::Duration::days(days))
}

pub(super) fn add_days_to_datetime(datetime: NaiveDateTime, days: i64) -> Option<NaiveDateTime> {
	datetime.checked_add_signed(chrono::Duration::days(days))
}

pub(super) fn add_days_to_value(value: &str, days: i64) -> Option<String> {

	if let Some(datetime) = parse_datetime(value) {
		return add_days_to_datetime(datetime, days).map(datetime_to_string);
	}

	parse_date(value)
		.and_then(|date| add_days_to_date(date, days))
		.map(date_to_string)

}

pub(super) fn sub_days_from_date(date: NaiveDate, days: i64) -> Option<NaiveDate> {
	date.checked_sub_signed(chrono::Duration::days(days))
}

pub(super) fn sub_days_from_value(value: &str, days: i64) -> Option<String> {
	add_days_to_value(value, -days)

}

pub(super) fn add_seconds_to_datetime(datetime: NaiveDateTime, seconds: i64) -> Option<NaiveDateTime> {
	datetime.checked_add_signed(chrono::Duration::seconds(seconds))
}

pub(super) fn sub_seconds_from_datetime(datetime: NaiveDateTime, seconds: i64) -> Option<NaiveDateTime> {
	datetime.checked_sub_signed(chrono::Duration::seconds(seconds))
}

pub(super) fn time_seconds_from_value(value: &str) -> Option<i64> {

	let trimmed = value.trim();
	let negative = trimmed.starts_with('-');
	let trimmed = trimmed.trim_start_matches(['+', '-']);

	let seconds = if let Some(time) = parse_time(trimmed) {
		time.num_seconds_from_midnight() as i64
	} else {
		trimmed.parse::<i64>().ok()?
	};

	Some(if negative { -seconds } else { seconds })
	
}

pub(super) fn time_from_seconds(seconds: i64) -> Option<String> {

	let negative = seconds.is_negative();
	let absolute = seconds.abs();
	let hours = absolute / 3600;
	let minutes = (absolute % 3600) / 60;
	let seconds = absolute % 60;
	
    Some(format!(
		"{}{:02}:{:02}:{:02}",
		if negative { "-" } else { "" },
		hours,
		minutes,
		seconds
	))

}

pub(super) fn datetime_from_unix_seconds(seconds: i64) -> Option<String> {

	chrono::DateTime::<Utc>::from_timestamp(seconds, 0)
		.map(|datetime| datetime.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())

}