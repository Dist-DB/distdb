use regex::RegexBuilder;

use super::SelectComparisonOp;

pub fn compare_like_value(actual: &[u8], pattern: &[u8], case_insensitive: bool) -> bool {
    let actual_text = String::from_utf8_lossy(actual);
    let pattern_text = String::from_utf8_lossy(pattern);

    let actual_chars = actual_text.chars().collect::<Vec<_>>();
    let pattern_chars = pattern_text.chars().collect::<Vec<_>>();

    like_matches(&actual_chars, &pattern_chars, case_insensitive)
}

fn like_matches(actual: &[char], pattern: &[char], case_insensitive: bool) -> bool {
    let mut actual_index = 0usize;
    let mut pattern_index = 0usize;
    let mut last_percent_index: Option<usize> = None;
    let mut retry_index = 0usize;

    while actual_index < actual.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '_'
                || like_char_eq(actual[actual_index], pattern[pattern_index], case_insensitive))
        {
            actual_index += 1;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '%' {
            last_percent_index = Some(pattern_index);
            pattern_index += 1;
            retry_index = actual_index;
        } else if let Some(percent_index) = last_percent_index {
            pattern_index = percent_index + 1;
            retry_index += 1;
            actual_index = retry_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == '%' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

fn like_char_eq(actual: char, pattern: char, case_insensitive: bool) -> bool {
    if case_insensitive {
        actual.to_ascii_lowercase() == pattern.to_ascii_lowercase()
    } else {
        actual == pattern
    }
}

pub fn validate_regex_pattern(pattern: &[u8]) -> Result<(), String> {
    
    let pattern_text = String::from_utf8_lossy(pattern);
    
    RegexBuilder::new(&pattern_text)
        .build()
        .map(|_| ())
        .map_err(|err| err.to_string())

}

pub fn compare_regex_value(actual: &[u8], pattern: &[u8], case_insensitive: bool) -> bool {
    
    let actual_text = String::from_utf8_lossy(actual);
    let pattern_text = String::from_utf8_lossy(pattern);

    let mut builder = RegexBuilder::new(&pattern_text);
    builder.case_insensitive(case_insensitive);

    match builder.build() {
        Ok(regex) => regex.is_match(&actual_text),
        Err(_) => false,
    }
    
}

pub fn compare_row_value(actual: &[u8], expected: &[u8], op: &SelectComparisonOp) -> bool {
    
    let ordering = compare_scalar_bytes(actual, expected);

    match op {
        SelectComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        SelectComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
        SelectComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        SelectComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
        SelectComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        SelectComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
    }

}

fn compare_scalar_bytes(left: &[u8], right: &[u8]) -> std::cmp::Ordering {

    let left_text = String::from_utf8_lossy(left);
    let right_text = String::from_utf8_lossy(right);

    match (left_text.parse::<i128>(), right_text.parse::<i128>()) {
        (Ok(lhs), Ok(rhs)) => lhs.cmp(&rhs),
        _ => left_text.cmp(&right_text),
    }

}