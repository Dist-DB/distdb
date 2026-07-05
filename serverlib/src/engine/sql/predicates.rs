use crate::{compare_stored_field_values, render_stored_field_value};

use regex::RegexBuilder;

use super::SelectComparisonOp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikeToken {
    AnySingle,
    AnyMany,
    Literal(char),
}

enum LikeFastPath {
    Exact(String),
    Prefix(String),
}

enum LikeAsciiFastPath {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
}

pub fn compare_like_value(
    actual: &[u8],
    pattern: &[u8],
    case_insensitive: bool,
    escape_char: Option<char>,
) -> bool {

    let actual_rendered = render_stored_field_value(actual);
    let pattern_rendered = render_stored_field_value(pattern);

    if let Some(matched) = compare_like_by_literal_segments(
        &actual_rendered,
        &pattern_rendered,
        case_insensitive,
        escape_char,
    ) {
        return matched;
    }

    if let Some(fast_path) = classify_like_ascii_fast_path(&pattern_rendered, escape_char) {
        return apply_like_ascii_fast_path(&actual_rendered, &fast_path, case_insensitive);
    }

    let actual_text = String::from_utf8_lossy(&actual_rendered);
    let pattern_text = String::from_utf8_lossy(&pattern_rendered);

    if let Some(fast_path) = classify_like_fast_path(&pattern_text, escape_char) {
        return apply_like_fast_path(&actual_text, fast_path, case_insensitive);
    }

    let actual_chars = actual_text.chars().collect::<Vec<_>>();
    let pattern_chars = pattern_text.chars().collect::<Vec<_>>();
    let tokens = compile_like_tokens(&pattern_chars, escape_char);

    like_matches(&actual_chars, &tokens, case_insensitive)

}

fn compare_like_by_literal_segments(
    actual: &[u8],
    pattern: &[u8],
    case_insensitive: bool,
    escape_char: Option<char>,
) -> Option<bool> {

    let (segments, leading_wildcard, trailing_wildcard) =
        parse_like_literal_segments(pattern, escape_char)?;

    if segments.is_empty() {
        return Some(true);
    }

    let first_segment = &segments[0];
    let candidate_starts = if leading_wildcard {
        find_all_segment_occurrences(actual, first_segment, case_insensitive)
    } else if matches_segment_at(actual, 0, first_segment, case_insensitive) {
        vec![0]
    } else {
        return Some(false);
    };

    for start in candidate_starts {
        let mut cursor = start + first_segment.len();
        let mut matched = true;

        for segment in &segments[1..] {
            let Some(next_start) = find_segment_from(actual, segment, cursor, case_insensitive) else {
                matched = false;
                break;
            };

            cursor = next_start + segment.len();
        }

        if matched && (trailing_wildcard || cursor == actual.len()) {
            return Some(true);
        }
    }

    Some(false)

}

fn parse_like_literal_segments(
    pattern: &[u8],
    escape_char: Option<char>,
) -> Option<(Vec<Vec<u8>>, bool, bool)> {

    let escape_byte = match escape_char {
        Some(ch) if ch.is_ascii() => Some(ch as u8),
        Some(_) => return None,
        None => None,
    };

    let mut segments = Vec::new();
    let mut current_segment = Vec::new();
    let mut leading_wildcard = false;
    let mut trailing_wildcard = false;
    let mut index = 0usize;

    while index < pattern.len() {

        let current = pattern[index];

        if let Some(escape) = escape_byte
            && current == escape
        {
            if index + 1 < pattern.len() {
                let escaped = pattern[index + 1];
                if !escaped.is_ascii() {
                    return None;
                }

                current_segment.push(escaped);
                index += 2;
                trailing_wildcard = false;
                continue;
            }

            current_segment.push(current);
            index += 1;
            trailing_wildcard = false;
            continue;
        }

        if !current.is_ascii() {
            return None;
        }

        match current {
            b'_' => return None,

            b'%' => {
                if segments.is_empty() && current_segment.is_empty() {
                    leading_wildcard = true;
                }

                if !current_segment.is_empty() {
                    segments.push(std::mem::take(&mut current_segment));
                }

                trailing_wildcard = true;
            }

            literal => {
                current_segment.push(literal);
                trailing_wildcard = false;
            }
        }

        index += 1;

    }

    if !current_segment.is_empty() {
        segments.push(current_segment);
    }

    Some((segments, leading_wildcard, trailing_wildcard))

}

fn find_all_segment_occurrences(
    actual: &[u8],
    segment: &[u8],
    case_insensitive: bool,
) -> Vec<usize> {

    if segment.is_empty() || segment.len() > actual.len() {
        return Vec::new();
    }

    let mut candidates = Vec::new();

    for start in 0..=actual.len() - segment.len() {
        if matches_segment_at(actual, start, segment, case_insensitive) {
            candidates.push(start);
        }
    }

    candidates

}

fn find_segment_from(
    actual: &[u8],
    segment: &[u8],
    start: usize,
    case_insensitive: bool,
) -> Option<usize> {

    if segment.is_empty() {
        return Some(start);
    }

    if start > actual.len() || segment.len() > actual.len().saturating_sub(start) {
        return None;
    }

    #[expect(clippy::manual_find, reason="the loop is intentionally written to avoid allocation and for performance")]
    for candidate_start in start..=actual.len() - segment.len() {
        if matches_segment_at(actual, candidate_start, segment, case_insensitive) {
            return Some(candidate_start);
        }
    }

    None

}

fn matches_segment_at(
    actual: &[u8],
    start: usize,
    segment: &[u8],
    case_insensitive: bool,
) -> bool {

    actual
        .get(start..start.saturating_add(segment.len()))
        .map(|window| {
            if case_insensitive {
                window.eq_ignore_ascii_case(segment)
            } else {
                window == segment
            }
        })
        .unwrap_or(false)

}

fn classify_like_ascii_fast_path(
    pattern: &[u8],
    escape_char: Option<char>,
) -> Option<LikeAsciiFastPath> {

    let escape_byte = match escape_char {
        Some(ch) if ch.is_ascii() => Some(ch as u8),
        Some(_) => return None,
        None => None,
    };

    let mut literal = Vec::with_capacity(pattern.len());
    let mut index = 0usize;
    let mut has_trailing_many = false;

    while index < pattern.len() {

        let current = pattern[index];

        if let Some(escape) = escape_byte
            && current == escape {
                if index + 1 < pattern.len() {
                    let escaped = pattern[index + 1];
                    if !escaped.is_ascii() {
                        return None;
                    }
                    literal.push(escaped);
                    index += 2;
                    continue;
                }

                literal.push(current);
                index += 1;
                continue;
            }

        if !current.is_ascii() {
            return None;
        }

        match current {

            b'_' => return None,

            b'%' => {
                if index + 1 < pattern.len() || has_trailing_many {
                    return None;
                }
                has_trailing_many = true;
            }

            _ => {
                if has_trailing_many {
                    return None;
                }
                literal.push(current);
            }

        }

        index += 1;

    }

    if has_trailing_many {
        Some(LikeAsciiFastPath::Prefix(literal))
    } else {
        Some(LikeAsciiFastPath::Exact(literal))
    }

}

fn apply_like_ascii_fast_path(
    actual: &[u8],
    fast_path: &LikeAsciiFastPath,
    case_insensitive: bool,
) -> bool {

    match fast_path {

        LikeAsciiFastPath::Exact(expected) => {
            if case_insensitive {
                actual.eq_ignore_ascii_case(expected)
            } else {
                actual == expected
            }
        },

        LikeAsciiFastPath::Prefix(prefix) => {
            if case_insensitive {
                actual
                    .get(..prefix.len())
                    .map(|head| head.eq_ignore_ascii_case(prefix))
                    .unwrap_or(false)
            } else {
                actual.starts_with(prefix)
            }
        }

    }

}

fn classify_like_fast_path(pattern: &str, escape_char: Option<char>) -> Option<LikeFastPath> {

    let mut literal = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    let mut escaped = false;
    let mut has_trailing_many = false;

    while let Some(ch) = chars.next() {

        if escaped {
            literal.push(ch);
            escaped = false;
            continue;
        }

        if escape_char.is_some_and(|escape| ch == escape) {
            escaped = true;
            continue;
        }

        match ch {

            '_' => return None,

            '%' => {
                if chars.peek().is_some() || has_trailing_many {
                    return None;
                }
                has_trailing_many = true;
            },

            _ => {
                if has_trailing_many {
                    return None;
                }
                literal.push(ch);
            }
            
        }

    }

    if escaped
        && let Some(escape) = escape_char {
            literal.push(escape);
        }

    if has_trailing_many {
        Some(LikeFastPath::Prefix(literal))
    } else {
        Some(LikeFastPath::Exact(literal))
    }

}

fn apply_like_fast_path(actual: &str, fast_path: LikeFastPath, case_insensitive: bool) -> bool {

    match fast_path {

        LikeFastPath::Exact(expected) => {
            if case_insensitive {
                actual.eq_ignore_ascii_case(&expected)
            } else {
                actual == expected
            }
        },

        LikeFastPath::Prefix(prefix) => {
            if case_insensitive {
                actual
                    .get(..prefix.len())
                    .map(|head| head.eq_ignore_ascii_case(&prefix))
                    .unwrap_or(false)
            } else {
                actual.starts_with(&prefix)
            }
        }

    }

}

fn compile_like_tokens(pattern: &[char], escape_char: Option<char>) -> Vec<LikeToken> {

    let mut tokens = Vec::with_capacity(pattern.len());
    let mut index = 0usize;

    while index < pattern.len() {
        let current = pattern[index];

        if escape_char.is_some_and(|escape| current == escape) {
            if index + 1 < pattern.len() {
                tokens.push(LikeToken::Literal(pattern[index + 1]));
                index += 2;
                continue;
            }

            tokens.push(LikeToken::Literal(current));
            index += 1;
            continue;
        }

        match current {
            '_' => tokens.push(LikeToken::AnySingle),
            '%' => tokens.push(LikeToken::AnyMany),
            literal => tokens.push(LikeToken::Literal(literal)),
        }

        index += 1;
    }

    tokens

}

fn like_matches(actual: &[char], pattern: &[LikeToken], case_insensitive: bool) -> bool {

    let mut actual_index = 0usize;
    let mut pattern_index = 0usize;
    let mut last_percent_index: Option<usize> = None;
    let mut retry_index = 0usize;

    while actual_index < actual.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                LikeToken::AnySingle => {
                    actual_index += 1;
                    pattern_index += 1;
                    continue;
                }

                LikeToken::Literal(expected) => {
                    if like_char_eq(actual[actual_index], expected, case_insensitive) {
                        actual_index += 1;
                        pattern_index += 1;
                        continue;
                    }
                }

                LikeToken::AnyMany => {
                    last_percent_index = Some(pattern_index);
                    pattern_index += 1;
                    retry_index = actual_index;
                    continue;
                }
            }
        }

        if let Some(percent_index) = last_percent_index {
            pattern_index = percent_index + 1;
            retry_index += 1;
            actual_index = retry_index;
            continue;
        }

        return false;

    }

    while pattern_index < pattern.len()
        && matches!(pattern[pattern_index], LikeToken::AnyMany)
    {
        pattern_index += 1;
    }

    pattern_index == pattern.len()

}

fn like_char_eq(actual: char, pattern: char, case_insensitive: bool) -> bool {

    #[expect(clippy::manual_ignore_case_cmp, reason="the comparison is intentionally done with ASCII lowercasing for performance and correctness")]
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
    
    let actual_rendered = render_stored_field_value(actual);
    let pattern_rendered = render_stored_field_value(pattern);
    let actual_text = String::from_utf8_lossy(&actual_rendered);
    let pattern_text = String::from_utf8_lossy(&pattern_rendered);

    let mut builder = RegexBuilder::new(&pattern_text);
    builder.case_insensitive(case_insensitive);

    match builder.build() {
        Ok(regex) => regex.is_match(&actual_text),
        Err(_) => false,
    }
    
}

pub fn compare_row_value(actual: &[u8], expected: &[u8], op: &SelectComparisonOp) -> bool {
    
    let ordering = compare_stored_field_values(actual, expected);

    match op {
        SelectComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        SelectComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
        SelectComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        SelectComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
        SelectComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        SelectComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
    }

}

#[cfg(test)]
mod tests {
    
    use super::{compare_like_value, compare_row_value};
    use crate::{FieldType, TypeConversionPolicy};
    use crate::engine::database::schema::migration::convert_value_to_field_type;
    use crate::SelectComparisonOp;

    #[test]
    fn compare_like_value_supports_escape_character() {
        assert!(compare_like_value(
            b"foo_1",
            b"foo\\_1",
            false,
            Some('\\'),
        ));
    }

    #[test]
    fn compare_like_value_escape_can_be_custom_character() {
        assert!(compare_like_value(
            b"100%",
            b"100!%",
            false,
            Some('!'),
        ));
    }

    #[test]
    fn compare_like_value_supports_simple_prefix_pattern() {
        assert!(compare_like_value(
            b"Amsterdam",
            b"Ams%",
            false,
            None,
        ));
        assert!(!compare_like_value(
            b"Oslo",
            b"Ams%",
            false,
            None,
        ));
    }

    #[test]
    fn compare_like_value_supports_case_insensitive_simple_prefix() {
        assert!(compare_like_value(
            b"amsterdam",
            b"Ams%",
            true,
            None,
        ));
    }

    #[test]
    fn compare_like_value_supports_ordered_multi_segment_patterns() {
        assert!(compare_like_value(
            b"amsterdam",
            b"%ter%am",
            false,
            None,
        ));
        assert!(!compare_like_value(
            b"amsterdam",
            b"%am%ter",
            false,
            None,
        ));
    }

    #[test]
    fn compare_row_value_supports_native_numeric_storage() {
        let actual = convert_value_to_field_type(
            b"42",
            &FieldType::UInt(64),
            TypeConversionPolicy::Safe,
        )
        .expect("numeric field should encode");

        assert!(compare_row_value(&actual, b"42", &SelectComparisonOp::Eq));
        assert!(compare_row_value(&actual, b"7", &SelectComparisonOp::Gt));
    }

}