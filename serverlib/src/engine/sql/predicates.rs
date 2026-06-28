use crate::{compare_stored_field_values, render_stored_field_value};

use regex::RegexBuilder;

use super::SelectComparisonOp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikeToken {
    AnySingle,
    AnyMany,
    Literal(char),
}

pub fn compare_like_value(
    actual: &[u8],
    pattern: &[u8],
    case_insensitive: bool,
    escape_char: Option<char>,
) -> bool {
    let actual_rendered = render_stored_field_value(actual);
    let pattern_rendered = render_stored_field_value(pattern);
    let actual_text = String::from_utf8_lossy(&actual_rendered);
    let pattern_text = String::from_utf8_lossy(&pattern_rendered);

    let actual_chars = actual_text.chars().collect::<Vec<_>>();
    let pattern_chars = pattern_text.chars().collect::<Vec<_>>();
    let tokens = compile_like_tokens(&pattern_chars, escape_char);

    like_matches(&actual_chars, &tokens, case_insensitive)
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
    use crate::engine::database::schema_migration::convert_value_to_field_type;
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