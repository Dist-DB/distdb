pub(super) fn split_top_level_csv_preserve(text: &str) -> Vec<String> {

    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;

    for (idx, ch) in text.char_indices() {

        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }

        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if in_backtick {
            if ch == '`' {
                in_backtick = false;
            }
            continue;
        }

        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            '`' => in_backtick = true,
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(text[start..idx].to_string());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    parts.push(text[start..].to_string());
    parts

}

pub(super) fn split_top_level_csv_trimmed(text: &str) -> Vec<String> {

    split_top_level_csv_preserve(text)
        .into_iter()
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()

}

pub(super) fn split_top_level_assignment(assignment: &str) -> Option<(String, String)> {

    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;

    for (idx, ch) in assignment.char_indices() {

        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }

        if in_double {
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if in_backtick {
            if ch == '`' {
                in_backtick = false;
            }
            continue;
        }

        match ch {

            '\'' => in_single = true,

            '"' => in_double = true,

            '`' => in_backtick = true,

            '(' => depth = depth.saturating_add(1),

            ')' => depth = depth.saturating_sub(1),

            '=' if depth == 0 => {
                let left = assignment[..idx].to_string();
                let right = assignment[idx + ch.len_utf8()..].to_string();
                return Some((left, right));
            },

            _ => {}

        }
        
    }

    None

}

pub(super) fn find_top_level_phrase_from(sql: &str, phrase: &str, start: usize) -> Option<usize> {

    if phrase.is_empty() || start >= sql.len() {
        return None;
    }

    let bytes = sql.as_bytes();
    let phrase_bytes = phrase.as_bytes();
    if phrase_bytes.len() > bytes.len() {
        return None;
    }

    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut idx = start;

    while idx + phrase_bytes.len() <= bytes.len() {

        let byte = bytes[idx];

        if in_single {
            if byte == b'\'' {
                in_single = false;
            }
            idx += 1;
            continue;
        }

        if in_double {
            if byte == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }

        if in_backtick {
            if byte == b'`' {
                in_backtick = false;
            }
            idx += 1;
            continue;
        }

        match byte {
            
            b'\'' => {
                in_single = true;
                idx += 1;
                continue;
            },

            b'"' => {
                in_double = true;
                idx += 1;
                continue;
            },

            b'`' => {
                in_backtick = true;
                idx += 1;
                continue;
            },

            b'(' => {
                depth = depth.saturating_add(1);
                idx += 1;
                continue;
            },

            b')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
                continue;
            },

            _ => {}

        }

        if depth == 0 && bytes[idx..idx + phrase_bytes.len()].eq_ignore_ascii_case(phrase_bytes)
        {
            let before_ok = idx == 0 || !is_ascii_word(bytes[idx - 1]);
            let after_end = idx + phrase_bytes.len();
            let after_ok = after_end >= bytes.len() || !is_ascii_word(bytes[after_end]);

            if before_ok && after_ok {
                return Some(idx);
            }
        }

        idx += 1;

    }

    None

}

pub(super) fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {

    let bytes = sql.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    if keyword_bytes.is_empty() || bytes.len() < keyword_bytes.len() {
        return None;
    }

    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut idx = 0usize;

    while idx + keyword_bytes.len() <= bytes.len() {
        let ch = bytes[idx] as char;

        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            idx += 1;
            continue;
        }

        if in_double {
            if ch == '"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }

        if in_backtick {
            if ch == '`' {
                in_backtick = false;
            }
            idx += 1;
            continue;
        }

        match ch {
            
            '\'' => {
                in_single = true;
                idx += 1;
                continue;
            },

            '"' => {
                in_double = true;
                idx += 1;
                continue;
            },

            '`' => {
                in_backtick = true;
                idx += 1;
                continue;
            },

            '(' => {
                depth = depth.saturating_add(1);
                idx += 1;
                continue;
            },

            ')' => {
                depth = depth.saturating_sub(1);
                idx += 1;
                continue;
            },

            _ => {}

        }

        if depth == 0 && bytes[idx..idx + keyword_bytes.len()].eq_ignore_ascii_case(keyword_bytes)
        {
            let left_ok = idx == 0
                || !((bytes[idx - 1] as char).is_ascii_alphanumeric() || bytes[idx - 1] == b'_');
            let right_idx = idx + keyword_bytes.len();
            let right_ok = right_idx == bytes.len()
                || !((bytes[right_idx] as char).is_ascii_alphanumeric() || bytes[right_idx] == b'_');

            if left_ok && right_ok {
                return Some(idx);
            }
        }

        idx += 1;

    }

    None

}

fn is_ascii_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}