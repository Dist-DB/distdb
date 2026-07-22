use sqlparser::ast::Statement;
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;

use super::classify;
use super::text_scan::{
    find_top_level_phrase_from, split_top_level_assignment, split_top_level_csv_preserve,
};
use super::types::ParsedOrFallback;
use super::SqlParseError;

pub(super) fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {

    let mysql = MySqlDialect {};
    let normalized = normalize_mysql_compat_hints(
        &normalize_insert_set_syntax(&normalize_insert_default_values_with_column_list(
            &normalize_mysql_compat_modifiers(sql),
        )),
    );
    let parse_input = if normalized != sql { normalized.as_str() } else { sql };

    match Parser::parse_sql(&mysql, parse_input) {
        
        Ok(statements) => Ok(statements),
        
        Err(mysql_error) => {
            let generic = GenericDialect {};
            Parser::parse_sql(&generic, parse_input)
                .map_err(|_| SqlParseError::UnsupportedStatement(mysql_error.to_string()))
        }

    }

}

fn normalize_insert_set_syntax(sql: &str) -> String {

    let lowered = sql.to_ascii_lowercase();
    if !lowered.contains("insert") || !lowered.contains(" set ") {
        return sql.to_string();
    }

    let Some(insert_idx) = find_top_level_phrase_from(sql, "insert", 0) else {
        return sql.to_string();
    };

    let Some(into_idx) = find_top_level_phrase_from(sql, "into", insert_idx + "insert".len()) else {
        return sql.to_string();
    };

    let Some(set_idx) = find_top_level_phrase_from(sql, "set", into_idx + "into".len()) else {
        return sql.to_string();
    };

    if find_top_level_phrase_from(sql, "values", into_idx + "into".len())
        .is_some_and(|idx| idx < set_idx)
    {
        return sql.to_string();
    }

    if find_top_level_phrase_from(sql, "select", into_idx + "into".len())
        .is_some_and(|idx| idx < set_idx)
    {
        return sql.to_string();
    }

    let assignments_start = skip_ascii_whitespace_bytes(sql.as_bytes(), set_idx + "set".len());

    let mut assignment_end = sql.len();

    for marker in ["on duplicate key update", "returning"] {
        if let Some(idx) = find_top_level_phrase_from(sql, marker, assignments_start)
            && idx < assignment_end
        {
            assignment_end = idx;
        }
    }

    let assignment_clause = sql[assignments_start..assignment_end].trim();

    if assignment_clause.is_empty() {
        return sql.to_string();
    }

    let mut columns = Vec::new();
    let mut values = Vec::new();

    for assignment in split_top_level_csv_preserve(assignment_clause) {
        let Some((column, value)) = split_top_level_assignment(assignment.as_str()) else {
            return sql.to_string();
        };

        let Some(normalized_column) = normalize_insert_set_target_column(column.trim()) else {
            return sql.to_string();
        };

        if value.trim().is_empty() {
            return sql.to_string();
        }

        columns.push(normalized_column);
        values.push(value.trim().to_string());
    }

    if columns.is_empty() {
        return sql.to_string();
    }

    let replacement = format!(
        "({}) values ({})",
        columns.join(", "),
        values.join(", "),
    );

    let prefix = sql[..set_idx].trim_end();
    let suffix = sql[assignment_end..].trim_start();

    if suffix.is_empty() {
        format!("{prefix} {replacement}")
    } else {
        format!("{prefix} {replacement} {suffix}")
    }

}

fn normalize_insert_default_values_with_column_list(sql: &str) -> String {

    let mut normalized = sql.to_string();
    let mut search_from = 0usize;
    let needle = "default values";

    loop {

        let lowered = normalized.to_ascii_lowercase();
        let Some(relative_idx) = lowered.get(search_from..).and_then(|slice| slice.find(needle)) else {
            break;
        };

        let default_idx = search_from + relative_idx;

        let Some((_, _, column_count)) =
            locate_insert_column_list_for_default_values(&normalized, default_idx)
        else {
            search_from = default_idx + needle.len();
            continue;
        };

        if column_count == 0 {
            search_from = default_idx + needle.len();
            continue;
        }

        let replacement = format!(
            "values ({})",
            std::iter::repeat_n("default", column_count)
                .collect::<Vec<_>>()
                .join(", ")
        );

        let default_end = default_idx + needle.len();
        normalized.replace_range(default_idx..default_end, &replacement);
        search_from = default_idx + replacement.len();

    }

    normalized

}

fn locate_insert_column_list_for_default_values(
    sql: &str,
    default_idx: usize,
) -> Option<(usize, usize, usize)> {

    let bytes = sql.as_bytes();
    let mut cursor = default_idx;

    while cursor > 0 && bytes[cursor - 1].is_ascii_whitespace() {
        cursor -= 1;
    }

    if cursor == 0 || bytes[cursor - 1] != b')' {
        return None;
    }

    let close_idx = cursor - 1;
    let open_idx = find_matching_open_paren(bytes, close_idx)?;

    let lowered = sql.to_ascii_lowercase();
    let insert_prefix = lowered.get(..open_idx)?;
    if !insert_prefix.contains("insert") || !insert_prefix.contains("into") {
        return None;
    }

    let columns = sql.get(open_idx + 1..close_idx)?.trim();
    if columns.is_empty() {
        return None;
    }

    let column_count = split_top_level_csv_preserve(columns)
        .into_iter()
        .filter(|segment| !segment.trim().is_empty())
        .count();

    Some((open_idx, close_idx, column_count))

}

fn find_matching_open_paren(bytes: &[u8], close_idx: usize) -> Option<usize> {

    if close_idx >= bytes.len() || bytes[close_idx] != b')' {
        return None;
    }

    let mut depth = 1usize;
    let mut idx = close_idx;

    while idx > 0 {
        
        idx -= 1;
        
        match bytes[idx] {
            
            b')' => depth = depth.saturating_add(1),

            b'(' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            },

            _ => {}

        }

    }

    None

}


fn normalize_insert_set_target_column(target: &str) -> Option<String> {

    let trimmed = target.trim();
    if trimmed.is_empty() {
        return None;
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }

    if !parts.iter().all(|part| is_valid_insert_set_identifier_part(part)) {
        return None;
    }

    let leaf = parts.last()?.trim().trim_matches('`').trim_matches('"');
    if leaf.is_empty() {
        return None;
    }

    Some(leaf.to_string())

}

fn is_valid_insert_set_identifier_part(part: &str) -> bool {

    let token = part.trim();
    if token.is_empty() {
        return false;
    }

    let unquoted = token.trim_matches('`').trim_matches('"');
    !unquoted.is_empty()
        && unquoted
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')

}


fn skip_ascii_whitespace_bytes(bytes: &[u8], mut idx: usize) -> usize {

    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }

    idx

}

fn normalize_mysql_compat_modifiers(sql: &str) -> String {

    let mut normalized = sql.to_string();

    // Common SELECT modifiers that are compatibility-only in current execution model.
    for (pattern, replacement) in [
        ("select sql_no_cache ", "select "),
        ("select sql_cache ", "select "),
        ("select high_priority ", "select "),
        ("select straight_join ", "select "),
        ("select sql_small_result ", "select "),
        ("select sql_big_result ", "select "),
        ("select sql_buffer_result ", "select "),
        ("select sql_calc_found_rows ", "select "),
        ("select distinct sql_no_cache ", "select distinct "),
        ("select distinct sql_cache ", "select distinct "),
        ("select distinct high_priority ", "select distinct "),
        ("select distinct straight_join ", "select distinct "),
        ("select distinct sql_small_result ", "select distinct "),
        ("select distinct sql_big_result ", "select distinct "),
        ("select distinct sql_buffer_result ", "select distinct "),
        ("select distinct sql_calc_found_rows ", "select distinct "),
        ("select all sql_no_cache ", "select all "),
        ("select all sql_cache ", "select all "),
        ("select all high_priority ", "select all "),
        ("select all straight_join ", "select all "),
        ("select all sql_small_result ", "select all "),
        ("select all sql_big_result ", "select all "),
        ("select all sql_buffer_result ", "select all "),
        ("select all sql_calc_found_rows ", "select all "),
    ] {
        normalized = replace_case_insensitive_pattern(&normalized, pattern, replacement);
    }

    // Common DML priority modifiers; treated as no-op compatibility tokens.
    for (pattern, replacement) in [
        ("insert low_priority ", "insert "),
        ("insert delayed ", "insert "),
        ("insert high_priority ", "insert "),
        ("insert low_priority delayed ", "insert "),
        ("insert delayed low_priority ", "insert "),
        ("insert ignore low_priority ", "insert ignore "),
        ("insert low_priority ignore ", "insert ignore "),
        ("insert ignore delayed ", "insert ignore "),
        ("insert delayed ignore ", "insert ignore "),
        ("insert ignore high_priority ", "insert ignore "),
        ("insert high_priority ignore ", "insert ignore "),
        ("update low_priority ", "update "),
        ("update ignore ", "update "),
        ("update low_priority ignore ", "update "),
        ("update ignore low_priority ", "update "),
        ("delete low_priority ", "delete "),
        ("delete quick ", "delete "),
        ("delete ignore ", "delete "),
        ("delete low_priority quick ", "delete "),
        ("delete low_priority ignore ", "delete "),
        ("delete quick low_priority ", "delete "),
        ("delete quick ignore ", "delete "),
        ("delete ignore low_priority ", "delete "),
        ("delete ignore quick ", "delete "),
        ("delete low_priority quick ignore ", "delete "),
        ("delete low_priority ignore quick ", "delete "),
        ("delete quick low_priority ignore ", "delete "),
        ("delete quick ignore low_priority ", "delete "),
        ("delete ignore low_priority quick ", "delete "),
        ("delete ignore quick low_priority ", "delete "),
    ] {
        normalized = replace_case_insensitive_pattern(&normalized, pattern, replacement);
    }

    normalized

}

fn replace_case_insensitive_pattern(input: &str, pattern: &str, replacement: &str) -> String {

    let mut current = input.to_string();

    loop {

        let lowered_current = current.to_ascii_lowercase();
        let lowered_pattern = pattern.to_ascii_lowercase();

        let Some(index) = lowered_current.find(&lowered_pattern) else {
            break;
        };

        let end = index + pattern.len();
        current.replace_range(index..end, replacement);

    }

    current

}

fn normalize_mysql_compat_hints(sql: &str) -> String {
    let without_optimizer_hints = strip_optimizer_hint_comments(sql);
    strip_table_index_hints(&without_optimizer_hints)
}

fn strip_optimizer_hint_comments(sql: &str) -> String {

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;

    while i < bytes.len() {

        let byte = bytes[i];

        if !in_single
            && !in_double
            && !in_backtick
            && i + 2 < bytes.len()
            && byte == b'/'
            && bytes[i + 1] == b'*'
            && bytes[i + 2] == b'+'
        {
            i += 3;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }

            if i + 1 < bytes.len() {
                i += 2;
            }

            out.push(' ');
            continue;
        }

        match byte {
            b'\'' if !in_double && !in_backtick => in_single = !in_single,
            b'"' if !in_single && !in_backtick => in_double = !in_double,
            b'`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ => {}
        }

        out.push(byte as char);
        i += 1;
    
    }

    out

}

fn strip_table_index_hints(sql: &str) -> String {

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;

    while i < bytes.len() {

        let byte = bytes[i];

        if !in_single
            && !in_double
            && !in_backtick
            && byte.is_ascii_whitespace()
            && let Some(end) = consume_table_index_hint(bytes, i)
        {
            out.push(' ');
            i = end;
            continue;
        }

        match byte {
            b'\'' if !in_double && !in_backtick => in_single = !in_single,
            b'"' if !in_single && !in_backtick => in_double = !in_double,
            b'`' if !in_single && !in_double => in_backtick = !in_backtick,
            _ => {}
        }

        out.push(byte as char);
        i += 1;

    }

    out

}

fn consume_table_index_hint(bytes: &[u8], start: usize) -> Option<usize> {

    if start >= bytes.len() || !bytes[start].is_ascii_whitespace() {
        return None;
    }

    let mut idx = skip_ascii_whitespace(bytes, start);

    idx = match_ascii_keyword(bytes, idx, "use")
        .or_else(|| match_ascii_keyword(bytes, idx, "ignore"))
        .or_else(|| match_ascii_keyword(bytes, idx, "force"))?;

    if idx >= bytes.len() || !bytes[idx].is_ascii_whitespace() {
        return None;
    }

    idx = skip_ascii_whitespace(bytes, idx);

    idx = match_ascii_keyword(bytes, idx, "index")
        .or_else(|| match_ascii_keyword(bytes, idx, "key"))?;

    let mut after_key = idx;

    if after_key < bytes.len() && bytes[after_key].is_ascii_whitespace() {

        let maybe_for = skip_ascii_whitespace(bytes, after_key);

        if let Some(mut after_for) = match_ascii_keyword(bytes, maybe_for, "for") {

            if after_for >= bytes.len() || !bytes[after_for].is_ascii_whitespace() {
                return None;
            }

            after_for = skip_ascii_whitespace(bytes, after_for);

            #[expect(clippy::question_mark, reason="we want to return None if the next token is not 'by'")]
            if let Some(after_join) = match_ascii_keyword(bytes, after_for, "join") {

                after_key = after_join;

            } else if let Some(after_order) = match_ascii_keyword(bytes, after_for, "order") {
                
                if after_order >= bytes.len() || !bytes[after_order].is_ascii_whitespace() {
                    return None;
                }
                
                let after_order_ws = skip_ascii_whitespace(bytes, after_order);
                after_key = match_ascii_keyword(bytes, after_order_ws, "by")?;

            } else if let Some(after_group) = match_ascii_keyword(bytes, after_for, "group") {
                
                if after_group >= bytes.len() || !bytes[after_group].is_ascii_whitespace() {
                    return None;
                }
                
                let after_group_ws = skip_ascii_whitespace(bytes, after_group);
                after_key = match_ascii_keyword(bytes, after_group_ws, "by")?;

            } else {

                return None;

            }

        }

    }
    
    idx = skip_ascii_whitespace(bytes, after_key);
    if idx >= bytes.len() || bytes[idx] != b'(' {
        return None;
    }

    let mut depth = 1usize;
    idx += 1;

    while idx < bytes.len() {

        match bytes[idx] {

            b'(' => depth = depth.saturating_add(1),

            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx + 1);
                }
            },

            _ => {}

        }

        idx += 1;

    }

    None

}

fn match_ascii_keyword(bytes: &[u8], start: usize, keyword: &str) -> Option<usize> {

    let keyword_bytes = keyword.as_bytes();
    let end = start.checked_add(keyword_bytes.len())?;
    if end > bytes.len() {
        return None;
    }

    if !bytes[start..end].eq_ignore_ascii_case(keyword_bytes) {
        return None;
    }

    if start > 0 && is_ascii_word(bytes[start - 1]) {
        return None;
    }

    if end < bytes.len() && is_ascii_word(bytes[end]) {
        return None;
    }

    Some(end)

}

fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {

    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    
    idx

}

fn is_ascii_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(super) fn parse_or_fallback(sql: &str) -> Result<ParsedOrFallback, SqlParseError> {
    
    match parse_mysql_statements(sql) {

        Ok(statements) => Ok(ParsedOrFallback::Parsed(statements)),
        
        Err(parse_error) => {
            let trimmed = sql.trim();
            if let Some(metadata) = classify::classify_text_fallback(trimmed) {
                Ok(ParsedOrFallback::Fallback {
                    trimmed_sql: trimmed.to_string(),
                    metadata,
                })
            } else {
                Err(parse_error)
            }
        }

    }

}
