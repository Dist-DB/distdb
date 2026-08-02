use std::collections::HashMap;

use crate::engine::database::core::{DatabaseCatalog, DatabaseTable};
use crate::{FieldIndex, FieldType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportTarget {
    Database,
    Table(String),
    View(String),
    OlapView(String),
    Function(String),
    Procedure(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRequest {
    pub target: ExportTarget,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportScriptPlan {
    pub script: String,
    pub data_table_ids: Vec<String>,
}

pub fn parse_export_request(sql: &str) -> Result<ExportRequest, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err("export requires format: export <structure> to <path>".to_string());
    }

    let tokens = tokenize_export_statement(trimmed);
    if !tokens
        .first()
        .is_some_and(|token| token.eq_ignore_ascii_case("export"))
    {
        return Err("export requires format: export <structure> to <path>".to_string());
    }

    let (target, path_token_index) = parse_export_target_from_tokens(&tokens)?;
    let path = parse_export_path_token(tokens.get(path_token_index).map(String::as_str))
        .ok_or_else(|| "export requires a destination path".to_string())?;

    Ok(ExportRequest { target, path })
}

pub fn plan_export_script(catalog: &DatabaseCatalog, target: &ExportTarget) -> Result<ExportScriptPlan, String> {
    let mut script = String::new();
    let database_name = if catalog.database_name().is_empty() {
        catalog.database_id.0.as_str()
    } else {
        catalog.database_name()
    };

    script.push_str("-- DistDB export\n");
    script.push_str(&format!(
        "CREATE DATABASE IF NOT EXISTS {};\n",
        quote_dotted_identifier(database_name),
    ));
    script.push_str(&format!("USE {};\n\n", quote_dotted_identifier(database_name)));

    let data_table_ids = match target {
        ExportTarget::Database => export_database_structure(catalog, &mut script),
        ExportTarget::Table(table_id) => export_table_structure(catalog, table_id, &mut script)?,
        ExportTarget::View(view_id) => {
            export_view_structure(catalog, view_id, &mut script)?;
            Vec::new()
        }
        ExportTarget::OlapView(view_id) => {
            export_olap_view_structure(catalog, view_id, &mut script)?;
            Vec::new()
        }
        ExportTarget::Function(function_id) => {
            export_function_structure(catalog, function_id, &mut script)?;
            Vec::new()
        }
        ExportTarget::Procedure(procedure_id) => {
            export_procedure_structure(catalog, procedure_id, &mut script)?;
            Vec::new()
        }
    };

    Ok(ExportScriptPlan {
        script,
        data_table_ids,
    })
}

pub fn append_table_rows_to_export_script(
    script: &mut String,
    table: &DatabaseTable,
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
) {
    if table.is_temporary() || live_rows.is_empty() {
        return;
    }

    let mut fields = table.schema.fields.clone();
    fields.sort_by_key(|field| field.seqno);

    let field_names = fields
        .iter()
        .map(|field| quote_dotted_identifier(&field.field_name))
        .collect::<Vec<_>>();

    for (_row_id, row_map) in live_rows {
        let values = fields
            .iter()
            .map(|field| sql_literal_from_row_value(row_map.get(&field.field_name), &field.field_type))
            .collect::<Vec<_>>();

        script.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            quote_dotted_identifier(&table.table_id),
            field_names.join(", "),
            values.join(", ")
        ));
    }

    script.push('\n');
}

fn export_database_structure(catalog: &DatabaseCatalog, script: &mut String) -> Vec<String> {
    let mut table_ids = catalog.table_ids();
    table_ids.sort();

    let mut function_sql = Vec::<String>::new();
    let mut procedure_sql = Vec::<String>::new();

    let mut routine_ids = catalog.stored_procedure_ids();
    routine_ids.sort();

    for routine_id in &routine_ids {
        if let Some(routine) = catalog.stored_procedure(routine_id) {
            if routine
                .sql
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("create function")
            {
                function_sql.push(routine.sql.clone());
            } else {
                procedure_sql.push(routine.sql.clone());
            }
        }
    }

    let mut view_ids = catalog.view_ids();
    view_ids.sort();
    let view_sql = view_ids
        .iter()
        .filter_map(|view_id| catalog.view(view_id).map(|view| ensure_sql_terminated(&view.sql)))
        .collect::<Vec<_>>();

    let mut olap_view_ids = catalog.olap_view_ids();
    olap_view_ids.sort();
    let olap_view_sql = olap_view_ids
        .iter()
        .filter_map(|view_id| {
            catalog
                .olap_view(view_id)
                .map(|view| ensure_sql_terminated(&view.sql))
        })
        .collect::<Vec<_>>();

    for table_id in &table_ids {
        let Some(table) = catalog.table(table_id) else {
            continue;
        };

        script.push_str(&render_create_table_sql(&table));
        script.push_str("\n\n");
    }

    for sql in function_sql {
        append_routine_sql(script, &sql);
    }

    for sql in procedure_sql {
        append_routine_sql(script, &sql);
    }

    for sql in view_sql {
        script.push_str(&sql);
        script.push_str("\n\n");
    }

    for sql in olap_view_sql {
        script.push_str(&sql);
        script.push_str("\n\n");
    }

    table_ids
}

fn export_table_structure(
    catalog: &DatabaseCatalog,
    table_id: &str,
    script: &mut String,
) -> Result<Vec<String>, String> {
    let Some(table) = catalog.table(table_id) else {
        return Err(format!("table '{}' not found", table_id));
    };

    script.push_str(&render_create_table_sql(&table));
    script.push_str("\n\n");

    Ok(vec![table_id.to_string()])
}

fn export_view_structure(
    catalog: &DatabaseCatalog,
    view_id: &str,
    script: &mut String,
) -> Result<(), String> {
    let Some(view) = catalog.view(view_id) else {
        return Err(format!("view '{}' not found", view_id));
    };

    script.push_str(&ensure_sql_terminated(&view.sql));
    script.push_str("\n\n");
    Ok(())
}

fn export_olap_view_structure(
    catalog: &DatabaseCatalog,
    view_id: &str,
    script: &mut String,
) -> Result<(), String> {
    let Some(view) = catalog.olap_view(view_id) else {
        return Err(format!("olapview '{}' not found", view_id));
    };

    script.push_str(&ensure_sql_terminated(&view.sql));
    script.push_str("\n\n");
    Ok(())
}

fn export_function_structure(
    catalog: &DatabaseCatalog,
    function_id: &str,
    script: &mut String,
) -> Result<(), String> {
    let Some(routine) = catalog.stored_procedure(function_id) else {
        return Err(format!("function '{}' not found", function_id));
    };

    if !routine
        .sql
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("create function")
    {
        return Err(format!("object '{}' is not a function", function_id));
    }

    append_routine_sql(script, &routine.sql);
    Ok(())
}

fn export_procedure_structure(
    catalog: &DatabaseCatalog,
    procedure_id: &str,
    script: &mut String,
) -> Result<(), String> {
    let Some(routine) = catalog.stored_procedure(procedure_id) else {
        return Err(format!("procedure '{}' not found", procedure_id));
    };

    if routine
        .sql
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("create function")
    {
        return Err(format!("object '{}' is a function, not a procedure", procedure_id));
    }

    append_routine_sql(script, &routine.sql);
    Ok(())
}

fn parse_export_target_from_tokens(tokens: &[String]) -> Result<(ExportTarget, usize), String> {
    let first = tokens
        .get(1)
        .ok_or_else(|| "export target is required".to_string())?
        .to_ascii_lowercase();

    match first.as_str() {
        "database" => {
            if tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(3).is_some()
                && tokens.get(4).is_none()
            {
                return Ok((ExportTarget::Database, 3));
            }
            Err("export database syntax: export database to <path>".to_string())
        }

        "table" => {
            if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(4).is_some()
                && tokens.get(5).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(2).map(String::as_str),
                    "export table syntax: export table <name> to <path>",
                )?;
                return Ok((ExportTarget::Table(name), 4));
            }
            Err("export table syntax: export table <name> to <path>".to_string())
        }

        "view" => {
            if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(4).is_some()
                && tokens.get(5).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(2).map(String::as_str),
                    "export view syntax: export view <name> to <path>",
                )?;
                return Ok((ExportTarget::View(name), 4));
            }
            Err("export view syntax: export view <name> to <path>".to_string())
        }

        "olapview" | "olap_view" => {
            if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(4).is_some()
                && tokens.get(5).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(2).map(String::as_str),
                    "export olapview syntax: export olapview <name> to <path>",
                )?;
                return Ok((ExportTarget::OlapView(name), 4));
            }
            Err("export olapview syntax: export olapview <name> to <path>".to_string())
        }

        "function" => {
            if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(4).is_some()
                && tokens.get(5).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(2).map(String::as_str),
                    "export function syntax: export function <name> to <path>",
                )?;
                return Ok((ExportTarget::Function(name), 4));
            }
            Err("export function syntax: export function <name> to <path>".to_string())
        }

        "procedure" => {
            if tokens.get(3).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(4).is_some()
                && tokens.get(5).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(2).map(String::as_str),
                    "export procedure syntax: export procedure <name> to <path>",
                )?;
                return Ok((ExportTarget::Procedure(name), 4));
            }
            Err("export procedure syntax: export procedure <name> to <path>".to_string())
        }

        "stored" => {
            if tokens.get(2).is_some_and(|token| token.eq_ignore_ascii_case("procedure"))
                && tokens.get(4).is_some_and(|token| token.eq_ignore_ascii_case("to"))
                && tokens.get(5).is_some()
                && tokens.get(6).is_none()
            {
                let name = parse_export_named_object(
                    tokens.get(3).map(String::as_str),
                    "export stored procedure syntax: export stored procedure <name> to <path>",
                )?;
                return Ok((ExportTarget::Procedure(name), 5));
            }
            Err("export stored procedure syntax: export stored procedure <name> to <path>".to_string())
        }

        _ => Err("unsupported export target; use database/table/view/olapview/function/procedure".to_string()),
    }
}

fn parse_export_named_object(raw: Option<&str>, message: &str) -> Result<String, String> {
    let raw = raw.ok_or_else(|| message.to_string())?;
    let normalized = normalize_dotted_identifier(raw).ok_or_else(|| message.to_string())?;

    if normalized.is_empty() {
        return Err(message.to_string());
    }

    Ok(common::normalize_identifier!(normalized))
}

fn normalize_dotted_identifier(identifier: &str) -> Option<String> {
    let parts = identifier
        .split('.')
        .map(|part| part.trim_matches(|c| c == ';' || c == '`' || c == '"' || c == '\''))
        .collect::<Vec<_>>();

    if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
        return None;
    }

    Some(parts.join("."))
}

fn parse_export_path_token(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim_matches('"').trim_matches('\'').trim();

    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn tokenize_export_statement(statement: &str) -> Vec<String> {
    let mut tokens = Vec::<String>::new();
    let mut current = String::new();

    let mut quote: Option<char> = None;
    let mut chars = statement.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(q) = quote {
            current.push(ch);

            if ch == q {
                quote = None;
                continue;
            }

            if q != '`' && ch == '\\' {
                if let Some(next_ch) = chars.next() {
                    current.push(next_ch);
                }
                continue;
            }

            continue;
        }

        if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            continue;
        }

        if ch == '\'' || ch == '"' || ch == '`' {
            quote = Some(ch);
            current.push(ch);
            continue;
        }

        current.push(ch);
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn render_create_table_sql(table: &DatabaseTable) -> String {
    let mut fields = table.schema.fields.clone();
    fields.sort_by_key(|field| field.seqno);

    let mut parts = fields
        .iter()
        .map(|field| field.to_sql_string())
        .collect::<Vec<_>>();

    let mut primary_keys = fields
        .iter()
        .filter(|field| matches!(field.indexed, FieldIndex::PrimaryKey))
        .map(|field| field.field_name.clone())
        .collect::<Vec<_>>();

    if primary_keys.is_empty()
        && let Some(index) = table
            .indexes
            .values()
            .find(|index| index.is_primary_key() && !index.field_names.is_empty())
    {
        primary_keys = index.field_names.clone();
    }

    if !primary_keys.is_empty() {
        let columns = primary_keys
            .iter()
            .map(|name| quote_dotted_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        parts.push(format!("PRIMARY KEY ({columns})"));
    }

    let mut secondary_indexes = table
        .indexes
        .values()
        .filter(|index| !index.is_primary_key() && !index.field_names.is_empty() && !index.is_temporary())
        .cloned()
        .collect::<Vec<_>>();

    secondary_indexes.sort_by(|left, right| left.index_id.0.cmp(&right.index_id.0));

    for index in secondary_indexes {
        let columns = index
            .field_names
            .iter()
            .map(|name| quote_dotted_identifier(name))
            .collect::<Vec<_>>()
            .join(", ");
        let index_name = quote_identifier_atom(&index.index_id.0);
        if index.is_unique_key() {
            parts.push(format!("UNIQUE KEY {} ({})", index_name, columns));
        } else {
            parts.push(format!("KEY {} ({})", index_name, columns));
        }
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        quote_dotted_identifier(&table.table_id),
        parts.join(", "),
    )
}

fn ensure_sql_terminated(sql: &str) -> String {
    let trimmed = sql.trim();

    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{};", trimmed)
    }
}

fn append_routine_sql(script: &mut String, sql: &str) {
    let body = sql.trim().trim_end_matches(';').trim();
    script.push_str("DELIMITER $$\n");
    script.push_str(body);
    script.push_str("\n$$\nDELIMITER ;\n\n");
}

fn sql_literal_from_row_value(value: Option<&Vec<u8>>, field_type: &FieldType) -> String {
    let Some(raw) = value else {
        return "NULL".to_string();
    };

    let rendered = crate::render_stored_field_value(raw);

    if rendered.is_empty() {
        return "NULL".to_string();
    }

    match field_type {
        FieldType::Int(_) | FieldType::UInt(_) => {
            sanitize_integer_literal(&rendered).unwrap_or_else(|| "NULL".to_string())
        }
        FieldType::Float(_) => sanitize_float_literal(&rendered).unwrap_or_else(|| "NULL".to_string()),
        _ => sql_string_literal_from_bytes(&rendered),
    }
}

fn quote_identifier_atom(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn quote_dotted_identifier(identifier: &str) -> String {
    identifier
        .split('.')
        .map(|part| quote_identifier_atom(part.trim_matches('`')))
        .collect::<Vec<_>>()
        .join(".")
}

fn sanitize_integer_literal(bytes: &[u8]) -> Option<String> {
    let numeric = std::str::from_utf8(bytes).ok()?.trim();
    if numeric.is_empty() {
        return None;
    }

    if let Ok(value) = numeric.parse::<i128>() {
        return Some(value.to_string());
    }

    if let Ok(value) = numeric.parse::<u128>() {
        return Some(value.to_string());
    }

    None
}

fn sanitize_float_literal(bytes: &[u8]) -> Option<String> {
    let numeric = std::str::from_utf8(bytes).ok()?.trim();
    if numeric.is_empty() {
        return None;
    }

    let value = numeric.parse::<f64>().ok()?;
    if !value.is_finite() {
        return None;
    }

    Some(value.to_string())
}

fn sql_string_literal_from_bytes(bytes: &[u8]) -> String {
    if std::str::from_utf8(bytes).is_err() {
        return format!("X'{}'", hex_encode(bytes));
    }

    let text = String::from_utf8_lossy(bytes);
    let escaped = text
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\0', "\\0")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('\u{001A}', "\\Z");

    format!("'{}'", escaped)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_export_request_supports_quoted_identifier_and_spaced_path() {
        let parsed = parse_export_request("export table `Order` to '/tmp/folder/export file.sql';")
            .expect("export table with quoted identifier and spaced path should parse");

        assert_eq!(parsed.target, ExportTarget::Table("order".to_string()));
        assert_eq!(parsed.path, "/tmp/folder/export file.sql");
    }

    #[test]
    fn parse_export_request_supports_path_containing_to_substring() {
        let parsed = parse_export_request("export view sales_view to '/tmp/to folder/export-to-file.sql';")
            .expect("export with to substring in path should parse");

        assert_eq!(parsed.target, ExportTarget::View("sales_view".to_string()));
        assert_eq!(parsed.path, "/tmp/to folder/export-to-file.sql");
    }

    #[test]
    fn parse_export_request_supports_stored_procedure_form() {
        let parsed = parse_export_request("export stored procedure `p_sync` to '/tmp/p sync.sql';")
            .expect("export stored procedure should parse");

        assert_eq!(parsed.target, ExportTarget::Procedure("p_sync".to_string()));
        assert_eq!(parsed.path, "/tmp/p sync.sql");
    }

    #[test]
    fn parse_export_request_rejects_missing_destination() {
        let parsed = parse_export_request("export database to");
        assert!(parsed.is_err());
    }

    #[test]
    fn quote_dotted_identifier_escapes_breakout_characters() {
        let quoted = quote_dotted_identifier("db.na`me");
        assert_eq!(quoted, "`db`.`na``me`");
    }

    #[test]
    fn sql_string_literal_from_bytes_escapes_statement_breakout_payload() {
        let literal = sql_string_literal_from_bytes(b"x' ; DROP TABLE users; --\\n");
        assert_eq!(literal, "'x\\' ; DROP TABLE users; --\\\\n'");
    }

    #[test]
    fn sql_literal_from_row_value_rejects_invalid_numeric_payload() {
        let payload = b"0); DROP TABLE users; --".to_vec();
        let literal = sql_literal_from_row_value(Some(&payload), &FieldType::Int(64));
        assert_eq!(literal, "NULL");
    }

    #[test]
    fn sql_string_literal_from_bytes_uses_hex_for_non_utf8() {
        let literal = sql_string_literal_from_bytes(&[0xff, 0x00, 0x41]);
        assert_eq!(literal, "X'ff0041'");
    }
}
