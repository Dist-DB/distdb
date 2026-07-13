use super::*;

const CTE_MAX_ITERATIONS_MIN: usize = 1;
const CTE_MAX_ITERATIONS_MAX: usize = 10_000;
const CTE_MAX_ROWS_MIN: usize = 1;
const CTE_MAX_ROWS_MAX: usize = 5_000_000;
const CTE_TIMEOUT_MS_MIN: u64 = 0;
const CTE_TIMEOUT_MS_MAX: u64 = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VariableScope {
    Global,
    Session,
    Local,
}

impl VariableScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Session => "session",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariableMutability {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VariableVisibility {
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Copy)]
struct VariableDefinition {
    name: &'static str,
    mutability: VariableMutability,
    visibility: VariableVisibility,
    supported_scopes: &'static [VariableScope],
}

const SESSION_LOCAL_SCOPES: &[VariableScope] = &[VariableScope::Session, VariableScope::Local];

const VARIABLE_DEFINITIONS: &[VariableDefinition] = &[
    VariableDefinition {
        name: "cte.max_iterations",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
    },
    VariableDefinition {
        name: "cte.max_rows",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
    },
    VariableDefinition {
        name: "cte.timeout_ms",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
    },
    VariableDefinition {
        name: "cte.union_all_repeat_detection",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
    },
];

pub(super) fn normalize_variable_name(input: &str) -> String {
    parse_scoped_variable_target(input).1
}

pub(super) fn parse_statement_scope_prefix(assignments_sql: &str) -> Option<(VariableScope, usize)> {
    for (keyword, scope) in [
        ("global", VariableScope::Global),
        ("session", VariableScope::Session),
        ("local", VariableScope::Local),
    ] {
        if assignments_sql
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && assignments_sql
                .chars()
                .nth(keyword.len())
                .is_some_and(char::is_whitespace)
        {
            return Some((scope, keyword.len()));
        }
    }

    None
}

pub(super) fn parse_scoped_variable_target(input: &str) -> (Option<VariableScope>, String) {
    let normalized = input
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .trim_matches(';')
        .to_ascii_lowercase();

    for (prefix, scope) in [
        ("@@global.", VariableScope::Global),
        ("@@session.", VariableScope::Session),
        ("@@local.", VariableScope::Local),
        ("global.", VariableScope::Global),
        ("session.", VariableScope::Session),
        ("local.", VariableScope::Local),
    ] {
        if let Some(stripped) = normalized.strip_prefix(prefix) {
            return (Some(scope), stripped.to_string());
        }
    }

    if let Some(stripped) = normalized.strip_prefix("@@") {
        return (None, stripped.to_string());
    }

    (None, normalized)
}

fn resolve_variable_definition(variable_name: &str) -> Option<&'static VariableDefinition> {
    VARIABLE_DEFINITIONS
        .iter()
        .find(|definition| definition.name == variable_name)
}

fn validate_variable_access(
    variable_name: &str,
    scope: VariableScope,
    require_mutability: Option<VariableMutability>,
) -> Result<&'static VariableDefinition, String> {
    let Some(definition) = resolve_variable_definition(variable_name) else {
        return Err(format!("unsupported variable '{variable_name}'"));
    };

    if let Some(required_mutability) = require_mutability
        && definition.mutability != required_mutability
    {
        return Err(format!("variable '{variable_name}' is read-only"));
    }

    if !definition.supported_scopes.contains(&scope) {
        return Err(format!(
            "variable '{variable_name}' does not support '{}' scope",
            scope.as_str()
        ));
    }

    Ok(definition)
}

fn visible_variable_definitions() -> impl Iterator<Item = &'static VariableDefinition> {
    VARIABLE_DEFINITIONS
        .iter()
        .filter(|definition| definition.visibility == VariableVisibility::Visible)
}

pub(super) fn recursive_cte_variable_rows(catalog: &DatabaseCatalog) -> Vec<(String, String)> {

    let settings = catalog.recursive_cte_execution_settings();

    let mut rows = Vec::new();

    for definition in visible_variable_definitions() {
        let value = match definition.name {
            "cte.max_iterations" => settings.max_iterations.to_string(),
            "cte.max_rows" => settings.max_rows.to_string(),
            "cte.timeout_ms" => settings.timeout_ms.to_string(),
            "cte.union_all_repeat_detection" => {
                settings.detect_repeating_union_all_frontier.to_string()
            }
            _ => continue,
        };

        rows.push((definition.name.to_string(), value));
    }

    rows
    
}

pub(super) fn apply_variable_assignment(
    next_settings: &mut serverlib::RecursiveCteExecutionSettings,
    variable_name: &str,
    variable_value: &str,
    scope: VariableScope,
) -> Result<(), String> {

    validate_variable_access(
        variable_name,
        scope,
        Some(VariableMutability::ReadWrite),
    )?;

    match variable_name {

        "cte.max_iterations" => {
            next_settings.max_iterations = parse_usize_in_range(
                "cte.max_iterations",
                variable_value,
                CTE_MAX_ITERATIONS_MIN,
                CTE_MAX_ITERATIONS_MAX,
            )?;
            Ok(())
        },

        "cte.max_rows" => {
            next_settings.max_rows = parse_usize_in_range(
                "cte.max_rows",
                variable_value,
                CTE_MAX_ROWS_MIN,
                CTE_MAX_ROWS_MAX,
            )?;
            Ok(())
        },

        "cte.timeout_ms" => {
            next_settings.timeout_ms = parse_u64_in_range(
                "cte.timeout_ms",
                variable_value,
                CTE_TIMEOUT_MS_MIN,
                CTE_TIMEOUT_MS_MAX,
            )?;
            Ok(())
        },

        "cte.union_all_repeat_detection" => {
            next_settings.detect_repeating_union_all_frontier =
                parse_boolean_value("cte.union_all_repeat_detection", variable_value)?;
            Ok(())
        },

        _ => Err(format!("unsupported variable '{variable_name}'")),

    }

}

fn parse_usize_in_range(
    variable_name: &str,
    variable_value: &str,
    min_value: usize,
    max_value: usize,
) -> Result<usize, String> {

    let parsed = variable_value.parse::<usize>().map_err(|_| {
        format!(
            "{variable_name} expects an integer in range [{min_value}, {max_value}], received '{variable_value}'"
        )
    })?;

    if parsed < min_value || parsed > max_value {
        return Err(format!(
            "{variable_name} is out of allowed range [{min_value}, {max_value}], received '{variable_value}'"
        ));
    }

    Ok(parsed)
}

fn parse_u64_in_range(
    variable_name: &str,
    variable_value: &str,
    min_value: u64,
    max_value: u64,
) -> Result<u64, String> {

    let parsed = variable_value.parse::<u64>().map_err(|_| {
        format!(
            "{variable_name} expects an integer in range [{min_value}, {max_value}], received '{variable_value}'"
        )
    })?;

    if parsed < min_value || parsed > max_value {
        return Err(format!(
            "{variable_name} is out of allowed range [{min_value}, {max_value}], received '{variable_value}'"
        ));
    }

    Ok(parsed)
}

fn parse_boolean_value(variable_name: &str, input: &str) -> Result<bool, String> {

    match input.trim().to_ascii_lowercase().as_str() {

        "1" | "true" | "on" => Ok(true),

        "0" | "false" | "off" => Ok(false),

        other => Err(format!(
            "{variable_name} expects true/false (or 1/0), received '{other}'"
        )),

    }
    
}