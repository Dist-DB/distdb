use super::*;

pub(crate) type SessionVariableOverrides = HashMap<String, String>;

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
    value_kind: VariableValueKind,
}

#[derive(Debug, Clone, Copy)]
enum VariableValueKind {
    UsizeRange { min: usize, max: usize },
    U64Range { min: u64, max: u64 },
    Bool,
}

#[derive(Debug, Clone, Copy)]
enum ParsedVariableValue {
    Usize(usize),
    U64(u64),
    Bool(bool),
}

impl ParsedVariableValue {
    fn as_string(self) -> String {
        match self {
            Self::Usize(value) => value.to_string(),
            Self::U64(value) => value.to_string(),
            Self::Bool(value) => value.to_string(),
        }
    }
}

const SESSION_LOCAL_SCOPES: &[VariableScope] = &[VariableScope::Session, VariableScope::Local];

const VARIABLE_DEFINITIONS: &[VariableDefinition] = &[
    VariableDefinition {
        name: "cte.max_iterations",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
        value_kind: VariableValueKind::UsizeRange {
            min: CTE_MAX_ITERATIONS_MIN,
            max: CTE_MAX_ITERATIONS_MAX,
        },
    },
    VariableDefinition {
        name: "cte.max_rows",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
        value_kind: VariableValueKind::UsizeRange {
            min: CTE_MAX_ROWS_MIN,
            max: CTE_MAX_ROWS_MAX,
        },
    },
    VariableDefinition {
        name: "cte.timeout_ms",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
        value_kind: VariableValueKind::U64Range {
            min: CTE_TIMEOUT_MS_MIN,
            max: CTE_TIMEOUT_MS_MAX,
        },
    },
    VariableDefinition {
        name: "cte.union_all_repeat_detection",
        mutability: VariableMutability::ReadWrite,
        visibility: VariableVisibility::Visible,
        supported_scopes: SESSION_LOCAL_SCOPES,
        value_kind: VariableValueKind::Bool,
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

fn session_user_id_for_acl(session_user: Option<&str>) -> Option<String> {

    let raw = session_user?.trim();
    if raw.is_empty() {
        return None;
    }

    let user_id = raw
        .split('@')
        .next()
        .unwrap_or(raw)
        .trim()
        .to_ascii_lowercase();

    if user_id.is_empty() {
        None
    } else {
        Some(user_id)
    }

}

fn has_variable_read_access(catalog: &DatabaseCatalog, session_user: Option<&str>) -> bool {

    let Some(user_id) = session_user_id_for_acl(session_user) else {
        return true;
    };

    if user_id.eq_ignore_ascii_case("root") {
        return true;
    }

    let Some(acl_entry) = catalog.effective_account_acl_entry(&user_id) else {
        return false;
    };

    [
        serverlib::engine::security::AccountPrivilege::SystemVariablesAdmin,
        serverlib::engine::security::AccountPrivilege::SessionVariablesAdmin,
        serverlib::engine::security::AccountPrivilege::SensitiveVariablesObserver,
    ]
    .iter()
    .any(|privilege| acl_entry.has_privilege_for_object(*privilege, None))

}

fn variable_value_by_name(
    settings: &serverlib::RecursiveCteExecutionSettings,
    variable_name: &str,
) -> Option<String> {

    match variable_name {

        "cte.max_iterations"    => Some(settings.max_iterations.to_string()),

        "cte.max_rows"          => Some(settings.max_rows.to_string()),

        "cte.timeout_ms"        => Some(settings.timeout_ms.to_string()),

        "cte.union_all_repeat_detection" => Some(settings.detect_repeating_union_all_frontier.to_string()),

        _ => None,

    }

}

pub(super) fn readable_variable_rows(
    catalog: &DatabaseCatalog,
    session_variable_overrides: Option<&SessionVariableOverrides>,
    session_user: Option<&str>,
) -> Vec<(String, String)> {
    if !has_variable_read_access(catalog, session_user) {
        return Vec::new();
    }

    let settings = effective_recursive_cte_execution_settings(catalog, session_variable_overrides);

    visible_variable_definitions()
        .filter_map(|definition| {
            variable_value_by_name(&settings, definition.name)
                .map(|value| (definition.name.to_string(), value))
        })
        .collect()
}

pub(super) fn runtime_variable_bindings(
    catalog: &DatabaseCatalog,
    session_variable_overrides: Option<&SessionVariableOverrides>,
    session_user: Option<&str>,
) -> HashMap<String, Vec<u8>> {
    
    let mut bindings = HashMap::new();

    for (name, value) in readable_variable_rows(catalog, session_variable_overrides, session_user) {
        
        let bytes = value.into_bytes();

        bindings.insert(name.clone(), bytes.clone());
        bindings.insert(format!("@@session.{name}"), bytes.clone());
        bindings.insert(format!("@@global.{name}"), bytes.clone());

        if let Some((_, short_name)) = name.rsplit_once('.') {
            bindings.entry(short_name.to_string()).or_insert(bytes.clone());
        }

    }

    bindings

}

pub(super) fn recursive_cte_variable_rows(catalog: &DatabaseCatalog) -> Vec<(String, String)> {

    readable_variable_rows(catalog, None, None)

}

pub(super) fn recursive_cte_variable_rows_with_session(
    catalog: &DatabaseCatalog,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> Vec<(String, String)> {

    readable_variable_rows(catalog, session_variable_overrides, None)
    
}

pub(super) fn effective_recursive_cte_execution_settings(
    catalog: &DatabaseCatalog,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> serverlib::RecursiveCteExecutionSettings {

    let mut settings = catalog.recursive_cte_execution_settings().clone();

    let Some(overrides) = session_variable_overrides else {
        return settings;
    };

    for (variable_name, variable_value) in overrides {
        if let Err(message) = apply_variable_assignment(
            &mut settings,
            variable_name,
            variable_value,
            VariableScope::Session,
        ) {
            log::warn!(
                "ignoring invalid session variable override name={} value={} reason={}",
                variable_name,
                variable_value,
                message,
            );
        }
    }

    settings

}

pub(super) fn apply_session_variable_assignment(
    session_variable_overrides: &mut SessionVariableOverrides,
    variable_name: &str,
    variable_value: &str,
    scope: VariableScope,
) -> Result<(), String> {

    let definition = validate_variable_access(
        variable_name,
        scope,
        Some(VariableMutability::ReadWrite),
    )?;

    let normalized_value = parse_variable_value(definition, variable_value)?.as_string();

    session_variable_overrides.insert(variable_name.to_string(), normalized_value);
    Ok(())

}

pub(super) fn apply_variable_assignment(
    next_settings: &mut serverlib::RecursiveCteExecutionSettings,
    variable_name: &str,
    variable_value: &str,
    scope: VariableScope,
) -> Result<(), String> {

    let definition = validate_variable_access(
        variable_name,
        scope,
        Some(VariableMutability::ReadWrite),
    )?;

    let parsed_value = parse_variable_value(definition, variable_value)?;
    apply_parsed_variable_value(next_settings, variable_name, parsed_value)

}

fn parse_variable_value(
    definition: &VariableDefinition,
    variable_value: &str,
) -> Result<ParsedVariableValue, String> {

    match definition.value_kind {

        VariableValueKind::UsizeRange { min, max } => Ok(ParsedVariableValue::Usize(
            parse_usize_in_range(definition.name, variable_value, min, max)?,
        )),
        
        VariableValueKind::U64Range { min, max } => Ok(ParsedVariableValue::U64(
            parse_u64_in_range(definition.name, variable_value, min, max)?,
        )),
        
        VariableValueKind::Bool => Ok(ParsedVariableValue::Bool(parse_boolean_value(
            definition.name,
            variable_value,
        )?)),

    }
    
}

fn apply_parsed_variable_value(
    next_settings: &mut serverlib::RecursiveCteExecutionSettings,
    variable_name: &str,
    parsed_value: ParsedVariableValue,
) -> Result<(), String> {

    match (variable_name, parsed_value) {

        ("cte.max_iterations", ParsedVariableValue::Usize(value)) => {
            next_settings.max_iterations = value;
            Ok(())
        },

        ("cte.max_rows", ParsedVariableValue::Usize(value)) => {
            next_settings.max_rows = value;
            Ok(())
        },

        ("cte.timeout_ms", ParsedVariableValue::U64(value)) => {
            next_settings.timeout_ms = value;
            Ok(())
        },

        ("cte.union_all_repeat_detection", ParsedVariableValue::Bool(value)) => {
            next_settings.detect_repeating_union_all_frontier = value;
            Ok(())
        },

        _ => Err(format!("unsupported variable '{variable_name}'")),

    }

}

fn value_for_definition_from_settings(
    definition: &VariableDefinition,
    settings: &serverlib::RecursiveCteExecutionSettings,
) -> Option<String> {

    match definition.name {

        "cte.max_iterations" => Some(settings.max_iterations.to_string()),

        "cte.max_rows" => Some(settings.max_rows.to_string()),

        "cte.timeout_ms" => Some(settings.timeout_ms.to_string()),

        "cte.union_all_repeat_detection" => {
            Some(settings.detect_repeating_union_all_frontier.to_string())
        },

        _ => None,

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