use crate::engine::sql::{
    parse_if_else_end_plan_from_create_procedure_statement, parse_mysql8_sql_requests,
    parse_select_read_plan_from_statement, IfElseEndPlan, SelectProjectionItem, SelectReadPlan,
    SqlDirective, SqlOperation,
};
use crate::{is_inbuilt_function, registered_inbuilt_function_names};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoredProcedureResourceDirection {
    In,
    Out,
    Internal,
    Ref,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoredProcedureResourceKind {
    Variable,
    Table,
    Dependency,
    ResultSet,
    Function,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProcedureResourceEntry {
    pub name: String,
    pub kind: StoredProcedureResourceKind,
    pub direction: StoredProcedureResourceDirection,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SQLProgramaticInboundParameter {
    pub name: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredProcedureResourceManifest {
    entries: Vec<StoredProcedureResourceEntry>,
    by_name: BTreeMap<String, Vec<usize>>,
    by_scope: BTreeMap<(StoredProcedureResourceKind, StoredProcedureResourceDirection), Vec<usize>>,
    inbound_parameters: BTreeMap<String, Vec<u8>>,
}

impl StoredProcedureResourceManifest {

    pub fn from_entries(entries: Vec<StoredProcedureResourceEntry>) -> Self {
        Self::from_entries_with_inbound_parameters(entries, &[])
    }

    pub fn from_entries_with_inbound_parameters(
        entries: Vec<StoredProcedureResourceEntry>,
        inbound_parameters: &[SQLProgramaticInboundParameter],
    ) -> Self {
        
        let entries = normalize_compilation_resources(entries);
        let mut by_name: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut by_scope: BTreeMap<(StoredProcedureResourceKind, StoredProcedureResourceDirection), Vec<usize>> =
            BTreeMap::new();
        let mut inbound_parameter_map = BTreeMap::new();

        for (index, entry) in entries.iter().enumerate() {
            by_name
                .entry(entry.name.to_ascii_lowercase())
                .or_default()
                .push(index);
            by_scope
                .entry((entry.kind, entry.direction))
                .or_default()
                .push(index);
        }

        for parameter in inbound_parameters {
            inbound_parameter_map.insert(parameter.name.to_ascii_lowercase(), parameter.value.clone());
        }

        Self {
            entries,
            by_name,
            by_scope,
            inbound_parameters: inbound_parameter_map,
        }

    }

    pub fn entries(&self) -> &[StoredProcedureResourceEntry] {
        self.entries.as_slice()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, StoredProcedureResourceEntry> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&StoredProcedureResourceEntry> {
        self.entries.get(index)
    }

    pub fn find_by_name(&self, name: &str) -> Vec<&StoredProcedureResourceEntry> {
        self.by_name
            .get(&name.to_ascii_lowercase())
            .map(|indices| {
                indices
                    .iter()
                    .filter_map(|index| self.entries.get(*index))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn find_by_scope(
        &self,
        kind: StoredProcedureResourceKind,
        direction: StoredProcedureResourceDirection,
    ) -> Vec<&StoredProcedureResourceEntry> {
        self.by_scope
            .get(&(kind, direction))
            .map(|indices| {
                indices
                    .iter()
                    .filter_map(|index| self.entries.get(*index))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn format_for_debug(&self) -> String {
        format_sql_programatic_resource_manifest(self)
    }

    pub fn inbound_parameter(&self, name: &str) -> Option<&[u8]> {
        self.inbound_parameters
            .get(&name.to_ascii_lowercase())
            .map(Vec::as_slice)
    }

    pub fn inbound_parameters(&self) -> &BTreeMap<String, Vec<u8>> {
        &self.inbound_parameters
    }
    
}

impl AsRef<[StoredProcedureResourceEntry]> for StoredProcedureResourceManifest {
    fn as_ref(&self) -> &[StoredProcedureResourceEntry] {
        self.entries()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProcedureResultSetShape {
    pub source_sql: String,
    pub columns: Vec<String>,
    pub wildcard: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProcedureCompilationArtifact {
    pub ir: StoredProcedureIr,
    pub resources: StoredProcedureResourceManifest,
    pub result_sets: Vec<StoredProcedureResultSetShape>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProcedureAnalysisArtifact {
    pub if_else_end_plan: Option<IfElseEndPlan>,
    pub resources: StoredProcedureResourceManifest,
    pub result_sets: Vec<StoredProcedureResultSetShape>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineKind {
    Procedure,
    Function,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineDeclaration {
    pub kind: RoutineKind,
    pub name: Option<String>,
    pub return_type: Option<String>,
}

pub type SQLProgramaticCompilationTarget = RoutineDeclaration;
pub type SQLProgramaticDeclaration = RoutineDeclaration;
pub type SQLProgramaticKind = RoutineKind;

pub type SQLProgramaticResourceDirection = StoredProcedureResourceDirection;
pub type SQLProgramaticResourceKind = StoredProcedureResourceKind;
pub type SQLProgramaticResourceEntry = StoredProcedureResourceEntry;
pub type SQLProgramaticResourceManifest = StoredProcedureResourceManifest;
pub type SQLProgramaticInboundBinding = SQLProgramaticInboundParameter;
pub type SQLProgramaticResultSetShape = StoredProcedureResultSetShape;
pub type SQLProgramaticCompilationArtifact = StoredProcedureCompilationArtifact;
pub type SQLProgramaticAnalysisArtifact = StoredProcedureAnalysisArtifact;
pub type SQLProgramaticIr = StoredProcedureIr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SQLProgramaticValidationIssue {
    pub code: &'static str,
    pub message: String,
}

pub type SQLProgramaticValidationResult = Result<(), Vec<SQLProgramaticValidationIssue>>;

pub fn format_sql_programatic_resource_manifest(
    resources: impl AsRef<[SQLProgramaticResourceEntry]>,
) -> String {
    resources
        .as_ref()
        .iter()
        .map(|entry| {
            let detail = entry
                .detail
                .as_deref()
                .map(|value| format!(" [{value}]"))
                .unwrap_or_default();
            format!(
                "{:?}.{:?}: {}{}",
                entry.kind,
                entry.direction,
                entry.name,
                detail,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn sql_programatic_resource_set_by_direction(
    manifest: &SQLProgramaticResourceManifest,
    direction: SQLProgramaticResourceDirection,
) -> Vec<&SQLProgramaticResourceEntry> {
    manifest
        .iter()
        .filter(|entry| entry.direction == direction)
        .collect()
}

pub fn validate_sql_programatic_function_artifact(
    artifact: &SQLProgramaticCompilationArtifact,
) -> SQLProgramaticValidationResult {
    let mut issues = validate_sql_programatic_inbound_bindings(artifact);
    let outbound = sql_programatic_resource_set_by_direction(
        &artifact.resources,
        SQLProgramaticResourceDirection::Out,
    );

    if outbound.is_empty() {
        issues.push(SQLProgramaticValidationIssue {
            code: "FUNCTION_MISSING_OUT_SET",
            message: "function artifact has no outbound resource set".to_string(),
        });
    }

    let outbound_result_columns = outbound
        .iter()
        .filter(|entry| entry.kind == SQLProgramaticResourceKind::ResultSet)
        .count();

    if outbound_result_columns > 1 {
        issues.push(SQLProgramaticValidationIssue {
            code: "FUNCTION_MULTIPLE_OUT_COLUMNS",
            message: format!(
                "function artifact exposes {outbound_result_columns} outbound result columns; expected at most 1"
            ),
        });
    }

    if outbound
        .iter()
        .any(|entry| entry.kind == SQLProgramaticResourceKind::Table)
    {
        issues.push(SQLProgramaticValidationIssue {
            code: "FUNCTION_TABLE_SIDE_EFFECT",
            message: "function artifact emits outbound table resources; expected expression-like output only".to_string(),
        });
    }

    if issues.is_empty() {
        Ok(())
    } else {
        Err(issues)
    }
}

pub fn validate_sql_programatic_procedure_artifact(
    artifact: &SQLProgramaticCompilationArtifact,
) -> SQLProgramaticValidationResult {
    let issues = validate_sql_programatic_inbound_bindings(artifact);
    if issues.is_empty() {
        Ok(())
    } else {
        Err(issues)
    }
}

fn validate_sql_programatic_inbound_bindings(
    artifact: &SQLProgramaticCompilationArtifact,
) -> Vec<SQLProgramaticValidationIssue> {
    let mut issues = Vec::new();

    for binding_name in artifact.resources.inbound_parameters().keys() {
        let declared_inbound = artifact
            .resources
            .find_by_name(binding_name)
            .iter()
            .any(|entry| entry.direction == SQLProgramaticResourceDirection::In);

        if !declared_inbound {
            issues.push(SQLProgramaticValidationIssue {
                code: "INBOUND_BINDING_MISSING_IN_SET",
                message: format!(
                    "inbound binding '{binding_name}' is present in store but missing from inbound resource set"
                ),
            });
        }
    }

    issues
}

pub trait StoredProcedureCompilerServices {

    fn registered_inbuilt_function_names(&self) -> Vec<String> {
        registered_inbuilt_function_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect()
    }

    fn is_inbuilt_function(&self, function_name: &str) -> bool {
        is_inbuilt_function(function_name)
    }

    fn resolve_table_like_name(&self, name: &str) -> Option<String> {
        Some(name.to_string())
    }

}

pub type SQLProgramaticCompilerServices = dyn StoredProcedureCompilerServices;

#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultStoredProcedureCompilerServices;

#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultSQLProgramaticCompilerServices;

impl StoredProcedureCompilerServices for DefaultStoredProcedureCompilerServices {}
impl StoredProcedureCompilerServices for DefaultSQLProgramaticCompilerServices {}

pub struct StoredProcedureCompilerContext<'a, S: StoredProcedureCompilerServices + ?Sized> {
    services: &'a S,
    pub directive: Option<SqlDirective>,
    pub database_id: Option<&'a str>,
    pub routine: Option<RoutineDeclaration>,
    pub inbound_parameters: Vec<SQLProgramaticInboundParameter>,
}

pub type SQLProgramaticCompilerContext<'a, S> = StoredProcedureCompilerContext<'a, S>;

impl<'a, S> StoredProcedureCompilerContext<'a, S>
where
    S: StoredProcedureCompilerServices + ?Sized,
{

    pub fn new(services: &'a S) -> Self {
        Self {
            services,
            directive: None,
            database_id: None,
            routine: None,
            inbound_parameters: Vec::new(),
        }
    }

    pub fn with_directive(mut self, directive: Option<SqlDirective>) -> Self {
        self.directive = directive;
        self
    }

    pub fn with_database_id(mut self, database_id: Option<&'a str>) -> Self {
        self.database_id = database_id;
        self
    }

    pub fn with_routine(mut self, routine: Option<RoutineDeclaration>) -> Self {
        self.routine = routine;
        self
    }

    pub fn with_inbound_parameters(
        mut self,
        inbound_parameters: Vec<SQLProgramaticInboundParameter>,
    ) -> Self {
        self.inbound_parameters = inbound_parameters;
        self
    }

    pub fn with_inbound_parameter(
        mut self,
        name: impl Into<String>,
        value: Vec<u8>,
    ) -> Self {
        self.inbound_parameters.push(SQLProgramaticInboundParameter {
            name: name.into(),
            value,
        });
        self
    }

    pub fn services(&self) -> &'a S {
        self.services
    }

    pub fn directive(&self) -> Option<SqlDirective> {
        self.directive
    }

    pub fn database_id(&self) -> Option<&'a str> {
        self.database_id
    }

    pub fn routine(&self) -> Option<&RoutineDeclaration> {
        self.routine.as_ref()
    }

    pub fn inbound_parameters(&self) -> &[SQLProgramaticInboundParameter] {
        self.inbound_parameters.as_slice()
    }

    pub fn registered_inbuilt_function_names(&self) -> Vec<String> {
        self.services.registered_inbuilt_function_names()
    }

    pub fn is_inbuilt_function(&self, function_name: &str) -> bool {
        self.services.is_inbuilt_function(function_name)
    }

}

pub struct StoredProcedureCompiler<'a, S: StoredProcedureCompilerServices + ?Sized> {
    context: StoredProcedureCompilerContext<'a, S>,
}

pub type SQLProgramaticCompiler<'a, S> = StoredProcedureCompiler<'a, S>;

impl<'a, S> StoredProcedureCompiler<'a, S>
where
    S: StoredProcedureCompilerServices + ?Sized,
{

    pub fn new(services: &'a S) -> Self {
        Self {
            context: StoredProcedureCompilerContext::new(services),
        }
    }

    pub fn with_context(context: StoredProcedureCompilerContext<'a, S>) -> Self {
        Self { context }
    }

    pub fn registered_inbuilt_function_names(&self) -> Vec<String> {
        self.context.registered_inbuilt_function_names()
    }

    pub fn is_inbuilt_function(&self, function_name: &str) -> bool {
        self.context.is_inbuilt_function(function_name)
    }

    pub fn directive(&self) -> Option<SqlDirective> {
        self.context.directive()
    }

    pub fn database_id(&self) -> Option<&'a str> {
        self.context.database_id()
    }

    pub fn routine(&self) -> Option<&RoutineDeclaration> {
        self.context.routine()
    }

    pub fn compile(&self, sql: &str) -> StoredProcedureIr {
        self.lower_ir(sql)
    }

    pub fn analyze(&self, sql: &str) -> StoredProcedureAnalysisArtifact {

        // Pass 1: semantic analysis. Collect resources and output shape information
        // plus any lowered control-flow plan that can be reused by IR generation.
        let available_function_names = self.registered_inbuilt_function_names();
        let directive = self.directive();
        let database_id = self.database_id();
        let routine = self.routine().cloned();
        let inbound_parameters = self.context.inbound_parameters().to_vec();

        let if_else_end_plan = parse_if_else_end_plan_from_create_procedure_statement(sql)
            .ok()
            .flatten();

        let resources = collect_compilation_resources(
            sql,
            directive,
            database_id,
            routine.as_ref(),
            &inbound_parameters,
            &available_function_names,
            self.context.services(),
        );
        let resources = StoredProcedureResourceManifest::from_entries_with_inbound_parameters(
            resources,
            &inbound_parameters,
        );

        let result_sets = collect_compilation_result_sets(sql, self.context.services());

        StoredProcedureAnalysisArtifact {
            if_else_end_plan,
            resources,
            result_sets,
        }
    }

    pub fn lower_ir(&self, sql: &str) -> StoredProcedureIr {
        let analysis = self.analyze(sql);
        self.lower_ir_from_analysis(&analysis)
    }

    pub fn lower_ir_from_analysis(&self, analysis: &StoredProcedureAnalysisArtifact) -> StoredProcedureIr {
        analysis
            .if_else_end_plan
            .clone()
            .map(StoredProcedureIr::IfElseEnd)
            .unwrap_or(StoredProcedureIr::PassthroughSql)
    }

    pub fn compile_artifact(&self, sql: &str) -> StoredProcedureCompilationArtifact {

        // Pass 1 analyzes routine semantics and pass 2 lowers that analysis to IR.
        let analysis = self.analyze(sql);
        let ir = self.lower_ir_from_analysis(&analysis);

        StoredProcedureCompilationArtifact {
            ir,
            resources: analysis.resources,
            result_sets: analysis.result_sets,
        }

    }

}

fn normalize_compilation_resources(
    mut resources: Vec<StoredProcedureResourceEntry>,
) -> Vec<StoredProcedureResourceEntry> {
    
    resources.sort_by(|left, right| {

        let left_key = (
            resource_kind_sort_key(left.kind),
            resource_direction_sort_key(left.direction),
            left.name.to_ascii_lowercase(),
            left.detail
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase(),
            left.name.as_str(),
            left.detail.as_deref().unwrap_or_default(),
        );

        let right_key = (
            resource_kind_sort_key(right.kind),
            resource_direction_sort_key(right.direction),
            right.name.to_ascii_lowercase(),
            right.detail
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase(),
            right.name.as_str(),
            right.detail.as_deref().unwrap_or_default(),
        );

        left_key.cmp(&right_key)
        
    });

    resources.dedup_by(|left, right| {
        left.kind == right.kind
            && left.direction == right.direction
            && left.name == right.name
            && left.detail == right.detail
    });

    resources

}

fn resource_kind_sort_key(kind: StoredProcedureResourceKind) -> u8 {
    match kind {
        StoredProcedureResourceKind::Variable => 0,
        StoredProcedureResourceKind::Table => 1,
        StoredProcedureResourceKind::Dependency => 2,
        StoredProcedureResourceKind::ResultSet => 3,
        StoredProcedureResourceKind::Function => 4,
    }
}

fn resource_direction_sort_key(direction: StoredProcedureResourceDirection) -> u8 {
    match direction {
        StoredProcedureResourceDirection::In => 0,
        StoredProcedureResourceDirection::Out => 1,
        StoredProcedureResourceDirection::Internal => 2,
        StoredProcedureResourceDirection::Ref => 3,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredProcedureIr {
    IfElseEnd(IfElseEndPlan),
    PassthroughSql,
}

pub fn compile_sql_programatic_sql(sql: &str) -> SQLProgramaticIr {
    compile_sql_programatic_sql_with_services(sql, &DefaultSQLProgramaticCompilerServices)
}

pub fn compile_sql_programatic_artifact(sql: &str) -> SQLProgramaticCompilationArtifact {
    compile_sql_programatic_artifact_with_services(sql, &DefaultSQLProgramaticCompilerServices)
}

pub fn analyze_sql_programatic_sql(sql: &str) -> SQLProgramaticAnalysisArtifact {
    analyze_sql_programatic_sql_with_services(sql, &DefaultSQLProgramaticCompilerServices)
}

pub fn compile_sql_programatic_sql_with_services<S>(
    sql: &str,
    services: &S,
) -> SQLProgramaticIr
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::new(services).compile(sql)
}

pub fn compile_sql_programatic_artifact_with_services<S>(
    sql: &str,
    services: &S,
) -> SQLProgramaticCompilationArtifact
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::new(services).compile_artifact(sql)
}

pub fn analyze_sql_programatic_sql_with_services<S>(
    sql: &str,
    services: &S,
) -> SQLProgramaticAnalysisArtifact
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::new(services).analyze(sql)
}

pub fn compile_sql_programatic_sql_with_context<S>(
    sql: &str,
    context: SQLProgramaticCompilerContext<'_, S>,
) -> SQLProgramaticIr
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::with_context(context).compile(sql)
}

pub fn compile_sql_programatic_artifact_with_context<S>(
    sql: &str,
    context: SQLProgramaticCompilerContext<'_, S>,
) -> SQLProgramaticCompilationArtifact
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::with_context(context).compile_artifact(sql)
}

pub fn analyze_sql_programatic_sql_with_context<S>(
    sql: &str,
    context: SQLProgramaticCompilerContext<'_, S>,
) -> SQLProgramaticAnalysisArtifact
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    StoredProcedureCompiler::with_context(context).analyze(sql)
}

pub fn compile_and_validate_sql_programatic_function_artifact_with_context<S>(
    sql: &str,
    context: SQLProgramaticCompilerContext<'_, S>,
) -> Result<SQLProgramaticCompilationArtifact, Vec<SQLProgramaticValidationIssue>>
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    let artifact = compile_sql_programatic_artifact_with_context(sql, context);
    validate_sql_programatic_function_artifact(&artifact)?;
    Ok(artifact)
}

pub fn compile_and_validate_sql_programatic_procedure_artifact_with_context<S>(
    sql: &str,
    context: SQLProgramaticCompilerContext<'_, S>,
) -> Result<SQLProgramaticCompilationArtifact, Vec<SQLProgramaticValidationIssue>>
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    let artifact = compile_sql_programatic_artifact_with_context(sql, context);
    validate_sql_programatic_procedure_artifact(&artifact)?;
    Ok(artifact)
}

impl StoredProcedureIr {

    pub fn if_else_end_plan(&self) -> Option<&IfElseEndPlan> {
        match self {
            StoredProcedureIr::IfElseEnd(plan) => Some(plan),
            StoredProcedureIr::PassthroughSql => None,
        }
    }

    pub fn is_passthrough_sql(&self) -> bool {
        matches!(self, StoredProcedureIr::PassthroughSql)
    }

}

fn collect_compilation_resources<S>(
    sql: &str,
    directive: Option<SqlDirective>,
    database_id: Option<&str>,
    routine: Option<&RoutineDeclaration>,
    inbound_parameters: &[SQLProgramaticInboundParameter],
    function_names: &[String],
    services: &S,
) -> Vec<StoredProcedureResourceEntry>
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    
    let mut resources = Vec::new();

    if let Some(routine) = routine {
        resources.push(StoredProcedureResourceEntry {
            name: routine
                .name
                .clone()
                .unwrap_or_else(|| "<anonymous routine>".to_string()),
            kind: StoredProcedureResourceKind::Dependency,
            direction: StoredProcedureResourceDirection::Internal,
            detail: Some(match routine.kind {
                RoutineKind::Procedure => "routine declaration: procedure".to_string(),
                RoutineKind::Function => "routine declaration: function".to_string(),
            }),
        });

        if let Some(return_type) = &routine.return_type {
            resources.push(StoredProcedureResourceEntry {
                name: return_type.clone(),
                kind: StoredProcedureResourceKind::ResultSet,
                direction: StoredProcedureResourceDirection::Out,
                detail: Some("routine return contract".to_string()),
            });
        }
    }

    for parameter in inbound_parameters {
        resources.push(StoredProcedureResourceEntry {
            name: parameter.name.clone(),
            kind: StoredProcedureResourceKind::Variable,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("inbound parameter binding".to_string()),
        });
    }

    if let Some(db) = database_id {
        resources.push(StoredProcedureResourceEntry {
            name: db.to_string(),
            kind: StoredProcedureResourceKind::Dependency,
            direction: StoredProcedureResourceDirection::Internal,
            detail: Some("compiler database context".to_string()),
        });
    }

    if let Some(directive) = directive {
        resources.push(StoredProcedureResourceEntry {
            name: format!("{:?}", directive),
            kind: StoredProcedureResourceKind::Dependency,
            direction: StoredProcedureResourceDirection::Internal,
            detail: Some("issued directive".to_string()),
        });
    }

    let procedure_sql = sql.trim().trim_end_matches(';').trim();
    let lowered = procedure_sql.to_ascii_lowercase();

    if lowered.starts_with("create procedure") {
        if let Ok(plan) = parse_if_else_end_plan_from_create_procedure_statement(procedure_sql) {
            if let Some(plan) = plan {
                resources.extend(collect_resources_from_if_else_plan(&plan, services, function_names));
            }
        }
    }

    resources

}

fn collect_resources_from_if_else_plan<S>(
    plan: &IfElseEndPlan,
    services: &S,
    function_names: &[String],
) -> Vec<StoredProcedureResourceEntry>
where
    S: StoredProcedureCompilerServices + ?Sized,
{

    let mut resources = Vec::new();

    for branch in &plan.branches {
        resources.extend(collect_resources_from_sql(&branch.action_sql, services, function_names));
        resources.extend(collect_resources_from_condition(&branch.condition));
    }

    if let Some(else_sql) = &plan.else_action_sql {
        resources.extend(collect_resources_from_sql(else_sql, services, function_names));
    }

    resources

}

fn collect_resources_from_sql<S>(
    sql: &str,
    services: &S,
    function_names: &[String],
) -> Vec<StoredProcedureResourceEntry>
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    
    let mut resources = Vec::new();

    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return resources;
    }

    if let Ok(select_plan) = parse_select_read_plan_from_statement(trimmed) {
        resources.extend(collect_resources_from_select_plan(&select_plan));
        resources.extend(collect_builtin_function_references(trimmed, function_names));
        let _ = services;
        return resources;
    }

    if let Ok(requests) = parse_mysql8_sql_requests(trimmed, "main") {

        for request in requests {

            if let Some(object_name) = request.object_name.as_deref() {

                let direction = match request.directive {
                    
                    SqlDirective::Create | 
                    SqlDirective::AlterSchema => {
                        StoredProcedureResourceDirection::Out
                    },
                    
                    SqlDirective::Retrieve => StoredProcedureResourceDirection::In,
                    
                    _ => StoredProcedureResourceDirection::Internal,

                };

                resources.push(StoredProcedureResourceEntry {
                    name: object_name.to_string(),
                    kind: match request.operation {
                        
                        SqlOperation::CreateTable |
                        SqlOperation::DropTable |
                        SqlOperation::AlterTable => StoredProcedureResourceKind::Table,

                        SqlOperation::CreateStoredProcedure | SqlOperation::DropStoredProcedure => {
                            StoredProcedureResourceKind::Dependency
                        }

                        _ => StoredProcedureResourceKind::Dependency,

                    },
                    direction,
                    detail: Some(format!("{:?}/{:?}", request.directive, request.operation)),
                });

            }

            resources.extend(collect_builtin_function_references(trimmed, function_names));

        }

    }

    let _ = services;

    resources
    
}


fn collect_resources_from_select_plan(plan: &SelectReadPlan) -> Vec<StoredProcedureResourceEntry> {

    let mut resources = Vec::new();

    if !plan.table_id.is_empty() {
        resources.push(StoredProcedureResourceEntry {
            name: plan.table_id.clone(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("select source".to_string()),
        });
    }

    for relation in &plan.relations {
        resources.push(StoredProcedureResourceEntry {
            name: relation.table_id.clone(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: relation.alias.clone(),
        });
    }

    for join in &plan.joins {
        resources.push(StoredProcedureResourceEntry {
            name: join.relation.table_id.clone(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: join.relation.alias.clone(),
        });
    }

    resources.extend(collect_result_set_shape(plan));

    resources

}

fn collect_result_set_shape(plan: &SelectReadPlan) -> Vec<StoredProcedureResourceEntry> {
    
    let mut resources = Vec::new();

    for item in &plan.projection_items {
        
        match item {
            
            SelectProjectionItem::Column { output_name, .. } |
            SelectProjectionItem::InbuiltFunction { output_name, .. } |
            SelectProjectionItem::Case { output_name, .. } => {
                resources.push(StoredProcedureResourceEntry {
                    name: output_name.clone(),
                    kind: StoredProcedureResourceKind::ResultSet,
                    direction: StoredProcedureResourceDirection::Out,
                    detail: Some("projection column".to_string()),
                });
            },
            
            SelectProjectionItem::Wildcard { relation } => {
                resources.push(StoredProcedureResourceEntry {
                    name: relation.clone().unwrap_or_else(|| "*".to_string()),
                    kind: StoredProcedureResourceKind::ResultSet,
                    direction: StoredProcedureResourceDirection::Out,
                    detail: Some("wildcard projection".to_string()),
                });
            }

        }

    }

    resources

}

fn collect_resources_from_condition(
    condition: &crate::engine::sql::SelectCondition,
) -> Vec<StoredProcedureResourceEntry> {
    
    let mut resources = Vec::new();

    match condition {

        crate::engine::sql::SelectCondition::And(children) |
        crate::engine::sql::SelectCondition::Or(children) => {
            for child in children {
                resources.extend(collect_resources_from_condition(child));
            }
        },

        crate::engine::sql::SelectCondition::Not(child) => {
            resources.extend(collect_resources_from_condition(child));
        },

        crate::engine::sql::SelectCondition::Predicate(predicate) => {
            resources.extend(collect_resources_from_predicate(predicate));
        }

    }

    resources

}

fn collect_resources_from_predicate(
    predicate: &crate::engine::sql::SelectPredicate,
) -> Vec<StoredProcedureResourceEntry> {

    let mut resources = Vec::new();

    use crate::engine::sql::SelectPredicate::*;

    match predicate {

        Comparison { field_name, .. } |
        Like { field_name, .. } |
        Regex { field_name, .. } |
        InList { field_name, .. } |
        IsNull { field_name, .. } |
        InSubquery { field_name, .. } |
        ScalarSubqueryComparison { field_name, .. } |
        AnySubqueryComparison { field_name, .. } |
        AllSubqueryComparison { field_name, .. } => {
            resources.push(StoredProcedureResourceEntry {
                name: field_name.clone(),
                kind: StoredProcedureResourceKind::Variable,
                direction: StoredProcedureResourceDirection::Ref,
                detail: Some("condition reference".to_string()),
            });
        },

        FieldComparison { left_field_name, right_field_name, .. } => {
            for field_name in [left_field_name, right_field_name] {
                resources.push(StoredProcedureResourceEntry {
                    name: field_name.clone(),
                    kind: StoredProcedureResourceKind::Variable,
                    direction: StoredProcedureResourceDirection::Ref,
                    detail: Some("field comparison".to_string()),
                });
            }
        },

        Exists { subquery, .. } => {
            resources.extend(collect_resources_from_select_plan(subquery));
        }

    }

    resources

}

fn collect_builtin_function_references(
    sql: &str,
    function_names: &[String],
) -> Vec<StoredProcedureResourceEntry> {

    let lowered = sql.to_ascii_lowercase();

    function_names
        .iter()
        .filter(|name| lowered.contains(&format!("{}(", name.to_ascii_lowercase())))
        .map(|name| StoredProcedureResourceEntry {
            name: name.clone(),
            kind: StoredProcedureResourceKind::Function,
            direction: StoredProcedureResourceDirection::Internal,
            detail: Some("builtin reference".to_string()),
        })
        .collect()

}

fn collect_compilation_result_sets<S>(
    sql: &str,
    services: &S,
) -> Vec<StoredProcedureResultSetShape>
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    
    let mut result_sets = Vec::new();

    if let Ok(plan) = parse_if_else_end_plan_from_create_procedure_statement(sql) {
        if let Some(plan) = plan {

            for branch in plan.branches {
                collect_result_set_from_action_sql(&branch.action_sql, &mut result_sets, services);
            }

            if let Some(else_sql) = plan.else_action_sql {
                collect_result_set_from_action_sql(&else_sql, &mut result_sets, services);
            }

        }
    }

    result_sets

}

fn collect_result_set_from_action_sql<S>(
    action_sql: &str,
    result_sets: &mut Vec<StoredProcedureResultSetShape>,
    services: &S,
)
where
    S: StoredProcedureCompilerServices + ?Sized,
{
    
    let _ = services;
    
    if let Ok(plan) = parse_select_read_plan_from_statement(action_sql) {
        result_sets.push(StoredProcedureResultSetShape {
            source_sql: action_sql.to_string(),
            columns: plan
                .projection_items
                .iter()
                .map(|item| match item {

                    SelectProjectionItem::Column { output_name, .. } |
                    SelectProjectionItem::InbuiltFunction { output_name, .. } |
                    SelectProjectionItem::Case { output_name, .. } => output_name.clone(),

                    SelectProjectionItem::Wildcard { relation } => {
                        relation.clone().unwrap_or_else(|| "*".to_string())
                    }

                })
                .collect(),
            wildcard: plan.projection_is_wildcard,
        });
    }

}

#[cfg(test)]
#[path = "ir_compiler_test.rs"]
mod ir_compiler_test;
