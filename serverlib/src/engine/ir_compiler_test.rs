use super::{
    analyze_sql_programatic_sql,
    compile_sql_programatic_artifact, compile_sql_programatic_artifact_with_context,
    compile_sql_programatic_sql, compile_sql_programatic_sql_with_context,
    compile_sql_programatic_sql_with_services, DefaultSQLProgramaticCompilerServices,
    compile_and_validate_sql_programatic_function_artifact_with_context,
    compile_and_validate_sql_programatic_procedure_artifact_with_context,
    format_sql_programatic_resource_manifest, normalize_compilation_resources,
    SQLProgramaticInboundParameter,
    validate_sql_programatic_function_artifact,
    validate_sql_programatic_procedure_artifact,
    RoutineDeclaration, RoutineKind, StoredProcedureCompilationArtifact, StoredProcedureCompiler,
    StoredProcedureCompilerContext, StoredProcedureCompilerServices, StoredProcedureIr,
    StoredProcedureResourceManifest,
    StoredProcedureResourceDirection, StoredProcedureResourceEntry, StoredProcedureResourceKind,
};

use crate::SqlDirective;
use crate::execute_if_else_end_plan;
use std::collections::HashMap;

struct MockCompilerServices {
    functions: Vec<String>,
}

impl StoredProcedureCompilerServices for MockCompilerServices {

    fn registered_inbuilt_function_names(&self) -> Vec<String> {
        self.functions.clone()
    }

    fn is_inbuilt_function(&self, function_name: &str) -> bool {
        self.functions
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(function_name))
    }

}

#[test]
fn compiles_if_else_end_procedure_to_ir() {
    let ir = compile_sql_programatic_sql(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
    );

    assert!(matches!(ir, StoredProcedureIr::IfElseEnd(_)));
}

#[test]
fn compiles_non_if_else_procedure_to_passthrough_sql_ir() {
    let ir = compile_sql_programatic_sql("create procedure p() begin select 1; end");

    assert!(matches!(ir, StoredProcedureIr::PassthroughSql));
}

#[test]
fn compiler_exposes_service_registry_to_lowering_context() {
    let services = MockCompilerServices {
        functions: vec!["abs".to_string(), "now".to_string()],
    };

    let compiler = StoredProcedureCompiler::new(&services);

    assert!(compiler.is_inbuilt_function("ABS"));
    assert_eq!(compiler.registered_inbuilt_function_names(), services.functions);
}

#[test]
fn compile_with_services_still_lower_if_else_plans() {

    let services = DefaultSQLProgramaticCompilerServices;

    let ir = super::compile_sql_programatic_sql_with_services(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        &services,
    );

    assert!(matches!(ir, StoredProcedureIr::IfElseEnd(_)));
    
}

#[test]
fn compiler_context_exposes_directive_and_database_identity() {

    let services = MockCompilerServices {
        functions: vec!["abs".to_string()],
    };

    let context = StoredProcedureCompilerContext::new(&services)
        .with_directive(Some(SqlDirective::Create))
        .with_database_id(Some("main"));

    let compiler = StoredProcedureCompiler::with_context(context);

    assert_eq!(compiler.directive(), Some(SqlDirective::Create));
    assert_eq!(compiler.database_id(), Some("main"));
    assert!(compiler.is_inbuilt_function("abs"));

}

#[test]
fn compile_with_context_still_lower_if_else_plans() {

    let services = DefaultSQLProgramaticCompilerServices;

    let context = StoredProcedureCompilerContext::new(&services)
        .with_directive(Some(SqlDirective::Create))
        .with_database_id(Some("main"));

    let ir = compile_sql_programatic_sql_with_context(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        context,
    );

    assert!(matches!(ir, StoredProcedureIr::IfElseEnd(_)));

}

#[test]
fn compile_artifact_exposes_resource_manifest_and_result_sets() {

    let services = DefaultSQLProgramaticCompilerServices;
    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin if active = 1 then select abs(1) from users; else select email from users; end if; end",
        StoredProcedureCompilerContext::new(&services)
            .with_directive(Some(SqlDirective::Create))
            .with_database_id(Some("main")),
    );

    assert!(matches!(artifact.ir, StoredProcedureIr::IfElseEnd(_)));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Table
            && entry.direction == StoredProcedureResourceDirection::In
            && entry.name == "users"
    }));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Variable
            && entry.direction == StoredProcedureResourceDirection::Ref
            && entry.name == "active"
    }));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Function
            && entry.name.eq_ignore_ascii_case("abs")
    }));
    
    assert!(artifact.result_sets.iter().any(|shape| {
        shape.columns.iter().any(|column| column == "abs(1)") || !shape.columns.is_empty()
    }));

}

#[test]
fn compile_artifact_tracks_complex_select_dialect_inside_procedure_branches() {

    let services = DefaultSQLProgramaticCompilerServices;
    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin set @phase = 'pre'; select id from users limit 1; if active = 1 then select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1 group by u.email, p.name having u.email = 'x' order by u.email limit 10 offset 2; else select id from users order by id; end if; end",
        StoredProcedureCompilerContext::new(&services)
            .with_directive(Some(SqlDirective::Create))
            .with_database_id(Some("main")),
    );

    assert!(matches!(artifact.ir, StoredProcedureIr::IfElseEnd(_)));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Table
            && entry.direction == StoredProcedureResourceDirection::In
            && entry.name == "users"
    }));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Table
            && entry.direction == StoredProcedureResourceDirection::In
            && entry.name == "profiles"
    }));
    
    assert!(artifact.result_sets.len() >= 2);

    assert!(artifact
        .result_sets
        .iter()
        .all(|shape| !shape.columns.is_empty() || shape.wildcard));

}

#[test]
fn compile_artifact_detects_builtin_function_usage_in_branches() {

    let services = MockCompilerServices {
        functions: vec!["upper".to_string(), "lower".to_string()],
    };

    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin set @phase = 'runtime'; select id from users limit 1; if active = 1 then select upper('yes') as state from users; else select lower('NO') as state from users; end if; end",
        StoredProcedureCompilerContext::new(&services)
            .with_directive(Some(SqlDirective::Create))
            .with_database_id(Some("main")),
    );

    assert!(matches!(artifact.ir, StoredProcedureIr::IfElseEnd(_)));
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Function
            && entry.name.eq_ignore_ascii_case("upper")
    }));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Function
            && entry.name.eq_ignore_ascii_case("lower")
    }));

    assert!(artifact
        .result_sets
        .iter()
        .any(|shape| shape.columns.iter().any(|column| column == "state")));

}

#[test]
fn compile_with_context_lowers_case_plan_after_setup_statements() {

    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services)
        .with_directive(Some(SqlDirective::Create))
        .with_database_id(Some("main"));

    let ir = compile_sql_programatic_sql_with_context(
        "create procedure p() begin set @phase = 'pre'; select id from users limit 1; case active when 1 then select 'on'; else select 'off'; end case; end",
        context,
    );

    assert!(matches!(ir, StoredProcedureIr::IfElseEnd(_)));

}

#[test]
fn compile_with_context_setup_only_procedure_remains_passthrough() {

    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services)
        .with_directive(Some(SqlDirective::Create))
        .with_database_id(Some("main"));

    let ir = compile_sql_programatic_sql_with_context(
        "create procedure p() begin set @phase = 'pre'; select id from users limit 1; end",
        context,
    );

    assert!(matches!(ir, StoredProcedureIr::PassthroughSql));

}

#[test]
fn compile_routine_artifact_matches_procedure_compiler_entry_point() {
    let artifact = compile_sql_programatic_artifact("create procedure p() begin select 1; end");
    assert!(matches!(artifact.ir, StoredProcedureIr::PassthroughSql));
}

#[test]
fn compiler_context_can_carry_routine_shape_metadata() {

    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services).with_routine(Some(
        RoutineDeclaration {
            kind: RoutineKind::Function,
            name: Some("f_total".to_string()),
            return_type: Some("int".to_string()),
        },
    ));

    let artifact = StoredProcedureCompiler::with_context(context)
        .compile_artifact("create function f_total() returns int begin select 1; end");

    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Dependency
            && entry.name == "f_total"
            && entry.detail.as_deref() == Some("routine declaration: function")
    }));
    
    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::ResultSet
            && entry.direction == StoredProcedureResourceDirection::Out
            && entry.name == "int"
    }));

}

#[test]
fn two_pass_analysis_and_lowering_match_compile_output() {
    
    let sql = "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end";
    let services = DefaultSQLProgramaticCompilerServices;
    let compiler = StoredProcedureCompiler::new(&services);

    let analysis = analyze_sql_programatic_sql(sql);
    let lowered = compiler.lower_ir_from_analysis(&analysis);
    let compiled = compiler.compile(sql);

    assert_eq!(lowered, compiled);
    assert!(analysis.if_else_end_plan.is_some());

}

#[test]
fn normalize_compilation_resources_sorts_and_deduplicates_entries() {

    let resources = vec![
        StoredProcedureResourceEntry {
            name: "users".to_string(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("select source".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "active".to_string(),
            kind: StoredProcedureResourceKind::Variable,
            direction: StoredProcedureResourceDirection::Ref,
            detail: Some("condition ref".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "users".to_string(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("select source".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "abs".to_string(),
            kind: StoredProcedureResourceKind::Function,
            direction: StoredProcedureResourceDirection::Ref,
            detail: None,
        },
    ];

    let normalized = normalize_compilation_resources(resources);

    assert_eq!(normalized.len(), 3);
    assert_eq!(normalized[0].kind, StoredProcedureResourceKind::Variable);
    assert_eq!(normalized[0].name, "active");
    assert_eq!(normalized[1].kind, StoredProcedureResourceKind::Table);
    assert_eq!(normalized[1].name, "users");
    assert_eq!(normalized[2].kind, StoredProcedureResourceKind::Function);
    assert_eq!(normalized[2].name, "abs");

}

#[test]
fn resource_manifest_formatter_emits_uniform_debug_lines() {

    let resources = vec![
        StoredProcedureResourceEntry {
            name: "users".to_string(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("select source".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "active".to_string(),
            kind: StoredProcedureResourceKind::Variable,
            direction: StoredProcedureResourceDirection::Ref,
            detail: None,
        },
    ];

    let rendered = format_sql_programatic_resource_manifest(&resources);

    assert!(rendered.contains("Table.In: users [select source]"));
    assert!(rendered.contains("Variable.Ref: active"));

}

#[test]
fn resource_manifest_connectivity_exposes_name_and_scope_lookups() {

    let manifest = StoredProcedureResourceManifest::from_entries(vec![
        StoredProcedureResourceEntry {
            name: "users".to_string(),
            kind: StoredProcedureResourceKind::Table,
            direction: StoredProcedureResourceDirection::In,
            detail: Some("select source".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "users".to_string(),
            kind: StoredProcedureResourceKind::Dependency,
            direction: StoredProcedureResourceDirection::Ref,
            detail: Some("query text ref".to_string()),
        },
        StoredProcedureResourceEntry {
            name: "abs".to_string(),
            kind: StoredProcedureResourceKind::Function,
            direction: StoredProcedureResourceDirection::Ref,
            detail: None,
        },
    ]);

    let by_name = manifest.find_by_name("USERS");
    let by_scope = manifest.find_by_scope(
        StoredProcedureResourceKind::Function,
        StoredProcedureResourceDirection::Ref,
    );

    assert_eq!(by_name.len(), 2);
    assert!(by_name.iter().all(|entry| entry.name.eq_ignore_ascii_case("users")));
    assert_eq!(by_scope.len(), 1);
    assert_eq!(by_scope[0].name, "abs");

}

#[test]
fn sql_programatic_aliases_match_routine_compiler_behavior() {

    let sql = "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end";

    let sql_programatic_ir = compile_sql_programatic_sql(sql);
    let sql_programatic_artifact = compile_sql_programatic_artifact(sql);
    let sql_programatic_analysis = analyze_sql_programatic_sql(sql);

    assert!(matches!(sql_programatic_ir, StoredProcedureIr::IfElseEnd(_)));
    assert!(matches!(sql_programatic_artifact.ir, StoredProcedureIr::IfElseEnd(_)));
    assert!(sql_programatic_analysis.if_else_end_plan.is_some());

}

#[test]
fn inbound_parameters_are_added_to_manifest_and_indexed_for_lookup() {

    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services)
        .with_inbound_parameters(vec![
            SQLProgramaticInboundParameter {
                name: "active".to_string(),
                value: b"1".to_vec(),
            },
            SQLProgramaticInboundParameter {
                name: "tenant_id".to_string(),
                value: b"acme".to_vec(),
            },
        ]);

    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        context,
    );

    assert!(artifact.resources.iter().any(|entry| {
        entry.kind == StoredProcedureResourceKind::Variable
            && entry.direction == StoredProcedureResourceDirection::In
            && entry.name == "active"
            && entry.detail.as_deref() == Some("inbound parameter binding")
    }));
    assert_eq!(artifact.resources.inbound_parameter("active"), Some(&b"1"[..]));
    assert_eq!(artifact.resources.inbound_parameter("TENANT_ID"), Some(&b"acme"[..]));

}

#[test]
fn compiled_ir_executes_using_inbound_parameters_from_manifest_store() {

    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services)
        .with_inbound_parameter("active", b"1".to_vec());

    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        context,
    );

    let StoredProcedureIr::IfElseEnd(plan) = artifact.ir.clone() else {
        panic!("expected lowered IF/ELSE plan");
    };

    let provider = artifact.resources.inbound_parameters().iter().fold(
        HashMap::new(),
        |mut acc, (name, value)| {
            acc.insert(name.clone(), value.clone());
            acc
        },
    );

    let executed = execute_if_else_end_plan(&provider, &plan, &mut |sql| Ok(sql.to_string()))
        .expect("execution with inbound parameter provider should succeed");

    assert_eq!(executed.as_deref(), Some("select 'on'"));

}

#[test]
fn function_validation_rejects_multi_column_outbound_sets() {

    let services = DefaultSQLProgramaticCompilerServices;
    
    let context = StoredProcedureCompilerContext::new(&services)
        .with_routine(Some(RoutineDeclaration {
            kind: RoutineKind::Function,
            name: Some("f_invalid".to_string()),
            return_type: Some("int".to_string()),
        }))
        .with_inbound_parameter("active", b"1".to_vec());

    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin if active = 1 then select a, b from users; else select c, d from users; end if; end",
        context,
    );

    let validation = validate_sql_programatic_function_artifact(&artifact);
    assert!(validation.is_err());
    
    let issues = validation.expect_err("function validation should fail");
    assert!(issues
        .iter()
        .any(|issue| issue.code == "FUNCTION_MULTIPLE_OUT_COLUMNS"));

}

#[test]
fn procedure_validation_accepts_compiled_artifact_with_inbound_set() {
    let services = DefaultSQLProgramaticCompilerServices;
    let context = StoredProcedureCompilerContext::new(&services)
        .with_routine(Some(RoutineDeclaration {
            kind: RoutineKind::Procedure,
            name: Some("p_valid".to_string()),
            return_type: None,
        }))
        .with_inbound_parameter("active", b"1".to_vec());

    let artifact = compile_sql_programatic_artifact_with_context(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        context,
    );

    let validation = validate_sql_programatic_procedure_artifact(&artifact);
    assert!(validation.is_ok());
}

#[test]
fn compile_and_validate_helpers_enforce_routine_specific_rules() {
    let services = DefaultSQLProgramaticCompilerServices;

    let function_context = StoredProcedureCompilerContext::new(&services)
        .with_routine(Some(RoutineDeclaration {
            kind: RoutineKind::Function,
            name: Some("f_compile_validate".to_string()),
            return_type: Some("int".to_string()),
        }))
        .with_inbound_parameter("active", b"1".to_vec());

    let function_result = compile_and_validate_sql_programatic_function_artifact_with_context(
        "create procedure p() begin if active = 1 then select a, b from users; else select c, d from users; end if; end",
        function_context,
    );
    assert!(function_result.is_err());

    let procedure_context = StoredProcedureCompilerContext::new(&services)
        .with_routine(Some(RoutineDeclaration {
            kind: RoutineKind::Procedure,
            name: Some("p_compile_validate".to_string()),
            return_type: None,
        }))
        .with_inbound_parameter("active", b"1".to_vec());

    let procedure_result = compile_and_validate_sql_programatic_procedure_artifact_with_context(
        "create procedure p() begin if active = 1 then select 'on'; else select 'off'; end if; end",
        procedure_context,
    );
    assert!(procedure_result.is_ok());
}
