pub mod access;
pub mod introspection;
pub mod mutation;
pub mod join;
pub mod runtime;
pub mod select;
pub mod commands;


#[cfg(test)]
mod mutation_test;

pub use access::{
    build_relation_probe_index, choose_index_lookup, collect_indexable_equality_filters,
    count_condition_predicates, field_has_single_column_index, load_live_rows,
    materialize_relation_rows, plan_relation_access, EqualityProbeSource,
    RelationAccessPlan, RelationAccessStrategy,
};

pub use join::build_joined_row_tuples;
pub use introspection::{describe_table_result, show_databases_result, show_tables_result};
pub use mutation::select_mutation_target_rows;
pub use commands::{
    condition_matches_provider, evaluate_case_projection, execute_if_else_end_block,
    execute_if_else_end_from_create_procedure_sql, execute_if_else_end_plan,
    execute_sql_cursor,
    execute_automatic_triggers_for_event, execute_stored_procedure_invocation,
    execute_stored_procedure_invocation_over_cursor,
    create_scoped_ephemeral_table, release_scoped_ephemeral_table,
    execute_trigger_invocation, EntityInvocationSource, ControlFlowBranch,
    CursorDiagnostics, CursorDirective, IfElseEndBlock,
    ScopedEphemeralTableHandle,
    SelectReadPlanCursorSource,
    SqlCursorFrame, SqlCursorSource, VecSqlCursorSource,
};
pub use select::{
    execute_joined_select_plan, execute_projection_only_select_plan,
    execute_relation_select_plan, explain_joined_select_plan_result,
    explain_select_plan_result, row_matches_select_condition,
    row_matches_select_condition_result,
    SelectExecutionResult,
};

pub use runtime::{
    compare_provider_fields, join_condition_field_names, join_condition_matches_provider,
    relation_qualifier, row_matches_condition_with, row_matches_condition_with_result,
    ConditionValueProvider,
    JoinedRowCandidateProvider, JoinedRowMember, JoinedRowTuple, MaterializedRelationRow,
};
pub use crate::engine::sql::compare_row_value;
