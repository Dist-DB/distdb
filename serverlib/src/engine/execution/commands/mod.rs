mod select_execute;
mod select_explain;
mod mutation_select;
mod control_flow;
mod invocation;
mod scoped_table;
mod procedure_local_entity;

pub use select_execute::{
	execute_joined_select_plan, execute_projection_only_select_plan,
	execute_relation_select_plan,
};

pub use select_explain::{
	advise_select_execution, explain_joined_select_plan_result,
	explain_select_plan_result, SelectExecutionAdvice,
};

pub use mutation_select::select_mutation_target_rows;

pub use control_flow::{
	condition_matches_provider, evaluate_case_projection, execute_if_else_end_block,
	execute_if_else_end_from_create_procedure_sql, execute_if_else_end_plan,
	execute_local_loop_block,
	execute_local_repeat_block, execute_local_while_block,
	execute_sql_cursor,
	LoopControlDirective,
	ControlFlowBranch, CursorDiagnostics, CursorDirective,
	IfElseEndBlock, SelectReadPlanCursorSource,
	SqlCursorFrame, SqlCursorSource, VecSqlCursorSource,
};

pub use invocation::{
	execute_automatic_triggers_for_event, execute_stored_procedure_invocation,
	execute_stored_procedure_invocation_over_cursor,
	execute_stored_procedure_invocation_with_scoped_teardown,
	execute_stored_procedure_invocation_over_cursor_with_scoped_teardown,
	execute_trigger_invocation, EntityInvocationSource,
};

pub use scoped_table::{
	create_scoped_ephemeral_table, release_scoped_ephemeral_table,
	ScopedEphemeralTableHandle, ScopedEphemeralTableScope,
};

pub use procedure_local_entity::{
	ProcedureLocalEntity, ProcedureLocalEntityScope,
	RoutineLocalEntity, RoutineLocalEntityScope,
};
