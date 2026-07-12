
/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
	See the GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>.
	
	This libary provides the core server-side types and logic for DistDB, 
	including database entities, execution plans, schema management, and 
	replication. It is used by the DistDB server to manage databases, execute queries, 
	and handle replication and affinity. It is not intended 
	for use by client applications, which should use the public 
	API provided by the `distdb-client` crate.

	This library is distributed under the GNU Affero General Public License v3.0. See 
	the LICENSE file in the project root for more information.

	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/


#![allow(dead_code)]
#![allow(unused_imports)]

pub mod core;
pub mod engine;
pub mod helpers;
pub mod security;

pub use core::config::NodeConfig;
pub use core::identity::{NodeId, UserId};
pub use engine::security::{AccountAclEntry, RoleGrant, UserCredential};
pub use common::schema::{
	normalize_field_name, validate_field_kind, FieldIndex, FieldKind, SchemaValidationError,
};

pub use engine::database::core::{
	DatabaseCatalog, DatabaseEntity, DatabaseError, DatabaseId, DatabaseIndex,
	DatabaseIndexKind, DatabaseIndexOrigin,
	DatabaseEntityAspect, DatabaseEntityKind, DatabaseRelationship, DatabaseReplicaState,
	DatabaseResult, DatabaseStoredProcedure, DatabaseTable, DatabaseTrigger,
	EntityMetadata,
	decode_encrypted_row_payload_envelope, decode_row_field_value, decode_row_payload,
	encode_encrypted_row_payload_envelope, encode_row_payload,
	AesGcmRowPayloadCryptoProvider,
	EncryptedRowPayloadEnvelope, EncryptedRowPayloadTransform,
	RowPayloadDecryptionProvider, RowPayloadDecryptionTransform,
	RowPayloadEncryptionProvider, RowPayloadEncryptionWriteTransform,
	EncryptedRowPayloadTransformPolicy, ENCRYPTED_ROW_PAYLOAD_ENVELOPE_VERSION,
	UnconfiguredRowPayloadDecryptionProvider, UnconfiguredRowPayloadEncryptionProvider,
	render_stored_field_value, display_stored_field_value, compare_stored_field_values,
	DatabaseObjectRef, DatabaseObjectType,
	DatabaseView, IndexId, ObjectStatus,
	DiskToMemorySchemaMigrationExecutor, FieldTypeChangeRule, NoopSchemaMigrationExecutor,
	run_schema_migration, SchemaMigrationExecutor, SchemaMigrationProgress, SchemaMutationRuleSet,
	TypeConversionPolicy,
};
pub use engine::ir_compiler::{
	RoutineDeclaration, RoutineKind,
	SQLProgramaticAnalysisArtifact, SQLProgramaticCompilationArtifact,
	SQLProgramaticCompilationTarget, SQLProgramaticCompiler,
	SQLProgramaticCompilerContext, SQLProgramaticCompilerServices,
	SQLProgramaticDeclaration, SQLProgramaticIr, SQLProgramaticKind,
	SQLProgramaticResourceDirection, SQLProgramaticResourceEntry,
	SQLProgramaticResourceKind, SQLProgramaticResourceManifest,
	SQLProgramaticResultSetShape,
	StoredProcedureIr,
	StoredProcedureCompiler, StoredProcedureCompilerContext,
	StoredProcedureCompilerServices, StoredProcedureAnalysisArtifact,
	StoredProcedureCompilationArtifact,
	StoredProcedureResourceDirection, StoredProcedureResourceEntry,
	StoredProcedureResourceKind, StoredProcedureResourceManifest,
	StoredProcedureResultSetShape,
	analyze_sql_programatic_sql, analyze_sql_programatic_sql_with_context,
	analyze_sql_programatic_sql_with_services, compile_sql_programatic_artifact,
	compile_and_validate_sql_programatic_function_artifact_with_context,
	compile_and_validate_sql_programatic_procedure_artifact_with_context,
	compile_sql_programatic_artifact_with_context,
	compile_sql_programatic_artifact_with_services, compile_sql_programatic_sql,
	compile_sql_programatic_sql_with_context,
	compile_sql_programatic_sql_with_services,
	format_sql_programatic_resource_manifest,
	DefaultSQLProgramaticCompilerServices, DefaultStoredProcedureCompilerServices,
	SQLProgramaticInboundBinding, SQLProgramaticInboundParameter,
	SQLProgramaticValidationIssue, SQLProgramaticValidationResult,
	sql_programatic_resource_set_by_direction,
	validate_sql_programatic_function_artifact,
	validate_sql_programatic_procedure_artifact,
};

pub use engine::database::runtime_index::{index_value_tuple, primary_key_index, RuntimeIndexStore};
pub use engine::execution::{
	build_joined_row_tuples, build_relation_probe_index, choose_index_lookup,
	collect_indexable_equality_filters, compare_row_value, count_condition_predicates,
	collect_indexable_equality_filters_for_schema,
	collect_indexable_prefix_like_filter_for_schema,
	collect_indexable_like_filter_for_schema,
		describe_sql_object_result,
		describe_table_result,
	condition_matches_provider, evaluate_case_projection,
	execute_if_else_end_block, execute_if_else_end_from_create_procedure_sql,
	execute_if_else_end_plan,
	execute_local_loop_block,
	execute_local_repeat_block, execute_local_while_block,
	execute_sql_cursor,
	execute_automatic_triggers_for_event, execute_stored_procedure_invocation,
	execute_stored_procedure_invocation_over_cursor,
	execute_stored_procedure_invocation_with_scoped_teardown,
	execute_stored_procedure_invocation_over_cursor_with_scoped_teardown,
	create_scoped_ephemeral_table, release_scoped_ephemeral_table,
	execute_trigger_invocation, EntityInvocationSource,
	select_mutation_target_rows,
	execute_sql_function_with_lookup,
	execute_joined_select_plan, execute_projection_only_select_plan,
	execute_relation_select_plan, explain_joined_select_plan_result, explain_select_plan_result,
	advise_select_execution, SelectExecutionAdvice,
	field_has_single_column_index, join_condition_field_names, join_condition_matches_provider,
	load_live_row_count, load_live_rows, load_live_rows_with_context, warm_equality_cache_from_live_rows, materialize_relation_rows, plan_relation_access, relation_qualifier,
	snapshot_equality_cache, restore_equality_cache_from_snapshot, EqualityTableCacheSnapshot,
	apply_equality_cache_row_mutation, apply_equality_cache_row_mutation_batch,
	row_matches_condition_with, row_matches_select_condition, ConditionValueProvider,
	ControlFlowBranch, CursorDiagnostics, CursorDirective,
	IfElseEndBlock, SelectReadPlanCursorSource,
	LoopControlDirective,
	ProcedureLocalEntity, ProcedureLocalEntityScope,
	RoutineLocalEntity, RoutineLocalEntityScope,
	ScopedEphemeralTableHandle, ScopedEphemeralTableScope,
	SqlCursorFrame, SqlCursorSource, VecSqlCursorSource,
	EqualityProbeSource,
	JoinedRowCandidateProvider, JoinedRowMember, JoinedRowTuple, MaterializedRelationRow,
		RelationAccessPlan, RelationAccessStrategy, SelectExecutionResult, show_databases_result,
		show_indexes_result, show_privileges_result, show_tables_result,
};

pub use engine::database::table::schema::{FieldDef, FieldType, SchemaError, SchemaResult, TableSchema};

pub use engine::database::transaction::
	{EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
	SqlObjectKind, IndexLifecycleAction, IndexLifecyclePayload, TableLifecycleAction, TableLifecyclePayload,
	ChainedTransactionPayloadResolver, ChainedTransactionPayloadWriter,
	PlainTransactionPayloadResolver,
	TransactionId, TransactionKind, TransactionPayloadContext, TransactionPayloadResolver,
	TransactionPayloadTransform, TransactionPayloadWriteTransform, TransactionRecord,
	encode_wal_frame, decode_wal_frame,
};

pub use engine::sql::{
	parse_create_view_dependencies_from_sql, parse_create_view_dependencies_from_statement,
};

pub use engine::affinity::{
	AffinityDocument, AffinityMember, AffinityMemberStatus, AffinityProcessor,
	AffinityProcessorError, AffinityProcessorState, AffinitySyncPhase, AffinitySyncStep,
	CheckpointMetadata, DatabaseSchemaSummary, ReplicationSecuritySummary,
};

pub use engine::affinity::storage::AffinityStorage;
pub use engine::replication_executor::ReplicationPhaseExecutor;

pub use engine::database::inbuilt::{
	evaluate_inbuilt_sql_function,
	evaluate_inbuilt_sql_function_with_context,
	inbuilt_sql_runtime_context,
	is_inbuilt_function,
	registered_inbuilt_function_names,
	with_inbuilt_sql_runtime_context,
	InbuiltSqlRuntimeContext,
};

pub use engine::sql::{
	create_table_schema_from_statement, 
	create_table_plan_from_statement,
	parse_mysql8_sql_requests, 
	parse_sql_requests,
	parse_insert_rows_from_parsed_statement,
	parse_insert_rows_from_statement,
	parse_update_rows_from_statement,
	parse_delete_rows_from_statement,
	parse_select_read_plan_from_statement,
	parse_union_select_read_plans_from_statement,
	parse_if_else_end_plan_from_create_procedure_statement,
	parse_if_else_end_plan_from_statement,
	parse_create_function_return_type_from_statement,
	parse_create_procedure_parameter_declarations_from_statement,
	parse_create_procedure_parameter_names_from_statement,
	bind_call_procedure_argument_bindings,
	RoutineArgumentBinding, RoutineParameterDeclaration, RoutineParameterMode,
	with_lookup_sql_function_evaluator,
	bind_call_procedure_arguments,
	parse_select_projection_from_statement,
	parse_alter_table_change_plan_from_statement,
	sql_directive_for_statement,
	IfElseEndBranchPlan, IfElseEndPlan,
	InsertRowsPlan, InsertRowsSource,
	DeleteRowsPlan,
	SelectComparisonOp, SelectCondition, SelectCtePlan, SelectJoin, SelectJoinKind,
	SelectOrderByItem, SelectSetBoundaryOp, SelectSetQueryStep, SelectPredicate, SelectProjectionItem,
	SelectReadPlan, SelectRelation,
	TriggerEventKind, TriggerInvocationBinding, TriggerTiming,
	UpdateAssignment, UpdateRowsPlan,
	AlterTableChangeOp, AlterTableChangePlan,
	AclMutationKind, AclMutationPlan,
	SqlCompatibilityTarget, SqlDirective, SqlOperation, SqlParseError, SqlRequest,
};

pub use engine::wal::{ConcurrentWalManager, WalStreamMode};

pub use security::{
	AutoTlsPaths, TlsEnrollmentRequestMaterial, build_tls_enrollment_request,
	ensure_or_generate_p2p_tls, import_p2p_ca_pem_if_missing, install_signed_p2p_tls,
	load_p2p_ca_pem, sign_tls_enrollment_csr,
};

