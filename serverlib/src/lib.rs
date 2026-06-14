#![allow(dead_code)]
#![allow(unused_imports)]

pub mod core;
pub mod engine;
pub mod helpers;
pub mod p2p;

pub use core::config::NodeConfig;
pub use core::identity::{NodeId, PasswordKey, UserId};
pub use common::schema::{
	normalize_field_name, validate_field_kind, FieldIndex, FieldKind, SchemaValidationError,
};

pub use engine::database::core::{
	DatabaseCatalog, DatabaseEntity, DatabaseError, DatabaseId, DatabaseIndex,
	DatabaseIndexKind, DatabaseIndexOrigin,
	DatabaseEntityAspect, DatabaseEntityKind, DatabaseRelationship, DatabaseReplicaState,
	DatabaseResult, DatabaseStoredProcedure, DatabaseTable, DatabaseTrigger,
	EntityMetadata,
	decode_row_payload, encode_row_payload,
	DatabaseObjectRef, DatabaseObjectType,
	DatabaseView, IndexId, ObjectStatus,
	DiskToMemorySchemaMigrationExecutor, FieldTypeChangeRule, NoopSchemaMigrationExecutor,
	run_schema_migration, SchemaMigrationExecutor, SchemaMigrationProgress, SchemaMutationRuleSet,
	TypeConversionPolicy,
};

pub use engine::database::runtime_index::{index_value_tuple, primary_key_index, RuntimeIndexStore};
pub use engine::execution::{
	build_joined_row_tuples, build_relation_probe_index, choose_index_lookup,
	collect_indexable_equality_filters, compare_row_value, count_condition_predicates,
		describe_table_result,
	select_mutation_target_rows,
	execute_joined_select_plan, execute_projection_only_select_plan,
	execute_relation_select_plan, explain_joined_select_plan_result, explain_select_plan_result,
	field_has_single_column_index, join_condition_field_names, join_condition_matches_provider,
	load_live_rows, materialize_relation_rows, plan_relation_access, relation_qualifier,
	row_matches_condition_with, row_matches_select_condition, ConditionValueProvider,
	EqualityProbeSource,
	JoinedRowCandidateProvider, JoinedRowMember, JoinedRowTuple, MaterializedRelationRow,
		RelationAccessPlan, RelationAccessStrategy, SelectExecutionResult, show_databases_result,
		show_tables_result,
};

pub use engine::database::table_schema::{FieldDef, FieldType, SchemaError, SchemaResult, TableSchema};
pub use engine::database::transaction::{
	EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
	SqlObjectKind, TableLifecycleAction, TableLifecyclePayload,
	TransactionId, TransactionKind, TransactionRecord,
};

pub use engine::affinity::{
	AffinityDocument, AffinityMember, AffinityMemberStatus, AffinityProcessor,
	AffinityProcessorError, AffinityProcessorState, AffinitySyncPhase, AffinitySyncStep,
	CheckpointMetadata, DatabaseSchemaSummary, ReplicationSecuritySummary,
};
pub use engine::affinity_storage::AffinityStorage;
pub use engine::replication_executor::ReplicationPhaseExecutor;
pub use engine::sql::{
	create_table_schema_from_statement, 
	parse_mysql8_sql_requests, 
	parse_sql_requests,
	parse_insert_rows_from_statement,
	parse_update_rows_from_statement,
	parse_delete_rows_from_statement,
	parse_select_read_plan_from_statement,
	parse_select_projection_from_statement,
	parse_alter_table_change_plan_from_statement,
	sql_directive_for_statement,
	InsertRowsPlan, InsertRowsSource,
	DeleteRowsPlan,
	SelectComparisonOp, SelectCondition, SelectJoin, SelectJoinKind, SelectPredicate, SelectProjectionItem,
	SelectReadPlan, SelectRelation,
	UpdateAssignment, UpdateRowsPlan,
	AlterTableChangeOp, AlterTableChangePlan,
	SqlCompatibilityTarget, SqlDirective, SqlOperation, SqlParseError, SqlRequest,
};

pub use engine::wal::ConcurrentWalManager;

pub use p2p::{
	DiscoveryMode, KademliaDiscoveryConfig, KademliaDiscoveryService,
	ServerP2pEvent, ServerP2pHandleOutcome, ServerP2pNetwork, ServerP2pRuntime,
};

