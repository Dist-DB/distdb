#![allow(dead_code)]

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
	DatabaseEntityAspect, DatabaseEntityKind, DatabaseRelationship, DatabaseReplicaState,
	DatabaseResult, DatabaseStoredProcedure, DatabaseTable, DatabaseTrigger,
	EntityMetadata,
	DatabaseObjectRef, DatabaseObjectType,
	DatabaseView, IndexId, ObjectStatus,
	DiskToMemorySchemaMigrationExecutor, FieldTypeChangeRule, NoopSchemaMigrationExecutor,
	run_schema_migration, SchemaMigrationExecutor, SchemaMigrationProgress, SchemaMutationRuleSet,
	TypeConversionPolicy,
};

pub use engine::database::table_schema::{FieldDef, FieldType, SchemaError, SchemaResult, TableSchema};
pub use engine::database::transaction::{
	EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
	SqlObjectKind, TableLifecycleAction, TableLifecyclePayload,
	TransactionId, TransactionKind, TransactionRecord,
};

pub use engine::replication::{EventType, PublicationEvent, SubscriptionKey};
pub use engine::sql::{
	create_table_schema_from_statement, parse_mysql8_sql_requests, parse_sql_requests,
	parse_alter_table_change_plan_from_statement,
	sql_directive_for_statement,
	AlterTableChangeOp, AlterTableChangePlan,
	SqlCompatibilityTarget, SqlDirective, SqlOperation, SqlParseError, SqlRequest,
};

pub use engine::wal::ConcurrentWalManager;

pub use p2p::{
	DiscoveryMode, KademliaDiscoveryConfig, KademliaDiscoveryService,
	ServerP2pEvent, ServerP2pHandleOutcome, ServerP2pNetwork, ServerP2pRuntime,
};

