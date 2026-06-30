
pub mod affinity;
pub mod replication_executor;
pub mod database;
pub mod execution;
pub mod security;
pub mod sql;
pub mod wal;

pub use database::core::{
	DatabaseCatalog, DatabaseError, DatabaseId, DatabaseIndex, DatabaseRelationship,
	DatabaseReplicaState, DatabaseResult, DatabaseStoredProcedure, DatabaseTable,
	DatabaseTrigger, DatabaseView, DatabaseEntity,
	EntityMetadata,
	DatabaseEntityAspect, DatabaseEntityKind, DatabaseObjectRef, DatabaseObjectType,
	IndexId, ObjectStatus,
};

pub use affinity::{
	AffinityDocument, AffinityMember, AffinityMemberStatus, AffinityProcessor,
	AffinityProcessorError, AffinityProcessorState, AffinitySyncPhase, AffinitySyncStep,
	DatabaseSchemaSummary, ReplicationSecuritySummary,
};

pub use database::table::schema::{FieldDef, FieldType, SchemaError, SchemaResult, TableSchema};
pub use database::transaction::{TransactionId, TransactionKind, TransactionRecord};

pub use database::transaction::{
	EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
	SqlObjectKind, TableLifecycleAction, TableLifecyclePayload,
};

pub use wal::{ConcurrentWalManager, WalStreamMode};
