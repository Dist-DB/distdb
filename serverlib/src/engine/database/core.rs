
pub type DatabaseResult<T> = Result<T, DatabaseError>;

pub use super::error::DatabaseError;
pub use super::schema_error::SchemaError;
pub use super::status::ObjectStatus;
pub use super::catalog::DatabaseCatalog;
pub use super::id::DatabaseId;
pub use super::replica_state::DatabaseReplicaState;
pub use super::entity_metadata::EntityMetadata;
pub use super::entity::{
	DatabaseEntity, DatabaseEntityAspect, DatabaseEntityKind, DatabaseObjectRef,
	DatabaseObjectType,
};
pub use super::index::{DatabaseIndex, IndexId};
pub use super::relationship::DatabaseRelationship;
pub use super::schema_change_tx::SchemaChangeTx;
pub use super::stored_procedure::DatabaseStoredProcedure;
pub use super::table::DatabaseTable;
pub use super::trigger::DatabaseTrigger;
pub use super::view::DatabaseView;
