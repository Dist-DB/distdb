
pub type DatabaseResult<T> = Result<T, DatabaseError>;

pub use crate::engine::database::error::DatabaseError;
pub use crate::engine::database::schema::error::SchemaError;
pub use crate::engine::database::status::ObjectStatus;
pub use crate::engine::database::catalog::DatabaseCatalog;
pub use crate::engine::database::id::DatabaseId;
pub use crate::engine::database::replica_state::DatabaseReplicaState;
pub use crate::engine::database::entity::metadata::EntityMetadata;
pub use crate::engine::database::entity::database_entity::DatabaseEntity;
pub use crate::engine::database::entity::aspect::DatabaseEntityAspect;
pub use crate::engine::database::entity::kind::DatabaseEntityKind;
pub use crate::engine::database::entity::object_ref::DatabaseObjectRef;
pub use crate::engine::database::entity::object_type::DatabaseObjectType;
pub use crate::engine::database::index::{DatabaseIndex, DatabaseIndexKind, DatabaseIndexOrigin, IndexId};
pub use crate::engine::database::relationship::DatabaseRelationship;

pub use crate::engine::database::row_payload::{
	decode_encrypted_row_payload_envelope, decode_row_field_value, decode_row_payload,
	encode_encrypted_row_payload_envelope, encode_row_payload,
	EncryptedRowPayloadEnvelope, EncryptedRowPayloadTransform,
	RowPayloadDecryptionProvider, RowPayloadDecryptionTransform,
	RowPayloadEncryptionProvider, RowPayloadEncryptionWriteTransform,
	EncryptedRowPayloadTransformPolicy, ENCRYPTED_ROW_PAYLOAD_ENVELOPE_VERSION,
	AesGcmRowPayloadCryptoProvider,
	UnconfiguredRowPayloadDecryptionProvider, UnconfiguredRowPayloadEncryptionProvider,
};

pub use crate::engine::database::schema::change_tx::SchemaChangeTx;
pub use crate::engine::database::schema::change_state::{ActiveSchemaChange, SchemaChangePhase};
pub use crate::engine::database::schema::migration::{
	compare_stored_field_values, display_stored_field_value, render_stored_field_value,
	run_schema_migration, DiskToMemorySchemaMigrationExecutor, NoopSchemaMigrationExecutor, 
	SchemaMigrationExecutor, SchemaMigrationProgress, FieldTypeChangeRule, SchemaMutationRuleSet,
	TypeConversionPolicy,
};

pub use crate::engine::database::stored_procedure::DatabaseStoredProcedure;
pub use crate::engine::database::table::DatabaseTable;
pub use crate::engine::database::trigger::DatabaseTrigger;
pub use crate::engine::database::view::DatabaseView;
