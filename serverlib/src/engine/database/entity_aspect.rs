
use super::core::ObjectStatus;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::table_schema::TableSchema;

pub trait DatabaseEntityAspect {
    fn name(&self) -> &str;
    fn kind(&self) -> DatabaseEntityKind;
    fn storage_key(&self) -> String;
    fn status(&self) -> ObjectStatus;
    fn metadata(&self) -> &EntityMetadata;
    fn wal_stream_id(&self, database_wal_id: &str) -> String;
    fn schema_revision(&self) -> Option<u64>;
    fn schema(&self) -> Option<&TableSchema>;
    fn normalize_in_place(&mut self);
}
