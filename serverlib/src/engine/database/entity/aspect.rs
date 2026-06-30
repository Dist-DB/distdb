
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::database::table::schema::TableSchema;

pub trait DatabaseEntityAspect {
    fn name(&self) -> &str;
    fn kind(&self) -> DatabaseEntityKind;
    fn storage_key(&self) -> String;
    fn set_entity_id(&mut self, entity_id: String);
    fn status(&self) -> ObjectStatus;
    fn metadata(&self) -> &EntityMetadata;
    fn wal_stream_id(&self, database_wal_id: &str) -> String;
    fn schema_revision(&self) -> Option<u64>;
    fn schema(&self) -> Option<&TableSchema>;
    fn normalize_in_place(&mut self);
}
