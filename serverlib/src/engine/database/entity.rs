
use super::core::ObjectStatus;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::relationship::DatabaseRelationship;
use super::stored_procedure::DatabaseStoredProcedure;
use super::table::DatabaseTable;
use super::table_schema::TableSchema;
use super::trigger::DatabaseTrigger;
use super::view::DatabaseView;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DatabaseEntity {
    Table(DatabaseTable),
    View(DatabaseView),
    Relationship(DatabaseRelationship),
    Trigger(DatabaseTrigger),
    StoredProcedure(DatabaseStoredProcedure),
}

impl DatabaseEntityAspect for DatabaseEntity {

    fn kind(&self) -> DatabaseEntityKind {
        match self {
            Self::Table(_)           => DatabaseEntityKind::Table,
            Self::View(_)            => DatabaseEntityKind::View,
            Self::Relationship(_)    => DatabaseEntityKind::Relationship,
            Self::Trigger(_)         => DatabaseEntityKind::Trigger,
            Self::StoredProcedure(_) => DatabaseEntityKind::StoredProcedure,
        }
    }

    fn storage_key(&self) -> String {
        match self {
            Self::Table(t)        => t.storage_key(),
            Self::View(v)         => v.storage_key(),
            Self::Relationship(r) => r.storage_key(),
            Self::Trigger(t)      => t.storage_key(),
            Self::StoredProcedure(p) => p.storage_key(),
        }
    }

    fn status(&self) -> ObjectStatus {
        match self {
            Self::Table(t)        => t.status(),
            Self::View(v)         => v.status(),
            Self::Relationship(r) => r.status(),
            Self::Trigger(t)      => t.status(),
            Self::StoredProcedure(p) => p.status(),
        }
    }

    fn metadata(&self) -> &EntityMetadata {
        match self {
            Self::Table(t)        => t.metadata(),
            Self::View(v)         => v.metadata(),
            Self::Relationship(r) => r.metadata(),
            Self::Trigger(t)      => t.metadata(),
            Self::StoredProcedure(p) => p.metadata(),
        }
    }

    fn wal_stream_id(&self, database_wal_id: &str) -> String {
        match self {
            Self::Table(t)        => t.wal_stream_id(database_wal_id),
            Self::View(v)         => v.wal_stream_id(database_wal_id),
            Self::Relationship(r) => r.wal_stream_id(database_wal_id),
            Self::Trigger(t)      => t.wal_stream_id(database_wal_id),
            Self::StoredProcedure(p) => p.wal_stream_id(database_wal_id),
        }
    }

    fn schema_revision(&self) -> Option<u64> {
        match self {
            Self::Table(t)        => Some(t.schema_revision()),
            Self::View(v)         => v.schema_revision(),
            Self::Relationship(r) => r.schema_revision(),
            Self::Trigger(t)      => t.schema_revision(),
            Self::StoredProcedure(p) => p.schema_revision(),
        }
    }

    fn schema(&self) -> Option<&TableSchema> {
        match self {
            Self::Table(t)        => Some(t.schema()),
            Self::View(v)         => v.schema(),
            Self::Relationship(r) => r.schema(),
            Self::Trigger(t)      => t.schema(),
            Self::StoredProcedure(p) => p.schema(),
        }
    }

    fn normalize_in_place(&mut self) {
        match self {
            Self::Table(t)        => t.normalize_in_place(),
            Self::View(v)         => v.normalize_in_place(),
            Self::Relationship(r) => r.normalize_in_place(),
            Self::Trigger(t)      => t.normalize_in_place(),
            Self::StoredProcedure(p) => p.normalize_in_place(),
        }
    }

}
