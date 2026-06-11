
use super::core::ObjectStatus;
use super::entity_metadata::EntityMetadata;
use super::index::DatabaseIndex;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEntityKind {
    Table,
    View,
    Relationship,
    Trigger,
    StoredProcedure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseObjectType {
    Table,
    View,
    Relationship,
    Trigger,
    StoredProcedure,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseObjectRef<'a> {
    Table(&'a DatabaseTable),
    View(&'a DatabaseView),
    Relationship(&'a DatabaseRelationship),
    Trigger(&'a DatabaseTrigger),
    StoredProcedure(&'a DatabaseStoredProcedure),
    Index(&'a DatabaseIndex),
}

impl<'a> DatabaseObjectRef<'a> {

    pub fn object_type(&self) -> DatabaseObjectType {
        match self {
            Self::Table(_) => DatabaseObjectType::Table,
            Self::View(_) => DatabaseObjectType::View,
            Self::Relationship(_) => DatabaseObjectType::Relationship,
            Self::Trigger(_) => DatabaseObjectType::Trigger,
            Self::StoredProcedure(_) => DatabaseObjectType::StoredProcedure,
            Self::Index(_) => DatabaseObjectType::Index,
        }
    }
    
}

pub trait DatabaseEntityAspect {
    fn kind(&self) -> DatabaseEntityKind;
    fn storage_key(&self) -> String;
    fn status(&self) -> ObjectStatus;
    fn metadata(&self) -> &EntityMetadata;
    fn wal_stream_id(&self, database_wal_id: &str) -> String;
    fn schema_revision(&self) -> Option<u64>;
    fn schema(&self) -> Option<&TableSchema>;
    fn normalize_in_place(&mut self);
}

impl DatabaseEntityAspect for DatabaseEntity {

    fn kind(&self) -> DatabaseEntityKind {
        match self {
            Self::Table(_) => DatabaseEntityKind::Table,
            Self::View(_) => DatabaseEntityKind::View,
            Self::Relationship(_) => DatabaseEntityKind::Relationship,
            Self::Trigger(_) => DatabaseEntityKind::Trigger,
            Self::StoredProcedure(_) => DatabaseEntityKind::StoredProcedure,
        }
    }

    fn storage_key(&self) -> String {
        match self {
            Self::Table(table) => table.storage_key(),
            Self::View(view) => view.storage_key(),
            Self::Relationship(relationship) => relationship.storage_key(),
            Self::Trigger(trigger) => trigger.storage_key(),
            Self::StoredProcedure(procedure) => procedure.storage_key(),
        }
    }

    fn status(&self) -> ObjectStatus {
        match self {
            Self::Table(table) => table.status(),
            Self::View(view) => view.status(),
            Self::Relationship(relationship) => relationship.status(),
            Self::Trigger(trigger) => trigger.status(),
            Self::StoredProcedure(procedure) => procedure.status(),
        }
    }

    fn metadata(&self) -> &EntityMetadata {
        match self {
            Self::Table(table) => table.metadata(),
            Self::View(view) => view.metadata(),
            Self::Relationship(relationship) => relationship.metadata(),
            Self::Trigger(trigger) => trigger.metadata(),
            Self::StoredProcedure(procedure) => procedure.metadata(),
        }
    }

    fn wal_stream_id(&self, database_wal_id: &str) -> String {
        match self {
            Self::Table(table) => table.wal_stream_id(database_wal_id),
            Self::View(view) => view.wal_stream_id(database_wal_id),
            Self::Relationship(relationship) => relationship.wal_stream_id(database_wal_id),
            Self::Trigger(trigger) => trigger.wal_stream_id(database_wal_id),
            Self::StoredProcedure(procedure) => procedure.wal_stream_id(database_wal_id),
        }
    }

    fn schema_revision(&self) -> Option<u64> {
        match self {
            Self::Table(table) => Some(table.schema_revision()),
            Self::View(view) => view.schema_revision(),
            Self::Relationship(relationship) => relationship.schema_revision(),
            Self::Trigger(trigger) => trigger.schema_revision(),
            Self::StoredProcedure(procedure) => procedure.schema_revision(),
        }
    }

    fn schema(&self) -> Option<&TableSchema> {
        match self {
            Self::Table(table) => Some(table.schema()),
            Self::View(view) => view.schema(),
            Self::Relationship(relationship) => relationship.schema(),
            Self::Trigger(trigger) => trigger.schema(),
            Self::StoredProcedure(procedure) => procedure.schema(),
        }
    }

    fn normalize_in_place(&mut self) {
        match self {
            Self::Table(table) => table.normalize_in_place(),
            Self::View(view) => view.normalize_in_place(),
            Self::Relationship(relationship) => relationship.normalize_in_place(),
            Self::Trigger(trigger) => trigger.normalize_in_place(),
            Self::StoredProcedure(procedure) => procedure.normalize_in_place(),
        }
    }
    
}
