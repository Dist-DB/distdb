
use std::collections::HashMap;
use std::path::Path;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, write_bytes};

use super::core::{DatabaseError, DatabaseResult, ObjectStatus};
use super::entity::{
    DatabaseEntity, DatabaseEntityAspect, DatabaseEntityKind, DatabaseObjectRef,
    DatabaseObjectType,
};
use super::id::DatabaseId;
use super::index::DatabaseIndex;
use super::relationship::DatabaseRelationship;
use super::schema_change_tx::SchemaChangeTx;
use super::stored_procedure::DatabaseStoredProcedure;
use super::table::DatabaseTable;
use super::table_schema::{FieldIndex, TableSchema};
use super::trigger::DatabaseTrigger;
use super::transaction::{
    EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
    SqlObjectKind, TransactionKind, TransactionLog,
};
use super::view::DatabaseView;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    status: ObjectStatus,
    entities: HashMap<String, DatabaseEntity>,
}

impl DatabaseCatalog {
    
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            status: ObjectStatus::Load,
            entities: HashMap::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> DatabaseResult<Self> {
        let database_id = DatabaseId::from_database_name(name)?;
        Ok(Self::new(database_id))
    }

    pub fn create_new_database(name: &str, directory: impl AsRef<Path>) -> DatabaseResult<Self> {
        let mut catalog = Self::create_empty_from_name(name)?;
        catalog.transition_status(ObjectStatus::Sync)?;
        catalog.save_in_directory(&directory)?;

        if !catalog.database_sync_acknowledged_stub() {
            return Err(DatabaseError::SyncPending);
        }

        catalog.transition_status(ObjectStatus::Ready)?;
        catalog.save_in_directory(directory)?;
        Ok(catalog)
    }

    pub fn register_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id.into());

        if self.entities.contains_key(&table_id) {
            return Err(DatabaseError::DuplicateTable);
        }

        let indexes = Self::indexes_for_schema(&table_id, &schema);

        self.entities.insert(
            table_id.clone(),
            DatabaseEntity::Table(DatabaseTable::new(table_id, schema, indexes)),
        );

        Ok(())
    }

    pub fn create_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id.into());
        self.register_table(table_id.clone(), schema)?;

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) {
        let left = common::normalize_identifier!(&relationship.left_table_id);
        let right = common::normalize_identifier!(&relationship.right_table_id);
        let name = common::normalize_identifier!(&relationship.relation_name);
        let entity_id = format!("rel:{left}:{right}:{name}");
        self.entities
            .insert(entity_id, DatabaseEntity::Relationship(relationship));
    }

    pub fn table(&self, table_id: &str) -> Option<&DatabaseTable> {
        match self.object(DatabaseObjectType::Table, table_id) {
            Some(DatabaseObjectRef::Table(table)) => Some(table),
            _ => None,
        }
    }

    pub fn index(&self, index_id: &str) -> Option<&DatabaseIndex> {
        match self.object(DatabaseObjectType::Index, index_id) {
            Some(DatabaseObjectRef::Index(index)) => Some(index),
            _ => None,
        }
    }

    pub fn object(&self, object_type: DatabaseObjectType, object_id: &str) -> Option<DatabaseObjectRef<'_>> {

        let normalized = common::normalize_identifier!(object_id);
        
        match object_type {
            DatabaseObjectType::Table => match self.entities.get(&normalized) {
                Some(DatabaseEntity::Table(table)) => Some(DatabaseObjectRef::Table(table)),
                _ => None,
            },
            DatabaseObjectType::View => match self.entities.get(&normalized) {
                Some(DatabaseEntity::View(view)) => Some(DatabaseObjectRef::View(view)),
                _ => None,
            },
            DatabaseObjectType::Relationship => self.entities.get(&normalized).and_then(|entity| match entity {
                DatabaseEntity::Relationship(relationship) => Some(DatabaseObjectRef::Relationship(relationship)),
                _ => None,
            }),
            DatabaseObjectType::Trigger => match self.entities.get(&normalized) {
                Some(DatabaseEntity::Trigger(trigger)) => Some(DatabaseObjectRef::Trigger(trigger)),
                _ => None,
            },
            DatabaseObjectType::StoredProcedure => match self.entities.get(&normalized) {
                Some(DatabaseEntity::StoredProcedure(procedure)) => {
                    Some(DatabaseObjectRef::StoredProcedure(procedure))
                }
                _ => None,
            },
            DatabaseObjectType::Index => {
                self.entities.values().find_map(|entity| match entity {
                    DatabaseEntity::Table(table) => table
                        .indexes
                        .get(&normalized)
                        .map(DatabaseObjectRef::Index),
                    _ => None,
                })
            }
        }
        
    }

    /// Return an object by id without requiring the caller to provide an
    /// object type. Entity ids are checked first, then table indexes.
    pub fn object_by_id(&self, object_id: &str) -> Option<DatabaseObjectRef<'_>> {

        let normalized = common::normalize_identifier!(object_id);

        if let Some(entity) = self.entities.get(&normalized) {
            return match entity {
                DatabaseEntity::Table(table) => Some(DatabaseObjectRef::Table(table)),
                DatabaseEntity::View(view) => Some(DatabaseObjectRef::View(view)),
                DatabaseEntity::Relationship(relationship) => {
                    Some(DatabaseObjectRef::Relationship(relationship))
                }
                DatabaseEntity::Trigger(trigger) => Some(DatabaseObjectRef::Trigger(trigger)),
                DatabaseEntity::StoredProcedure(procedure) => {
                    Some(DatabaseObjectRef::StoredProcedure(procedure))
                }
            };
        }

        self.entities.values().find_map(|entity| match entity {
            DatabaseEntity::Table(table) => table.indexes.get(&normalized).map(DatabaseObjectRef::Index),
            _ => None,
        })

    }

    pub fn entity(&self, entity_id: &str) -> Option<&DatabaseEntity> {
        let normalized = common::normalize_identifier!(entity_id);
        self.entities.get(&normalized)
    }

    pub fn entity_kind(&self, entity_id: &str) -> Option<DatabaseEntityKind> {
        self.entity(entity_id).map(DatabaseEntityAspect::kind)
    }

    pub fn entity_status(&self, entity_id: &str) -> Option<ObjectStatus> {
        self.entity(entity_id).map(DatabaseEntityAspect::status)
    }

    pub fn entity_metadata(&self, entity_id: &str) -> Option<&super::entity_metadata::EntityMetadata> {
        self.entity(entity_id).map(DatabaseEntityAspect::metadata)
    }

    pub fn entity_wal_stream_id(&self, entity_id: &str) -> Option<String> {
        self.entity(entity_id)
            .map(|entity| entity.wal_stream_id(&self.database_id.0))
    }

    pub fn entity_schema_revision(&self, entity_id: &str) -> Option<u64> {
        self.entity(entity_id)
            .and_then(DatabaseEntityAspect::schema_revision)
    }

    pub fn relationships(&self) -> Vec<&DatabaseRelationship> {
        self.entities
            .values()
            .filter_map(|entity| match entity {
                DatabaseEntity::Relationship(relationship) => Some(relationship),
                _ => None,
            })
            .collect()
    }

    pub fn status(&self) -> ObjectStatus {
        self.status
    }

    pub fn transition_status(&mut self, next: ObjectStatus) -> DatabaseResult<()> {
        if !self.status.can_transition_to(next) {
            return Err(DatabaseError::InvalidStatusTransition);
        }
        self.status = next;
        Ok(())
    }

    pub fn table_schema(&self, table_id: &str) -> Option<&TableSchema> {
        self.table(table_id).map(DatabaseTable::schema)
    }

    pub fn table_schema_revision(&self, table_id: &str) -> Option<u64> {
        self.table(table_id).map(DatabaseTable::schema_revision)
    }

    /// Lock `table_id` (`Ready -> Lock`) and return a [`SchemaChangeTx`] that
    /// owns the pending schema mutations. The table stays locked until the
    /// returned transaction is either committed or aborted.
    pub fn begin_schema_change(&mut self, table_id: &str) -> DatabaseResult<SchemaChangeTx> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        let pending_schema = table.schema().clone();
        let next_revision = table.schema_revision() + 1;

        table.lock()?;

        Ok(SchemaChangeTx::new(table_id, next_revision, pending_schema))
    }

    /// Internal: apply a payload and drive `Lock -> Sync -> Ready`.
    /// Called only from `SchemaChangeTx::commit`.
    pub(crate) fn finalize_schema_change(
        &mut self,
        payload: SchemaChangePayload,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(&payload.table_id);
        self.apply_schema_change(payload)?;
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;
        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    /// Internal: release the lock without changing the schema (`Lock -> Ready`).
    /// Called only from `SchemaChangeTx::abort`.
    pub(crate) fn release_schema_lock(&mut self, table_id: &str) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.abort()
    }

    /// Internal: apply a schema payload directly. Does not enforce or alter
    /// table status. Used by `finalize_schema_change` and WAL replay.
    pub fn apply_schema_change(&mut self, payload: SchemaChangePayload) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(payload.table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        if payload.schema_revision <= table.schema_revision() {
            return Err(DatabaseError::SchemaRevisionOutOfOrder);
        }

        let indexes = Self::indexes_for_schema(&table_id, &payload.schema);

        table.replace_schema(payload.schema_revision, payload.schema, indexes);
        Ok(())
    }

    pub fn apply_entity_metadata(&mut self, payload: EntityMetadataPayload) -> DatabaseResult<()> {
        let entity_id = common::normalize_identifier!(payload.entity_id);
        let entity = self
            .entities
            .get_mut(&entity_id)
            .ok_or(DatabaseError::EntityNotFound)?;

        match entity {
            DatabaseEntity::Table(table) => table.metadata = payload.metadata,
            DatabaseEntity::View(view) => view.metadata = payload.metadata,
            DatabaseEntity::Relationship(relationship) => relationship.metadata = payload.metadata,
            DatabaseEntity::Trigger(trigger) => trigger.metadata = payload.metadata,
            DatabaseEntity::StoredProcedure(procedure) => procedure.metadata = payload.metadata,
        }

        Ok(())
    }

    pub fn set_entity_metadata(
        &mut self,
        entity_id: impl Into<String>,
        metadata: super::entity_metadata::EntityMetadata,
    ) -> DatabaseResult<()> {
        let payload = EntityMetadataPayload {
            entity_id: entity_id.into(),
            metadata,
        };
        self.apply_entity_metadata(payload)
    }

    pub fn apply_sql_definition(&mut self, payload: SqlDefinitionPayload) -> DatabaseResult<()> {
        let object_id = common::normalize_identifier!(payload.object_id);

        match payload.action {
            SqlDefinitionAction::Upsert => {
                let normalized_dependencies = payload
                    .dependencies
                    .into_iter()
                    .map(|dep| common::normalize_identifier!(dep))
                    .collect::<Vec<_>>();

                match payload.object_kind {
                    SqlObjectKind::View => {
                        if self.view(&object_id).is_none() {
                            self.register_view(
                                object_id.clone(),
                                payload.sql.clone(),
                                TableSchema::new(Vec::new()),
                            )?;
                        }

                        let view = self.view_mut(&object_id).ok_or(DatabaseError::ViewNotFound)?;
                        view.sql = payload.sql;
                        view.dependencies = normalized_dependencies;
                        Ok(())
                    }
                    SqlObjectKind::Trigger => {
                        if self.trigger(&object_id).is_none() {
                            self.register_trigger(
                                object_id.clone(),
                                payload.sql.clone(),
                                normalized_dependencies.clone(),
                            )?;
                        }

                        let trigger = self
                            .trigger_mut(&object_id)
                            .ok_or(DatabaseError::TriggerNotFound)?;
                        trigger.sql = payload.sql;
                        trigger.dependencies = normalized_dependencies;
                        Ok(())
                    }
                    SqlObjectKind::StoredProcedure => {
                        if self.stored_procedure(&object_id).is_none() {
                            self.register_stored_procedure(
                                object_id.clone(),
                                payload.sql.clone(),
                                normalized_dependencies.clone(),
                            )?;
                        }

                        let procedure = self
                            .stored_procedure_mut(&object_id)
                            .ok_or(DatabaseError::StoredProcedureNotFound)?;
                        procedure.sql = payload.sql;
                        procedure.dependencies = normalized_dependencies;
                        Ok(())
                    }
                }
            }
            SqlDefinitionAction::Drop => match payload.object_kind {
                SqlObjectKind::View => match self.drop_view(&object_id) {
                    Ok(()) | Err(DatabaseError::ViewNotFound) => Ok(()),
                    Err(e) => Err(e),
                },
                SqlObjectKind::Trigger => match self.drop_trigger(&object_id) {
                    Ok(()) | Err(DatabaseError::TriggerNotFound) => Ok(()),
                    Err(e) => Err(e),
                },
                SqlObjectKind::StoredProcedure => match self.drop_stored_procedure(&object_id) {
                    Ok(()) | Err(DatabaseError::StoredProcedureNotFound) => Ok(()),
                    Err(e) => Err(e),
                },
            },
        }
    }

    pub fn set_sql_definition(
        &mut self,
        object_id: impl Into<String>,
        object_kind: SqlObjectKind,
        sql: impl Into<String>,
        dependencies: Vec<String>,
    ) -> DatabaseResult<()> {
        let payload = SqlDefinitionPayload {
            object_id: object_id.into(),
            object_kind,
            action: SqlDefinitionAction::Upsert,
            sql: sql.into(),
            dependencies,
        };
        self.apply_sql_definition(payload)
    }

    pub fn replay_schema_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {
        let mut applied = 0usize;

        for record in log.since(wal_id, None) {
            if record.kind != TransactionKind::SchemaChange {
                continue;
            }

            let payload = SchemaChangePayload::decode(&record.payload)
                .map_err(|_| DatabaseError::SchemaPayloadDeserialize)?;
            self.apply_schema_change(payload)?;
            applied += 1;
        }

        Ok(applied)
    }

    pub fn replay_entity_construction_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {
        let mut applied = 0usize;

        for record in log.since(wal_id, None) {
            match record.kind {
                TransactionKind::SchemaChange => {
                    let payload = SchemaChangePayload::decode(&record.payload)
                        .map_err(|_| DatabaseError::SchemaPayloadDeserialize)?;
                    self.apply_schema_change(payload)?;
                    applied += 1;
                }
                TransactionKind::MetadataChange | TransactionKind::SecurityChange => {
                    let payload = EntityMetadataPayload::decode(&record.payload)
                        .map_err(|_| DatabaseError::MetadataPayloadDeserialize)?;
                    self.apply_entity_metadata(payload)?;
                    applied += 1;
                }
                TransactionKind::SqlDefinitionChange => {
                    let payload = SqlDefinitionPayload::decode(&record.payload)
                        .map_err(|_| DatabaseError::SqlDefinitionPayloadDeserialize)?;
                    self.apply_sql_definition(payload)?;
                    applied += 1;
                }
                _ => {}
            }
        }

        Ok(applied)
    }

    pub fn ensure_ready_for_write(&self, table_id: &str) -> DatabaseResult<()> {
        if self.status != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        let table = self.table(table_id).ok_or(DatabaseError::TableNotFound)?;

        if table.status() != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        Ok(())
    }

    pub fn table_status(&self, table_id: &str) -> Option<ObjectStatus> {
        self.table(table_id).map(DatabaseTable::status)
    }

    pub fn file_name(&self) -> String {
        FileKind::Catalog.file_name(common::normalize_identifier!(self.database_id.0.clone()))
    }

    pub fn from_file_stem(stem: &str) -> Self {
        Self::new(DatabaseId(common::normalize_identifier!(stem)))
    }

    pub fn table_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::Table(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    /// Register a view definition with a pre-derived schema. The schema is
    /// resolved by the caller at `CREATE VIEW` time against the current table
    /// catalog and stored here so schema inspection never needs to re-execute
    /// the view SQL.
    pub fn register_view(
        &mut self,
        view_id: impl Into<String>,
        sql: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let view_id = common::normalize_identifier!(view_id.into());

        if self.entities.contains_key(&view_id) {
            return Err(DatabaseError::DuplicateView);
        }

        self.entities.insert(
            view_id.clone(),
            DatabaseEntity::View(DatabaseView::new(view_id, sql.into(), schema)),
        );

        Ok(())
    }

    pub fn view(&self, view_id: &str) -> Option<&DatabaseView> {
        match self.object(DatabaseObjectType::View, view_id) {
            Some(DatabaseObjectRef::View(view)) => Some(view),
            _ => None,
        }
    }

    pub fn drop_view(&mut self, view_id: &str) -> DatabaseResult<()> {
        let normalized = common::normalize_identifier!(view_id);
        match self.entities.get(&normalized) {
            Some(DatabaseEntity::View(_)) => {
                self.entities.remove(&normalized);
                Ok(())
            }
            _ => Err(DatabaseError::ViewNotFound),
        }
    }

    pub fn relationship(&self, relationship_id: &str) -> Option<&DatabaseRelationship> {
        match self.object(DatabaseObjectType::Relationship, relationship_id) {
            Some(DatabaseObjectRef::Relationship(relationship)) => Some(relationship),
            _ => None,
        }
    }

    pub fn register_trigger(
        &mut self,
        trigger_id: impl Into<String>,
        sql: impl Into<String>,
        dependencies: Vec<String>,
    ) -> DatabaseResult<()> {
        let trigger_id = common::normalize_identifier!(trigger_id.into());

        if self.entities.contains_key(&trigger_id) {
            return Err(DatabaseError::DuplicateTrigger);
        }

        self.entities.insert(
            trigger_id.clone(),
            DatabaseEntity::Trigger(DatabaseTrigger::new(
                trigger_id,
                sql.into(),
                dependencies,
            )),
        );

        Ok(())
    }

    pub fn trigger(&self, trigger_id: &str) -> Option<&DatabaseTrigger> {
        match self.object(DatabaseObjectType::Trigger, trigger_id) {
            Some(DatabaseObjectRef::Trigger(trigger)) => Some(trigger),
            _ => None,
        }
    }

    pub fn drop_trigger(&mut self, trigger_id: &str) -> DatabaseResult<()> {
        let normalized = common::normalize_identifier!(trigger_id);
        match self.entities.get(&normalized) {
            Some(DatabaseEntity::Trigger(_)) => {
                self.entities.remove(&normalized);
                Ok(())
            }
            _ => Err(DatabaseError::TriggerNotFound),
        }
    }

    pub fn trigger_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::Trigger(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn register_stored_procedure(
        &mut self,
        procedure_id: impl Into<String>,
        sql: impl Into<String>,
        dependencies: Vec<String>,
    ) -> DatabaseResult<()> {
        let procedure_id = common::normalize_identifier!(procedure_id.into());

        if self.entities.contains_key(&procedure_id) {
            return Err(DatabaseError::DuplicateStoredProcedure);
        }

        self.entities.insert(
            procedure_id.clone(),
            DatabaseEntity::StoredProcedure(DatabaseStoredProcedure::new(
                procedure_id,
                sql.into(),
                dependencies,
            )),
        );

        Ok(())
    }

    pub fn stored_procedure(&self, procedure_id: &str) -> Option<&DatabaseStoredProcedure> {
        match self.object(DatabaseObjectType::StoredProcedure, procedure_id) {
            Some(DatabaseObjectRef::StoredProcedure(procedure)) => Some(procedure),
            _ => None,
        }
    }

    pub fn drop_stored_procedure(&mut self, procedure_id: &str) -> DatabaseResult<()> {
        let normalized = common::normalize_identifier!(procedure_id);
        match self.entities.get(&normalized) {
            Some(DatabaseEntity::StoredProcedure(_)) => {
                self.entities.remove(&normalized);
                Ok(())
            }
            _ => Err(DatabaseError::StoredProcedureNotFound),
        }
    }

    pub fn stored_procedure_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::StoredProcedure(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn view_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::View(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn view_schema(&self, view_id: &str) -> Option<&TableSchema> {
        self.view(view_id).map(|v| &v.schema)
    }

    /// Returns `true` for tables, `false` for views. Used at the query
    /// routing layer to reject write operations against view sources before
    /// any execution begins.
    pub fn is_writable(&self, object_id: &str) -> bool {
        let normalized = common::normalize_identifier!(object_id);
        matches!(
            self.entities.get(&normalized),
            Some(DatabaseEntity::Table(_))
        )
    }

    fn table_mut(&mut self, table_id: &str) -> Option<&mut DatabaseTable> {
        let normalized = common::normalize_identifier!(table_id);
        match self.entities.get_mut(&normalized) {
            Some(DatabaseEntity::Table(table)) => Some(table),
            _ => None,
        }
    }

    fn view_mut(&mut self, view_id: &str) -> Option<&mut DatabaseView> {
        let normalized = common::normalize_identifier!(view_id);
        match self.entities.get_mut(&normalized) {
            Some(DatabaseEntity::View(view)) => Some(view),
            _ => None,
        }
    }

    fn trigger_mut(&mut self, trigger_id: &str) -> Option<&mut DatabaseTrigger> {
        let normalized = common::normalize_identifier!(trigger_id);
        match self.entities.get_mut(&normalized) {
            Some(DatabaseEntity::Trigger(trigger)) => Some(trigger),
            _ => None,
        }
    }

    fn stored_procedure_mut(
        &mut self,
        procedure_id: &str,
    ) -> Option<&mut DatabaseStoredProcedure> {
        let normalized = common::normalize_identifier!(procedure_id);
        match self.entities.get_mut(&normalized) {
            Some(DatabaseEntity::StoredProcedure(procedure)) => Some(procedure),
            _ => None,
        }
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> DatabaseResult<Self> {
        let bytes = read_bytes(path).map_err(|_| DatabaseError::CatalogRead)?;

        common::helpers::format::verify_header(FileKind::Catalog, &bytes)
            .map_err(|_| DatabaseError::CatalogInvalidHeader)?;

        if bytes.len() <= common::helpers::format::HEADER_SIZE {
            return Err(DatabaseError::CatalogPayloadMissing);
        }

        let mut catalog = bincode::deserialize::<Self>(&bytes[common::helpers::format::HEADER_SIZE..])
            .map_err(|_| DatabaseError::CatalogDeserialize)?;

        catalog.normalize_loaded_entities()?;
        Ok(catalog)
    }

    pub fn save_in_directory(&self, directory: impl AsRef<Path>) -> DatabaseResult<()> {
        let path = directory.as_ref().join(self.file_name());
        self.save_to_path(path)
    }

    fn save_to_path(&self, path: impl AsRef<Path>) -> DatabaseResult<()> {
        let payload = bincode::serialize(self).map_err(|_| DatabaseError::CatalogSerialize)?;
        let mut file = Vec::with_capacity(common::helpers::format::HEADER_SIZE + payload.len());
        file.extend_from_slice(&common::helpers::format::make_header(FileKind::Catalog));
        file.extend_from_slice(&payload);
        write_bytes(path, &file).map_err(|_| DatabaseError::CatalogWrite)
    }

    // Stub for future p2p/quorum integration.
    // With zero configured replicas, sync can promote to Ready immediately.
    fn database_sync_acknowledged_stub(&self) -> bool {
        self.received_database_replica_acks_stub() >= self.required_database_replica_acks_stub()
    }

    // Stub for future p2p/quorum integration.
    // With zero configured replicas, sync can promote to Ready immediately.
    fn table_sync_acknowledged_stub(&self, table_id: &str) -> bool {
        self.received_table_replica_acks_stub(table_id)
            >= self.required_table_replica_acks_stub(table_id)
    }

    fn required_database_replica_acks_stub(&self) -> usize {
        0
    }

    fn received_database_replica_acks_stub(&self) -> usize {
        0
    }

    fn required_table_replica_acks_stub(&self, _table_id: &str) -> usize {
        0
    }

    fn received_table_replica_acks_stub(&self, _table_id: &str) -> usize {
        0
    }

    fn indexes_for_schema(table_id: &str, schema: &TableSchema) -> HashMap<String, DatabaseIndex> {
        let mut indexes = HashMap::new();
        for field in &schema.fields {
            if matches!(field.indexed, FieldIndex::Indexed | FieldIndex::PrimaryKey) {
                let index = DatabaseIndex::from_table_field(table_id, field);
                indexes.insert(index.index_id.0.clone(), index);
            }
        }
        indexes
    }

    fn normalize_loaded_entities(&mut self) -> DatabaseResult<()> {
        let mut normalized_entities = HashMap::with_capacity(self.entities.len());
        for (_, mut entity) in std::mem::take(&mut self.entities) {
            entity.normalize_in_place();

            if let DatabaseEntity::Table(table) = &mut entity {
                table.indexes = Self::indexes_for_schema(&table.table_id, &table.schema);
            }

            let key = entity.storage_key();
            if normalized_entities.insert(key, entity).is_some() {
                return Err(DatabaseError::CatalogDeserialize);
            }
        }

        self.entities = normalized_entities;
        Ok(())
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use crate::EntityMetadata;

    #[test]
    fn create_empty_catalog_from_name_sets_obscured_id() {
        let catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        assert!(catalog.table_ids().is_empty());
        assert!(!catalog.database_id.0.is_empty());
        assert_ne!(catalog.database_id.0, "maindb");
    }

    #[test]
    fn empty_database_name_is_rejected() {
        let created = DatabaseCatalog::create_empty_from_name("   ");
        assert!(created.is_err());
    }

    #[test]
    fn duplicate_table_registration_is_rejected() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema { fields: Vec::new() };

        let first = catalog.register_table("users", schema.clone());
        let second = catalog.register_table("users", schema);

        assert!(first.is_ok());
        assert!(second.is_err());
    }

    #[test]
    fn catalog_and_table_start_in_load_state() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema { fields: Vec::new() };

        catalog
            .register_table("users", schema)
            .expect("table register should succeed");

        assert_eq!(catalog.status(), ObjectStatus::Load);
        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Load));
    }

    #[test]
    fn lock_moves_to_sync_then_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .transition_status(ObjectStatus::Lock)
            .expect("load->lock is valid");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("lock->sync is valid");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready is valid");

        assert_eq!(catalog.status(), ObjectStatus::Ready);
    }

    #[test]
    fn lock_to_ready_is_valid_for_abort_path() {
        // Lock -> Ready is permitted so that table transactions can be aborted.
        // The catalog's own status follows the same state machine.
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .transition_status(ObjectStatus::Lock)
            .expect("load->lock is valid");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("lock->ready is valid as an abort path");

        assert_eq!(catalog.status(), ObjectStatus::Ready);
    }

    #[test]
    fn create_table_moves_load_sync_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .create_table("users", TableSchema { fields: Vec::new() })
            .expect("create table should succeed");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
    }

    #[test]
    fn write_requires_database_and_table_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .create_table("users", TableSchema { fields: Vec::new() })
            .expect("create table should succeed");

        let denied = catalog.ensure_ready_for_write("users");
        assert!(matches!(denied, Err(DatabaseError::NotReadyForWrite)));

        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let allowed = catalog.ensure_ready_for_write("users");
        assert!(allowed.is_ok());
    }

    #[test]
    fn schema_can_be_retrieved_from_table() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema::new(Vec::new());

        catalog
            .register_table("users", schema.clone())
            .expect("table register should succeed");

        assert_eq!(catalog.table_schema("users"), Some(&schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(0));
    }

    #[test]
    fn schema_change_payload_updates_existing_table() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("table register should succeed");

        let updated_schema = TableSchema::new(Vec::new());
        let payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 3,
            schema: updated_schema.clone(),
        };

        catalog
            .apply_schema_change(payload)
            .expect("schema change should apply");

        assert_eq!(catalog.table_schema("users"), Some(&updated_schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(3));
    }

    #[test]
    fn schema_change_tx_commit_applies_schema_and_returns_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let mut tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Lock));

        tx.add_field(crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        })
        .expect("add_field should succeed");

        let mut captured_payload: Option<SchemaChangePayload> = None;
        tx.commit::<DatabaseError, _>(&mut catalog, |payload| {
            captured_payload = Some(payload.clone());
            Ok(())
        })
        .expect("commit should succeed");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema_revision("users"), Some(1));
        assert!(catalog
            .table_schema("users")
            .and_then(|s| s.field("email"))
            .is_some());
        assert_eq!(captured_payload.expect("captured payload").schema_revision, 1);
    }

    #[test]
    fn schema_change_tx_abort_returns_table_to_ready_without_schema_change() {

        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        let initial_schema = TableSchema::new(vec![crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
        }]);
        catalog
            .create_table("users", initial_schema.clone())
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let mut tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");
        tx.remove_field("name")
            .expect("remove should succeed on pending schema");

        tx.abort(&mut catalog).expect("abort should release lock");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema("users"), Some(&initial_schema));
    }

    #[test]
    fn schema_change_tx_commit_aborts_when_persist_fails() {

        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        let initial_schema = TableSchema::new(Vec::new());
        catalog
            .create_table("users", initial_schema.clone())
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");

        let result = tx.commit::<DatabaseError, _>(&mut catalog, |_payload| {
            Err(DatabaseError::NotReadyForWrite)
        });

        assert!(result.is_err());
        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema("users"), Some(&initial_schema));
    }

    #[test]
    fn schema_replay_uses_latest_transaction_payload() {

        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("table register should succeed");

        let wal = crate::engine::wal::ConcurrentWalManager::new();
        let actor = crate::core::identity::UserId::from_username("schema-tester");

        let first_schema = TableSchema::new(vec![crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
        }]);

        let first_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 1,
            schema: first_schema,
        };

        wal.append(
            "users",
            crate::TransactionRecord {
                id: crate::TransactionId(1),
                refid: None,
                timestamp_epoch_ms: 1,
                actor: actor.clone(),
                kind: crate::TransactionKind::SchemaChange,
                payload: first_payload.encode().expect("schema payload should encode"),
            },
        )
        .expect("first schema append should succeed");

        let second_schema = TableSchema::new(vec![crate::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);

        let second_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 2,
            schema: second_schema.clone(),
        };

        wal.append(
            "users",
            crate::TransactionRecord {
                id: crate::TransactionId(2),
                refid: None,
                timestamp_epoch_ms: 2,
                actor,
                kind: crate::TransactionKind::SchemaChange,
                payload: second_payload.encode().expect("schema payload should encode"),
            },
        )
        .expect("second schema append should succeed");

        let applied = catalog
            .replay_schema_from_log("users", &wal)
            .expect("schema replay should succeed");

        assert_eq!(applied, 2);
        assert_eq!(catalog.table_schema("users"), Some(&second_schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(2));
        assert!(catalog.index("users:email").is_some());

    }

    #[test]
    fn metadata_and_sql_definition_replay_builds_view_state() {
        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .register_view(
                "users_view",
                "select id from users",
                TableSchema::new(Vec::new()),
            )
            .expect("view register should succeed");

        let wal = crate::engine::wal::ConcurrentWalManager::new();
        let actor = crate::core::identity::UserId::from_username("view-tester");

        let metadata_payload = EntityMetadataPayload {
            entity_id: "users_view".to_string(),
            metadata: EntityMetadata::default()
                .with_creator("alice")
                .with_created_at(100),
        };

        wal.append(
            "main_db",
            crate::TransactionRecord {
                id: crate::TransactionId(1),
                refid: None,
                timestamp_epoch_ms: 100,
                actor: actor.clone(),
                kind: crate::TransactionKind::MetadataChange,
                payload: metadata_payload
                    .encode()
                    .expect("metadata payload should encode"),
            },
        )
        .expect("metadata append should succeed");

        let sql_payload = SqlDefinitionPayload {
            object_id: "users_view".to_string(),
            object_kind: SqlObjectKind::View,
            action: SqlDefinitionAction::Upsert,
            sql: "select id, email from users".to_string(),
            dependencies: vec!["Users".to_string(), "Accounts".to_string()],
        };

        wal.append(
            "main_db",
            crate::TransactionRecord {
                id: crate::TransactionId(2),
                refid: Some(crate::TransactionId(1)),
                timestamp_epoch_ms: 101,
                actor,
                kind: crate::TransactionKind::SqlDefinitionChange,
                payload: sql_payload
                    .encode()
                    .expect("sql payload should encode"),
            },
        )
        .expect("sql append should succeed");

        let applied = catalog
            .replay_entity_construction_from_log("main_db", &wal)
            .expect("replay should succeed");
        assert_eq!(applied, 2);

        let view = catalog.view("users_view").expect("view should exist");
        assert_eq!(view.metadata.created_by.as_deref(), Some("alice"));
        assert_eq!(view.metadata.created_at_epoch_ms, Some(100));
        assert_eq!(view.sql, "select id, email from users");
        assert_eq!(view.dependencies, vec!["users", "accounts"]);
    }

    #[test]
    fn trigger_and_procedure_registration_and_updates_work() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .register_trigger(
                "audit_insert",
                "create trigger audit_insert before insert on users for each row set @x = 1",
                vec!["Users".to_string()],
            )
            .expect("trigger register should succeed");

        catalog
            .register_stored_procedure(
                "refresh_accounts",
                "create procedure refresh_accounts() begin select 1; end",
                vec!["Accounts".to_string()],
            )
            .expect("procedure register should succeed");

        catalog
            .set_sql_definition(
                "audit_insert",
                SqlObjectKind::Trigger,
                "create trigger audit_insert before insert on users for each row set @x = 2",
                vec!["users".to_string(), "logs".to_string()],
            )
            .expect("trigger sql update should succeed");

        catalog
            .set_sql_definition(
                "refresh_accounts",
                SqlObjectKind::StoredProcedure,
                "create procedure refresh_accounts() begin select 2; end",
                vec!["accounts".to_string(), "users".to_string()],
            )
            .expect("procedure sql update should succeed");

        catalog
            .set_entity_metadata(
                "audit_insert",
                EntityMetadata::default().with_creator("ops"),
            )
            .expect("metadata update should succeed");

        let trigger = catalog
            .trigger("audit_insert")
            .expect("trigger should exist");
        assert_eq!(trigger.dependencies, vec!["users", "logs"]);
        assert_eq!(trigger.metadata.created_by.as_deref(), Some("ops"));

        let procedure = catalog
            .stored_procedure("refresh_accounts")
            .expect("procedure should exist");
        assert_eq!(procedure.dependencies, vec!["accounts", "users"]);

        assert_eq!(catalog.trigger_ids(), vec!["audit_insert".to_string()]);
        assert_eq!(
            catalog.stored_procedure_ids(),
            vec!["refresh_accounts".to_string()]
        );
    }

    #[test]
    fn drop_helpers_remove_sql_backed_entities() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .register_view("users_view", "select * from users", TableSchema::new(Vec::new()))
            .expect("view register should succeed");
        catalog
            .register_trigger(
                "audit_insert",
                "create trigger audit_insert before insert on users for each row set @x = 1",
                vec!["users".to_string()],
            )
            .expect("trigger register should succeed");
        catalog
            .register_stored_procedure(
                "refresh_accounts",
                "create procedure refresh_accounts() begin select 1; end",
                vec!["accounts".to_string()],
            )
            .expect("procedure register should succeed");

        catalog.drop_view("users_view").expect("view drop should succeed");
        catalog
            .drop_trigger("audit_insert")
            .expect("trigger drop should succeed");
        catalog
            .drop_stored_procedure("refresh_accounts")
            .expect("procedure drop should succeed");

        assert!(catalog.view("users_view").is_none());
        assert!(catalog.trigger("audit_insert").is_none());
        assert!(catalog.stored_procedure("refresh_accounts").is_none());
    }

    #[test]
    fn entity_aspects_expose_status_and_wal_stream() {

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("table register should succeed");

        catalog
            .register_view("users_view", "select * from users", TableSchema::new(Vec::new()))
            .expect("view register should succeed");

        catalog.register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ));

        assert_eq!(catalog.entity_status("users"), Some(ObjectStatus::Load));
        assert_eq!(catalog.entity_wal_stream_id("users"), Some("users".to_string()));
        assert_eq!(catalog.entity_schema_revision("users"), Some(0));

        assert_eq!(
            catalog.entity_wal_stream_id("users_view"),
            Some(catalog.database_id.0.clone())
        );
        assert_eq!(catalog.entity_schema_revision("users_view"), None);

        assert_eq!(
            catalog.entity_wal_stream_id("rel:users:accounts:owns"),
            Some(catalog.database_id.0.clone())
        );

    }

    #[test]
    fn normalize_loaded_entities_rekeys_and_rebuilds_indexes() {

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        let schema = TableSchema::new(vec![crate::FieldDef {
            seqno: 1,
            field_name: "UserId".to_string(),
            field_type: crate::FieldType::UInt(64),
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);

        catalog
            .register_table("Users", schema)
            .expect("table register should succeed");

        let entity = catalog
            .entities
            .remove("users")
            .expect("expected normalized table entry");
        catalog.entities.insert("Users".to_string(), entity);

        catalog
            .normalize_loaded_entities()
            .expect("normalization should succeed");

        assert!(catalog.entities.contains_key("users"));
        assert!(!catalog.entities.contains_key("Users"));
        assert!(catalog.index("users:userid").is_some());

    }

    #[test]
    fn object_accessor_routes_all_supported_types() {

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        let schema = TableSchema::new(vec![crate::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);

        catalog
            .register_table("users", schema.clone())
            .expect("table register should succeed");
        catalog
            .register_view("users_view", "select * from users", schema)
            .expect("view register should succeed");
        catalog.register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ));
        catalog
            .register_trigger(
                "audit_insert",
                "create trigger audit_insert before insert on users for each row set @x = 1",
                vec!["users".to_string()],
            )
            .expect("trigger register should succeed");
        catalog
            .register_stored_procedure(
                "refresh_accounts",
                "create procedure refresh_accounts() begin select 1; end",
                vec!["accounts".to_string()],
            )
            .expect("procedure register should succeed");

        assert!(matches!(
            catalog.object(DatabaseObjectType::Table, "users"),
            Some(DatabaseObjectRef::Table(_))
        ));
        
        assert!(matches!(
            catalog.object(DatabaseObjectType::View, "users_view"),
            Some(DatabaseObjectRef::View(_))
        ));
        
        assert!(matches!(
            catalog.object(DatabaseObjectType::Relationship, "rel:users:accounts:owns"),
            Some(DatabaseObjectRef::Relationship(_))
        ));

        assert!(matches!(
            catalog.object(DatabaseObjectType::Trigger, "audit_insert"),
            Some(DatabaseObjectRef::Trigger(_))
        ));

        assert!(matches!(
            catalog.object(DatabaseObjectType::StoredProcedure, "refresh_accounts"),
            Some(DatabaseObjectRef::StoredProcedure(_))
        ));
        
        assert!(matches!(
            catalog.object(DatabaseObjectType::Index, "users:email"),
            Some(DatabaseObjectRef::Index(_))
        ));

    }

    #[test]
    fn object_by_index_returns_untyped_object_by_id() {

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        let schema = TableSchema::new(vec![crate::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);

        catalog
            .register_table("users", schema.clone())
            .expect("table register should succeed");

        catalog
            .register_view("users_view", "select * from users", schema)
            .expect("view register should succeed");

        catalog.register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ));

        assert!(matches!(catalog.object_by_id("users"), Some(DatabaseObjectRef::Table(_))));
        assert!(matches!(catalog.object_by_id("users_view"), Some(DatabaseObjectRef::View(_))));
        assert!(matches!(
            catalog.object_by_id("rel:users:accounts:owns"),
            Some(DatabaseObjectRef::Relationship(_))
        ));

        catalog
            .register_trigger(
                "audit_insert",
                "create trigger audit_insert before insert on users for each row set @x = 1",
                vec!["users".to_string()],
            )
            .expect("trigger register should succeed");

        catalog
            .register_stored_procedure(
                "refresh_accounts",
                "create procedure refresh_accounts() begin select 1; end",
                vec!["accounts".to_string()],
            )
            .expect("procedure register should succeed");

        assert!(matches!(
            catalog.object_by_id("audit_insert"),
            Some(DatabaseObjectRef::Trigger(_))
        ));

        assert!(matches!(
            catalog.object_by_id("refresh_accounts"),
            Some(DatabaseObjectRef::StoredProcedure(_))
        ));
        
        assert!(matches!(
            catalog.object_by_id("users:email"),
            Some(DatabaseObjectRef::Index(_))
        ));

        assert!(catalog.object_by_id("missing_object").is_none());
    
    }

}
