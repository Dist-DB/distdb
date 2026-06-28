
use std::collections::HashMap;
use std::path::Path;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, write_bytes};

use super::core::{DatabaseError, DatabaseResult, ObjectStatus};
use super::entity::DatabaseEntity;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_object_ref::DatabaseObjectRef;
use super::entity_object_type::DatabaseObjectType;
use super::id::DatabaseId;
use super::index::{DatabaseIndex, DatabaseIndexKind, DatabaseIndexOrigin};
use super::relationship::DatabaseRelationship;
use super::schema_change_tx::SchemaChangeTx;
use super::schema_migration::{run_schema_migration, SchemaMigrationExecutor};
use super::schema_change_state::{ActiveSchemaChange, SchemaChangePhase};
use super::stored_procedure::DatabaseStoredProcedure;
use super::table::DatabaseTable;
use super::table_lifecycle_payload::{TableLifecycleAction, TableLifecyclePayload};
use super::table_schema::{FieldIndex, TableSchema};
use super::trigger::DatabaseTrigger;
use super::transaction::{
    DecodedTransactionPayload,
    EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
    SqlObjectKind, TransactionKind, TransactionLog,
};
use super::view::DatabaseView;
use crate::engine::sql::{TriggerEventKind, TriggerTiming};



#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    #[serde(default)]
    database_name: String,
    #[serde(default)]
    at_rest_encryption_key_ref: Option<String>,
    #[serde(default)]
    at_rest_encryption_key_version: u32,
    status: ObjectStatus,
    #[serde(default)]
    schema_epoch: u64,
    #[serde(default)]
    active_schema_change: Option<ActiveSchemaChange>,
    entities: HashMap<String, DatabaseEntity>,
}

impl DatabaseCatalog {

    fn resolve_entity_key(&self, entity_id: &str) -> Option<String> {

        if self.entities.contains_key(entity_id) {
            return Some(entity_id.to_string());
        }

        if let Some((key, _)) = self.entities.iter().find(|(_, entity)| match entity {
            DatabaseEntity::Relationship(relationship) => {
                let left = &relationship.left_table_id;
                let right = &relationship.right_table_id;
                let name = &relationship.relation_name;
                entity_id == format!("rel:{left}:{right}:{name}")
            }
            _ => false,
        }) {
            return Some(key.clone());
        }

        self.entities
            .iter()
            .find(|(_, entity)| entity.name() == entity_id)
            .map(|(key, _)| key.clone())

    }
    
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            database_name: String::new(),
            at_rest_encryption_key_ref: None,
            at_rest_encryption_key_version: 0,
            status: ObjectStatus::Load,
            schema_epoch: 0,
            active_schema_change: None,
            entities: HashMap::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> DatabaseResult<Self> {
        let database_id = DatabaseId::from_database_name(name)?;
        let mut catalog = Self::new(database_id);
        catalog.database_name = common::normalize_identifier!(name);
        Ok(catalog)
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

        schema
            .validate()
            .map_err(DatabaseError::SchemaChange)?;

        if self.resolve_entity_key(&table_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let indexes = Self::indexes_for_schema(&table_id, &schema);
        let table = DatabaseTable::new(table_id, schema, indexes);
        let storage_key = table.storage_key();

        self.entities.insert(storage_key, DatabaseEntity::Table(table));

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
        table.complete_sync()?;
        self.bump_schema_epoch();
        
        Ok(())

    }

    pub fn drop_object(
        &mut self,
        object_type: DatabaseObjectType,
        object_id: &str,
    ) -> DatabaseResult<()> {

        let Some(resolved_key) = self.resolve_entity_key(object_id) else {
            return Err(match object_type {
                DatabaseObjectType::Table => DatabaseError::TableNotFound,
                DatabaseObjectType::View => DatabaseError::ViewNotFound,
                DatabaseObjectType::Trigger => DatabaseError::TriggerNotFound,
                DatabaseObjectType::StoredProcedure => DatabaseError::StoredProcedureNotFound,
                DatabaseObjectType::Relationship | DatabaseObjectType::Index => {
                    DatabaseError::EntityNotFound
                }
            });
        };

        let removed = match object_type {
            
            DatabaseObjectType::Table => match self.entities.get(&resolved_key) {
                Some(DatabaseEntity::Table(_)) => {
                    self.entities.remove(&resolved_key);
                    Ok(())
                }
                _ => Err(DatabaseError::TableNotFound),
            },
            
            DatabaseObjectType::View => match self.entities.get(&resolved_key) {
                Some(DatabaseEntity::View(_)) => {
                    self.entities.remove(&resolved_key);
                    Ok(())
                }
                _ => Err(DatabaseError::ViewNotFound),
            },
            
            DatabaseObjectType::Trigger => match self.entities.get(&resolved_key) {
                Some(DatabaseEntity::Trigger(_)) => {
                    self.entities.remove(&resolved_key);
                    Ok(())
                }
                _ => Err(DatabaseError::TriggerNotFound),
            },
            
            DatabaseObjectType::StoredProcedure => match self.entities.get(&resolved_key) {
                Some(DatabaseEntity::StoredProcedure(_)) => {
                    self.entities.remove(&resolved_key);
                    Ok(())
                }
                _ => Err(DatabaseError::StoredProcedureNotFound),
            },
            
            DatabaseObjectType::Relationship => match self.entities.get(&resolved_key) {
                Some(DatabaseEntity::Relationship(_)) => {
                    self.entities.remove(&resolved_key);
                    Ok(())
                }
                _ => Err(DatabaseError::EntityNotFound),
            },

            DatabaseObjectType::Index => Err(DatabaseError::EntityNotFound),

        };

        if removed.is_ok() {
            self.bump_schema_epoch();
        }

        removed

    }

    pub fn drop_table(&mut self, table_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::Table, table_id)
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) -> DatabaseResult<()> {

        let entity_id = relationship.storage_key();

        if self.entities.contains_key(&entity_id) {
            return Err(DatabaseError::DuplicateEntity);
        }
        
        self.entities
            .insert(entity_id, DatabaseEntity::Relationship(relationship));
        
        self.bump_schema_epoch();

        Ok(())

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

        let entity_key = self.resolve_entity_key(object_id);
        
        match object_type {

            DatabaseObjectType::Table => match entity_key.as_deref().and_then(|key| self.entities.get(key)) {
                Some(DatabaseEntity::Table(table)) => Some(DatabaseObjectRef::Table(table)),
                _ => None,
            },
            
            DatabaseObjectType::View => match entity_key.as_deref().and_then(|key| self.entities.get(key)) {
                Some(DatabaseEntity::View(view)) => Some(DatabaseObjectRef::View(view)),
                _ => None,
            },
            
            DatabaseObjectType::Relationship => entity_key.as_deref().and_then(|key| self.entities.get(key)).and_then(|entity| match entity {
                DatabaseEntity::Relationship(relationship) => Some(DatabaseObjectRef::Relationship(relationship)),
                _ => None,
            }),
            
            DatabaseObjectType::Trigger => match entity_key.as_deref().and_then(|key| self.entities.get(key)) {
                Some(DatabaseEntity::Trigger(trigger)) => Some(DatabaseObjectRef::Trigger(trigger)),
                _ => None,
            },

            DatabaseObjectType::StoredProcedure => match entity_key.as_deref().and_then(|key| self.entities.get(key)) {
                Some(DatabaseEntity::StoredProcedure(procedure)) => {
                    Some(DatabaseObjectRef::StoredProcedure(procedure))
                }
                _ => None,
            },

            DatabaseObjectType::Index => {
                self.entities.values().find_map(|entity| match entity {
                    DatabaseEntity::Table(table) => table
                        .indexes
                        .get(object_id)
                        .map(DatabaseObjectRef::Index),
                    _ => None,
                })
            }

        }
        
    }

    /// Return an object by id without requiring the caller to provide an
    /// object type. Entity ids are checked first, then table indexes.
    pub fn object_by_id(&self, object_id: &str) -> Option<DatabaseObjectRef<'_>> {

        let entity_key = self.resolve_entity_key(object_id);

        if let Some(entity) = entity_key.as_deref().and_then(|key| self.entities.get(key)) {
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
            DatabaseEntity::Table(table) => table.indexes.get(object_id).map(DatabaseObjectRef::Index),
            _ => None,
        })

    }

    pub fn entity(&self, entity_id: &str) -> Option<&DatabaseEntity> {
        self.resolve_entity_key(entity_id)
            .and_then(|key| self.entities.get(&key))
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

    pub fn entity_name(&self, entity_id: &str) -> Option<&str> {
        self.entity(entity_id).map(DatabaseEntityAspect::name)
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

    pub fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    pub fn active_schema_change(&self) -> Option<&ActiveSchemaChange> {
        self.active_schema_change.as_ref()
    }

    pub fn execute_schema_migration<E: SchemaMigrationExecutor>(
        &mut self,
        table_id: &str,
        executor: &E,
    ) -> DatabaseResult<()> {
        run_schema_migration(self, table_id, executor)
    }

    pub fn transition_status(&mut self, next: ObjectStatus) -> DatabaseResult<()> {
        if !self.status.can_transition_to(next) {
            return Err(DatabaseError::InvalidStatusTransition);
        }
        self.status = next;
        Ok(())
    }

    pub fn begin_indexing(&mut self) -> DatabaseResult<()> {
        if self.status == ObjectStatus::Indexing {
            for entity in self.entities.values_mut() {
                if let DatabaseEntity::Table(table) = entity {
                    table.begin_indexing()?;
                }
            }

            return Ok(());
        }

        self.transition_status(ObjectStatus::Indexing)?;

        for entity in self.entities.values_mut() {
            if let DatabaseEntity::Table(table) = entity {
                table.begin_indexing()?;
            }
        }

        Ok(())
    }

    pub fn complete_indexing(&mut self) -> DatabaseResult<()> {
        if self.status == ObjectStatus::Ready {
            for entity in self.entities.values_mut() {
                if let DatabaseEntity::Table(table) = entity {
                    table.complete_indexing()?;
                }
            }

            return Ok(());
        }

        self.transition_status(ObjectStatus::Ready)?;

        for entity in self.entities.values_mut() {
            if let DatabaseEntity::Table(table) = entity
                && table.status() == ObjectStatus::Indexing {
                    table.complete_indexing()?;
                }
        }

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

        if self.active_schema_change.is_some() {
            return Err(DatabaseError::SchemaChangeInProgress);
        }

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        let pending_schema = table.schema().clone();
        let next_revision = table.schema_revision() + 1;

        table.lock()?;

        self.active_schema_change = Some(ActiveSchemaChange::begin(
            table_id.clone(),
            next_revision,
            self.schema_epoch.saturating_add(1),
        ));

        Ok(SchemaChangeTx::new(table_id, next_revision, pending_schema))

    }

    pub fn begin_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.lock()

    }

    pub fn finalize_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        if table.status() != ObjectStatus::Lock {
            return Err(DatabaseError::TableNotLocked);
        }

        table.begin_sync()?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()

    }

    pub fn abort_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        if table.status() != ObjectStatus::Lock {
            return Err(DatabaseError::TableNotLocked);
        }

        table.abort()

    }

    pub(crate) fn transition_schema_change_phase(
        &mut self,
        table_id: &str,
        phase: SchemaChangePhase,
    ) -> DatabaseResult<()> {

        let normalized = common::normalize_identifier!(table_id);
        let active = self
            .active_schema_change
            .as_mut()
            .ok_or(DatabaseError::TableNotLocked)?;

        if active.table_id != normalized {
            return Err(DatabaseError::TableNotLocked);
        }

        if !active.phase.can_transition_to(phase) {
            return Err(DatabaseError::InvalidStatusTransition);
        }

        active.phase = phase;
        
        Ok(())

    }

    pub(crate) fn checkpoint_schema_change_progress(
        &mut self,
        table_id: &str,
        rows_rewritten: u64,
        rows_total: Option<u64>,
        resume_token: Option<String>,
    ) -> DatabaseResult<()> {

        let normalized = common::normalize_identifier!(table_id);
        
        let active = self
            .active_schema_change
            .as_mut()
            .ok_or(DatabaseError::TableNotLocked)?;

        if active.table_id != normalized {
            return Err(DatabaseError::TableNotLocked);
        }

        active.update_progress(rows_rewritten, rows_total, resume_token);
        
        Ok(())

    }

    /// Internal: apply a payload and drive `Lock -> Sync -> Ready`.
    /// Called only from `SchemaChangeTx::commit`.
    pub(crate) fn finalize_schema_change(
        &mut self,
        payload: SchemaChangePayload,
    ) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(&payload.table_id);
        self.apply_schema_change(payload)?;
        self.transition_schema_change_phase(&table_id, SchemaChangePhase::Syncing)?;

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }
        
        self.transition_schema_change_phase(&table_id, SchemaChangePhase::Cutover)?;

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()?;
        
        self.active_schema_change = None;

        Ok(())

    }

    /// Internal: release the lock without changing the schema (`Lock -> Ready`).
    /// Called only from `SchemaChangeTx::abort`.
    pub(crate) fn release_schema_lock(&mut self, table_id: &str) -> DatabaseResult<()> {
        
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        
        table.abort()?;

        self.active_schema_change = None;
        
        Ok(())
        
    }

    /// Internal: apply a schema payload directly. Does not enforce or alter
    /// table status. Used by `finalize_schema_change` and WAL replay.
    pub fn apply_schema_change(&mut self, payload: SchemaChangePayload) -> DatabaseResult<()> {

        if !self.should_apply_schema_epoch(payload.schema_epoch) {
            return Ok(());
        }

        let table_id = common::normalize_identifier!(payload.table_id);
        if self.table(&table_id).is_none() {
            self.register_table(table_id.clone(), payload.schema.clone())?;
        }

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        if payload.schema_revision <= table.schema_revision() {
            return Ok(());
        }

        let indexes = Self::indexes_for_schema(&table_id, &payload.schema);

        table.replace_schema(payload.schema_revision, payload.schema, indexes);
        self.accept_schema_epoch(payload.schema_epoch);

        Ok(())

    }

    pub fn apply_table_lifecycle(&mut self, payload: TableLifecyclePayload) -> DatabaseResult<()> {

        if !self.should_apply_schema_epoch(payload.schema_epoch) {
            return Ok(());
        }

        let table_id = common::normalize_identifier!(payload.table_id);

        match payload.action {
            TableLifecycleAction::Create => {
                let schema = payload.schema.unwrap_or_else(|| TableSchema::new(Vec::new()));
                if self.table(&table_id).is_none() {
                    self.register_table(table_id, schema)?;
                }
                self.accept_schema_epoch(payload.schema_epoch);
                Ok(())
            }
            TableLifecycleAction::Drop => match self.drop_table(&table_id) {
                Ok(()) | Err(DatabaseError::TableNotFound) => {
                    self.accept_schema_epoch(payload.schema_epoch);
                    Ok(())
                }
                Err(e) => Err(e),
            },
        }

    }

    pub fn apply_entity_metadata(&mut self, payload: EntityMetadataPayload) -> DatabaseResult<()> {

        let entity_id = common::normalize_identifier!(payload.entity_id);
        let resolved_key = self
            .resolve_entity_key(&entity_id)
            .ok_or(DatabaseError::EntityNotFound)?;
        let entity = self
            .entities
            .get_mut(&resolved_key)
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

        if !self.should_apply_schema_epoch(payload.schema_epoch) {
            return Ok(());
        }

        let object_id = common::normalize_identifier!(payload.object_id);

        match payload.action {

            SqlDefinitionAction::Upsert => {

                let existed_before = match payload.object_kind {
                    SqlObjectKind::View => self.view(&object_id).is_some(),
                    SqlObjectKind::Trigger => self.trigger(&object_id).is_some(),
                    SqlObjectKind::StoredProcedure => self.stored_procedure(&object_id).is_some(),
                };

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
                        
                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }
                        
                        Ok(())
                        
                    },

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

                        trigger.set_sql(payload.sql);
                        trigger.dependencies = normalized_dependencies;

                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }

                        Ok(())

                    },

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
                        procedure.set_sql(payload.sql);
                        procedure.dependencies = normalized_dependencies;

                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }

                        Ok(())
                        
                    },

                }
            
            },

            SqlDefinitionAction::Drop => match payload.object_kind {

                SqlObjectKind::View => match self.drop_view(&object_id) {
                    Ok(()) | Err(DatabaseError::ViewNotFound) => {
                        self.accept_schema_epoch(payload.schema_epoch);
                        Ok(())
                    }
                    Err(e) => Err(e),
                },

                SqlObjectKind::Trigger => match self.drop_trigger(&object_id) {
                    Ok(()) | Err(DatabaseError::TriggerNotFound) => {
                        self.accept_schema_epoch(payload.schema_epoch);
                        Ok(())
                    }
                    Err(e) => Err(e),
                },

                SqlObjectKind::StoredProcedure => match self.drop_stored_procedure(&object_id) {
                    Ok(()) | Err(DatabaseError::StoredProcedureNotFound) => {
                        self.accept_schema_epoch(payload.schema_epoch);
                        Ok(())
                    }
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
            schema_epoch: self.schema_epoch.saturating_add(1),
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

            let payload = SchemaChangePayload::decode(
                record
                    .payload_logical()
                    .ok_or(DatabaseError::SchemaPayloadDeserialize)?,
            )
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
                TransactionKind::SchemaChange
                | TransactionKind::TableLifecycle
                | TransactionKind::MetadataChange
                | TransactionKind::SecurityChange
                | TransactionKind::SqlDefinitionChange => {
                    let decoded = DecodedTransactionPayload::decode(
                        record.kind,
                        record
                            .payload_logical()
                            .ok_or_else(|| match record.kind {
                                TransactionKind::SchemaChange | TransactionKind::TableLifecycle => {
                                    DatabaseError::SchemaPayloadDeserialize
                                }
                                TransactionKind::MetadataChange | TransactionKind::SecurityChange => {
                                    DatabaseError::MetadataPayloadDeserialize
                                }
                                TransactionKind::SqlDefinitionChange => {
                                    DatabaseError::SqlDefinitionPayloadDeserialize
                                }
                                _ => unreachable!("payload decode dispatch should only map structured transaction kinds"),
                            })?,
                    )
                        .map_err(|_| match record.kind {
                            TransactionKind::SchemaChange | TransactionKind::TableLifecycle => {
                                DatabaseError::SchemaPayloadDeserialize
                            }
                            TransactionKind::MetadataChange | TransactionKind::SecurityChange => {
                                DatabaseError::MetadataPayloadDeserialize
                            }
                            TransactionKind::SqlDefinitionChange => {
                                DatabaseError::SqlDefinitionPayloadDeserialize
                            }
                            _ => unreachable!("payload decode dispatch should only map structured transaction kinds"),
                        })?;

                    match decoded {
                        DecodedTransactionPayload::SchemaChange(payload) => {
                            self.apply_schema_change(payload)?;
                        }
                        DecodedTransactionPayload::TableLifecycle(payload) => {
                            self.apply_table_lifecycle(payload)?;
                        }
                        DecodedTransactionPayload::EntityMetadata(payload) => {
                            self.apply_entity_metadata(payload)?;
                        }
                        DecodedTransactionPayload::SqlDefinition(payload) => {
                            self.apply_sql_definition(payload)?;
                        }
                    }

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
            .filter_map(|(_, entity)| match entity {
                DatabaseEntity::Table(table) => Some(table.table_id.clone()),
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

        if self.resolve_entity_key(&view_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let view = DatabaseView::new(view_id.clone(), sql.into(), schema);
        let storage_key = view.storage_key();

        self.entities.insert(storage_key, DatabaseEntity::View(view));

        self.bump_schema_epoch();

        Ok(())
    }

    pub fn view(&self, view_id: &str) -> Option<&DatabaseView> {
        match self.object(DatabaseObjectType::View, view_id) {
            Some(DatabaseObjectRef::View(view)) => Some(view),
            _ => None,
        }
    }

    pub fn drop_view(&mut self, view_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::View, view_id)
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

        if self.resolve_entity_key(&trigger_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let trigger = DatabaseTrigger::new(trigger_id.clone(), sql.into(), dependencies);
        let storage_key = trigger.storage_key();

        self.entities.insert(storage_key, DatabaseEntity::Trigger(trigger));

        self.bump_schema_epoch();

        Ok(())
    }

    pub fn trigger(&self, trigger_id: &str) -> Option<&DatabaseTrigger> {
        match self.object(DatabaseObjectType::Trigger, trigger_id) {
            Some(DatabaseObjectRef::Trigger(trigger)) => Some(trigger),
            _ => None,
        }
    }

    pub fn drop_trigger(&mut self, trigger_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::Trigger, trigger_id)
    }

    pub fn trigger_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(_, entity)| match entity {
                DatabaseEntity::Trigger(trigger) => Some(trigger.name().to_string()),
                _ => None,
            })
            .collect()
    }

    pub fn triggers_for_event(
        &self,
        table_id: &str,
        timing: TriggerTiming,
        event: TriggerEventKind,
    ) -> Vec<&DatabaseTrigger> {
        let normalized_table_id = common::normalize_identifier!(table_id);

        self.entities
            .iter()
            .filter_map(|(_, entity)| match entity {
                DatabaseEntity::Trigger(trigger) => Some(trigger),
                _ => None,
            })
            .filter(|trigger| {
                trigger.invocation_binding().is_some_and(|binding| {
                    binding.table_id == normalized_table_id
                        && binding.timing == timing
                        && binding.event == event
                })
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

        if self.resolve_entity_key(&procedure_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let procedure = DatabaseStoredProcedure::new(procedure_id.clone(), sql.into(), dependencies);
        let storage_key = procedure.storage_key();

        self.entities.insert(storage_key, DatabaseEntity::StoredProcedure(procedure));

        self.bump_schema_epoch();

        Ok(())

    }

    pub fn stored_procedure(&self, procedure_id: &str) -> Option<&DatabaseStoredProcedure> {
        match self.object(DatabaseObjectType::StoredProcedure, procedure_id) {
            Some(DatabaseObjectRef::StoredProcedure(procedure)) => Some(procedure),
            _ => None,
        }
    }

    pub fn drop_stored_procedure(&mut self, procedure_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::StoredProcedure, procedure_id)
    }

    pub fn stored_procedure_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(_, entity)| match entity {
                DatabaseEntity::StoredProcedure(procedure) => {
                    Some(procedure.name().to_string())
                }
                _ => None,
            })
            .collect()
    }

    pub fn view_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(_, entity)| match entity {
                DatabaseEntity::View(view) => Some(view.name().to_string()),
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
        self.resolve_entity_key(object_id)
            .and_then(|key| self.entities.get(&key))
            .is_some_and(|entity| matches!(entity, DatabaseEntity::Table(_)))
    }

    fn table_mut(&mut self, table_id: &str) -> Option<&mut DatabaseTable> {
        let key = self.resolve_entity_key(table_id)?;
        match self.entities.get_mut(&key) {
            Some(DatabaseEntity::Table(table)) => Some(table),
            _ => None,
        }
    }

    fn view_mut(&mut self, view_id: &str) -> Option<&mut DatabaseView> {
        let key = self.resolve_entity_key(view_id)?;
        match self.entities.get_mut(&key) {
            Some(DatabaseEntity::View(view)) => Some(view),
            _ => None,
        }
    }

    fn trigger_mut(&mut self, trigger_id: &str) -> Option<&mut DatabaseTrigger> {
        let key = self.resolve_entity_key(trigger_id)?;
        match self.entities.get_mut(&key) {
            Some(DatabaseEntity::Trigger(trigger)) => Some(trigger),
            _ => None,
        }
    }

    fn stored_procedure_mut(
        &mut self,
        procedure_id: &str,
    ) -> Option<&mut DatabaseStoredProcedure> {
        
        let key = self.resolve_entity_key(procedure_id)?;

        match self.entities.get_mut(&key) {
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

        if catalog.database_name.is_empty() {
            catalog.database_name = catalog.database_id.0.clone();
        }

        if catalog.at_rest_encryption_key_ref.is_some()
            && catalog.at_rest_encryption_key_version == 0
        {
            catalog.at_rest_encryption_key_version = 1;
        }

        catalog.normalize_loaded_entities()?;
        
        Ok(catalog)

    }

    pub fn save_in_directory(&self, directory: impl AsRef<Path>) -> DatabaseResult<()> {
        let path = directory.as_ref().join(self.file_name());
        self.save_to_path(path)
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn at_rest_encryption_enabled(&self) -> bool {
        self.at_rest_encryption_key_ref.is_some()
    }

    pub fn at_rest_encryption_key_ref(&self) -> Option<&str> {
        self.at_rest_encryption_key_ref.as_deref()
    }

    pub fn at_rest_encryption_key_version(&self) -> u32 {
        self.at_rest_encryption_key_version
    }

    pub fn configure_at_rest_encryption_key_ref(
        &mut self,
        key_ref: impl Into<String>,
    ) -> DatabaseResult<()> {
        let normalized = key_ref.into().trim().to_string();
        if normalized.is_empty() {
            return Err(DatabaseError::InvalidEncryptionKeyRef);
        }

        match self.at_rest_encryption_key_ref.as_deref() {
            Some(current) if current == normalized => Ok(()),
            Some(_) => Err(DatabaseError::ImmutableEncryptionConfiguration),
            None => {
                self.at_rest_encryption_key_ref = Some(normalized);
                if self.at_rest_encryption_key_version == 0 {
                    self.at_rest_encryption_key_version = 1;
                }
                Ok(())
            }
        }
    }

    pub fn set_database_name(&mut self, name: &str) {
        self.database_name = name.to_string();
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

        let primary_key_fields = schema
            .fields
            .iter()
            .filter(|field| field.indexed == FieldIndex::PrimaryKey)
            .map(|field| field.field_name.clone())
            .collect::<Vec<_>>();

        if !primary_key_fields.is_empty() {

            let index = DatabaseIndex::from_table_fields_with_origin(
                table_id,
                DatabaseIndexKind::PrimaryKey,
                DatabaseIndexOrigin::Derived,
                None,
                primary_key_fields,
            );
            
            indexes.insert(index.index_id.0.clone(), index);

        }

        for field in &schema.fields {
            if matches!(field.indexed, FieldIndex::Indexed) {
                
                let index_kind = DatabaseIndexKind::Indexed;

                let index = DatabaseIndex::from_table_fields_with_origin(
                    table_id,
                    index_kind,
                    DatabaseIndexOrigin::Derived,
                    None,
                    vec![field.field_name.clone()],
                );
                
                indexes.insert(index.index_id.0.clone(), index);
            }
        }
        
        indexes

    }

    fn bump_schema_epoch(&mut self) {
        self.schema_epoch = self.schema_epoch.saturating_add(1);
    }

    fn should_apply_schema_epoch(&self, incoming_epoch: u64) -> bool {
        incoming_epoch >= self.schema_epoch
    }

    fn accept_schema_epoch(&mut self, incoming_epoch: u64) {
        self.schema_epoch = self.schema_epoch.max(incoming_epoch);
    }

    fn normalize_loaded_entities(&mut self) -> DatabaseResult<()> {

        let mut normalized_entities = HashMap::with_capacity(self.entities.len());
        
        for (_legacy_key, mut entity) in std::mem::take(&mut self.entities) {

            if entity.storage_key().is_empty() {
                entity.set_entity_id(common::helpers::utils::unique_id());                
            }

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
#[path = "catalog_test.rs"]
mod tests;
