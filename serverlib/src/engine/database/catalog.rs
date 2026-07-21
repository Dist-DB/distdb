
use std::collections::HashMap;
use std::path::Path;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, write_bytes};

use crate::engine::database::core::{DatabaseError, DatabaseResult, ObjectStatus};
use crate::engine::database::entity::database_entity::DatabaseEntity;
use crate::engine::database::entity::handle::EntityHandle;
use crate::engine::database::entity::aspect::DatabaseEntityAspect;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::entity::object_ref::DatabaseObjectRef;
use crate::engine::database::entity::object_type::DatabaseObjectType;
use crate::engine::database::id::DatabaseId;
use crate::engine::database::index_id::IndexId;
use crate::engine::database::index::{DatabaseIndex, DatabaseIndexKind, DatabaseIndexOrigin};
use crate::engine::database::index_lifecycle_payload::{IndexLifecycleAction, IndexLifecyclePayload};
use crate::engine::database::relationship::DatabaseRelationship;
use crate::engine::database::schema::change_tx::SchemaChangeTx;
use crate::engine::database::schema::migration::{run_schema_migration, SchemaMigrationExecutor};
use crate::engine::database::schema::change_state::{ActiveSchemaChange, SchemaChangePhase};
use crate::engine::database::stored_procedure::DatabaseStoredProcedure;
use crate::engine::database::table::DatabaseTable;
use crate::engine::database::table::lifecycle_payload::{TableLifecycleAction, TableLifecyclePayload};
use crate::engine::database::table::schema::{FieldIndex, TableSchema};
use crate::engine::database::trigger::DatabaseTrigger;

use crate::engine::database::transaction::{
    DecodedTransactionPayload,
    EntityMetadataPayload, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
    SqlObjectKind, TransactionKind, TransactionLog,
};

use crate::engine::database::view::DatabaseView;
use crate::engine::database::olap_view::DatabaseOlapView;
use crate::engine::security::{AccountAclEntry, PrivilegeSelector, UserCredential};
use crate::engine::sql::{TriggerEventKind, TriggerTiming};
use crate::core::identity::UserId;

const ROOT_USER_ID: &str = "root";

fn normalize_acl_user_key(user_id: &str) -> String {
    user_id.trim().to_ascii_lowercase()
}


#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecursiveCteExecutionSettings {
    pub max_iterations: usize,
    pub max_rows: usize,
    pub timeout_ms: u64,
    pub detect_repeating_union_all_frontier: bool,
}

impl Default for RecursiveCteExecutionSettings {
    
    fn default() -> Self {
        Self {
            max_iterations: 128,
            max_rows: 50_000,
            timeout_ms: 0,
            detect_repeating_union_all_frontier: true,
        }
    }

}

impl RecursiveCteExecutionSettings {
    
    pub fn sanitized(mut self) -> Self {
        if self.max_iterations == 0 {
            self.max_iterations = 1;
        }
        if self.max_rows == 0 {
            self.max_rows = 1;
        }
        self
    }

}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    database_name: String,
    at_rest_encryption_key_ref: Option<String>,
    at_rest_encryption_key_version: u32,
    status: ObjectStatus,
    schema_epoch: u64,
    active_schema_change: Option<ActiveSchemaChange>,
    account_acl_entries: HashMap<String, AccountAclEntry>,
    user_credentials: HashMap<String, UserCredential>,
    recursive_cte_execution_settings: RecursiveCteExecutionSettings,
    entity_handles: HashMap<String, EntityHandle>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct DatabaseCatalogSnapshot {
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
    #[serde(default)]
    account_acl_entries: HashMap<String, AccountAclEntry>,
    #[serde(default)]
    user_credentials: HashMap<String, UserCredential>,
    #[serde(default)]
    recursive_cte_execution_settings: RecursiveCteExecutionSettings,
    entities: HashMap<String, DatabaseEntity>,
}

impl DatabaseCatalog {

    fn entity_snapshot_by_key(&self, entity_id: &str) -> Option<DatabaseEntity> {
        self.entity_handles().get(entity_id).map(EntityHandle::snapshot)
    }

    fn entity_handles(&self) -> &HashMap<String, EntityHandle> {
        &self.entity_handles
    }

    fn materialize_entities(&self) -> HashMap<String, DatabaseEntity> {
        self.entity_handles
            .iter()
            .map(|(entity_id, handle)| (entity_id.clone(), handle.snapshot()))
            .collect()
    }

    fn index_snapshot_by_id(&self, index_id: &str) -> Option<DatabaseIndex> {
        self.entity_handles().values().find_map(|handle| {
            handle
                .read_table(|table| table.indexes.get(index_id).cloned())
                .flatten()
        })
    }

    fn with_table_read<R>(
        &self,
        table_id: &str,
        apply: impl FnOnce(&DatabaseTable) -> R,
    ) -> Option<R> {
        let key = self.resolve_entity_key(table_id)?;
        let handle = self.entity_handles().get(&key)?;
        handle.read_table(apply)
    }

    fn with_entity_read<R>(
        &self,
        entity_id: &str,
        apply: impl FnOnce(&DatabaseEntity) -> R,
    ) -> Option<R> {
        let key = self.resolve_entity_key(entity_id)?;
        let handle = self.entity_handles().get(&key)?;
        Some(handle.read(apply))
    }

    fn object_ref_from_entity(entity: DatabaseEntity) -> DatabaseObjectRef {

        match entity {

            DatabaseEntity::Table(table)                          => DatabaseObjectRef::Table(table),

            DatabaseEntity::View(view)                             => DatabaseObjectRef::View(view),

            DatabaseEntity::OlapView(view)                     => DatabaseObjectRef::OlapView(view),

            DatabaseEntity::Relationship(relationship)     => DatabaseObjectRef::Relationship(relationship),

            DatabaseEntity::Trigger(trigger)                    => DatabaseObjectRef::Trigger(trigger),

            DatabaseEntity::StoredProcedure(procedure)  => DatabaseObjectRef::StoredProcedure(procedure),

        }
    }

    fn object_ref_from_typed_entity(
        object_type: DatabaseObjectType,
        entity: DatabaseEntity,
    ) -> Option<DatabaseObjectRef> {

        match (object_type, entity) {

            (DatabaseObjectType::Table, DatabaseEntity::Table(table)) => {
                Some(DatabaseObjectRef::Table(table))
            },

            (DatabaseObjectType::View, DatabaseEntity::View(view)) => {
                Some(DatabaseObjectRef::View(view))
            },

            (DatabaseObjectType::OlapView, DatabaseEntity::OlapView(view)) => {
                Some(DatabaseObjectRef::OlapView(view))
            },

            (DatabaseObjectType::Relationship, DatabaseEntity::Relationship(relationship)) => {
                Some(DatabaseObjectRef::Relationship(relationship))
            },

            (DatabaseObjectType::Trigger, DatabaseEntity::Trigger(trigger)) => {
                Some(DatabaseObjectRef::Trigger(trigger))
            },

            (
                DatabaseObjectType::StoredProcedure,
                DatabaseEntity::StoredProcedure(procedure),
            ) => Some(DatabaseObjectRef::StoredProcedure(procedure)),

            _ => None,

        }

    }

    fn has_sql_object_kind(&self, object_id: &str, object_kind: SqlObjectKind) -> bool {

        self.with_entity_read(object_id, |entity| {
            matches!(
                (object_kind, entity),
                (SqlObjectKind::View, DatabaseEntity::View(_)) |
                (SqlObjectKind::OlapView, DatabaseEntity::OlapView(_)) |
                (SqlObjectKind::Trigger, DatabaseEntity::Trigger(_)) |
                (SqlObjectKind::StoredProcedure, DatabaseEntity::StoredProcedure(_))
            )
        })
        .unwrap_or(false)

    }

    fn has_table(&self, table_id: &str) -> bool {

        self.with_entity_read(table_id, |entity| matches!(entity, DatabaseEntity::Table(_)))
            .unwrap_or(false)

    }

    fn insert_entity_handle(&mut self, entity: DatabaseEntity) -> DatabaseResult<()> {

        let storage_key = entity.storage_key();

        if self.entity_handles().contains_key(&storage_key) {
            return Err(DatabaseError::DuplicateEntity);
        }

        self.entity_handles
            .insert(storage_key, EntityHandle::new(entity));

        Ok(())

    }

    fn with_table_mut<R>(
        &mut self,
        table_id: &str,
        apply: impl FnOnce(&mut DatabaseTable) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {

        let key = self
            .resolve_entity_key(table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        let handle = self
            .entity_handles()
            .get(&key)
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;

        handle.mutate(|entity| match entity {
            DatabaseEntity::Table(table) => apply(table),
            _ => Err(DatabaseError::TableNotFound),
        })

    }

    fn with_view_mut<R>(
        &mut self,
        view_id: &str,
        apply: impl FnOnce(&mut DatabaseView) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {

        let key = self
            .resolve_entity_key(view_id)
            .ok_or(DatabaseError::ViewNotFound)?;

        let handle = self
            .entity_handles()
            .get(&key)
            .cloned()
            .ok_or(DatabaseError::ViewNotFound)?;

        handle.mutate(|entity| match entity {
            DatabaseEntity::View(view) => apply(view),
            _ => Err(DatabaseError::ViewNotFound),
        })

    }

    fn with_olap_view_mut<R>(
        &mut self,
        view_id: &str,
        apply: impl FnOnce(&mut DatabaseOlapView) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {
        
        let key = self
            .resolve_entity_key(view_id)
            .ok_or(DatabaseError::OlapViewNotFound)?;
        
        let handle = self
            .entity_handles()
            .get(&key)
            .cloned()
            .ok_or(DatabaseError::OlapViewNotFound)?;

        handle.mutate(|entity| match entity {
            DatabaseEntity::OlapView(view) => apply(view),
            _ => Err(DatabaseError::OlapViewNotFound),
        })

    }

    fn with_trigger_mut<R>(
        &mut self,
        trigger_id: &str,
        apply: impl FnOnce(&mut DatabaseTrigger) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {
        
        let key = self
            .resolve_entity_key(trigger_id)
            .ok_or(DatabaseError::TriggerNotFound)?;
        
        let handle = self
            .entity_handles()
            .get(&key)
            .cloned()
            .ok_or(DatabaseError::TriggerNotFound)?;

        handle.mutate(|entity| match entity {
            DatabaseEntity::Trigger(trigger) => apply(trigger),
            _ => Err(DatabaseError::TriggerNotFound),
        })

    }

    fn with_stored_procedure_mut<R>(
        &mut self,
        procedure_id: &str,
        apply: impl FnOnce(&mut DatabaseStoredProcedure) -> DatabaseResult<R>,
    ) -> DatabaseResult<R> {
        
        let key = self
            .resolve_entity_key(procedure_id)
            .ok_or(DatabaseError::StoredProcedureNotFound)?;
        
        let handle = self
            .entity_handles()
            .get(&key)
            .cloned()
            .ok_or(DatabaseError::StoredProcedureNotFound)?;

        handle.mutate(|entity| match entity {
            DatabaseEntity::StoredProcedure(procedure) => apply(procedure),
            _ => Err(DatabaseError::StoredProcedureNotFound),
        })

    }

    fn snapshot(&self) -> DatabaseCatalogSnapshot {

        DatabaseCatalogSnapshot {
            database_id: self.database_id.clone(),
            database_name: self.database_name.clone(),
            at_rest_encryption_key_ref: self.at_rest_encryption_key_ref.clone(),
            at_rest_encryption_key_version: self.at_rest_encryption_key_version,
            status: self.status,
            schema_epoch: self.schema_epoch,
            active_schema_change: self.active_schema_change.clone(),
            account_acl_entries: self.account_acl_entries.clone(),
            user_credentials: self.user_credentials.clone(),
            recursive_cte_execution_settings: self.recursive_cte_execution_settings.clone(),
            entities: self.materialize_entities(),
        }
        
    }

    fn register_table_with_entity_id(
        &mut self,
        table_id: String,
        schema: TableSchema,
        entity_id: Option<String>,
    ) -> DatabaseResult<()> {

        schema
            .validate()
            .map_err(DatabaseError::SchemaChange)?;

        if self.resolve_entity_key(&table_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let indexes = Self::indexes_for_schema(&table_id, &schema);
        let mut table = DatabaseTable::new(table_id.clone(), schema, indexes);

        if let Some(entity_id) = entity_id {
            let normalized_entity_id = common::normalize_identifier!(entity_id);

            if !normalized_entity_id.is_empty() {
                table.set_entity_id(normalized_entity_id);
            }
        }

        self.insert_entity_handle(DatabaseEntity::Table(table))?;

        Ok(())

    }

    fn create_table_with_entity_id(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
        entity_id: Option<String>,
    ) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id.into());
        self.register_table_with_entity_id(table_id.clone(), schema, entity_id)?;

        self.with_table_mut(&table_id, |table| {
            table.begin_sync()?;
            Ok(())
        })?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        self.with_table_mut(&table_id, |table| {
            table.complete_sync()?;
            Ok(())
        })?;
        self.bump_schema_epoch();

        Ok(())

    }

    fn resolve_entity_key(&self, entity_id: &str) -> Option<String> {

        if self.entity_handles().contains_key(entity_id) {
            return Some(entity_id.to_string());
        }

        let relationship_target = entity_id.strip_prefix("rel:").and_then(|rest| {
            let (left, right_and_name) = rest.split_once(':')?;
            let (right, name) = right_and_name.split_once(':')?;
            Some((left, right, name))
        });

        self.entity_handles().iter().find_map(|(key, handle)| {
            handle.read(|entity| {
                if let Some((left, right, name)) = relationship_target
                    && let DatabaseEntity::Relationship(relationship) = entity
                    && relationship.left_table_id == left
                    && relationship.right_table_id == right
                    && relationship.relation_name == name
                {
                    return Some(key.clone());
                }

                if entity.name() == entity_id {
                    return Some(key.clone());
                }

                None
            })
        })

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
            account_acl_entries: HashMap::new(),
            user_credentials: HashMap::new(),
            recursive_cte_execution_settings: RecursiveCteExecutionSettings::default(),
            entity_handles: HashMap::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> DatabaseResult<Self> {
        let database_id = DatabaseId::from_database_name(name)?;
        let mut catalog = Self::new(database_id);
        catalog.database_name = common::normalize_identifier!(name);
        catalog.ensure_default_root_account_acl();
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
        self.register_table_with_entity_id(table_id, schema, None)
        
    }

    pub fn create_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {

        self.create_table_with_entity_id(table_id, schema, None)

    }

    pub fn create_temporary_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id.into());
        self.create_table_with_entity_id(table_id.clone(), schema, None)?;

        self.with_table_mut(&table_id, |table| {
            table.temporary = true;
            Ok(())
        })?;

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
                
                DatabaseObjectType::OlapView => DatabaseError::OlapViewNotFound,
                
                DatabaseObjectType::Trigger => DatabaseError::TriggerNotFound,
                
                DatabaseObjectType::StoredProcedure => DatabaseError::StoredProcedureNotFound,
                
                DatabaseObjectType::Relationship | 
                DatabaseObjectType::Index => {
                    DatabaseError::EntityNotFound
                }
                
            });

        };

        let matches_type = self
            .entity_handles()
            .get(&resolved_key)
            .map(|handle| {
                
                handle.read(|entity| {

                    matches!(
                        
                        (object_type, entity),

                        (DatabaseObjectType::Table, DatabaseEntity::Table(_)) |
                        (DatabaseObjectType::View, DatabaseEntity::View(_)) |
                        (DatabaseObjectType::OlapView, DatabaseEntity::OlapView(_)) |
                        (DatabaseObjectType::Trigger, DatabaseEntity::Trigger(_)) |
                        (DatabaseObjectType::StoredProcedure, DatabaseEntity::StoredProcedure(_)) |
                        (DatabaseObjectType::Relationship, DatabaseEntity::Relationship(_))

                    )

                })

            })
            .unwrap_or(false);

        if !matches_type {

            return Err(match object_type {

                DatabaseObjectType::Table           => DatabaseError::TableNotFound,
                
                DatabaseObjectType::View            => DatabaseError::ViewNotFound,
                
                DatabaseObjectType::OlapView        => DatabaseError::OlapViewNotFound,
                
                DatabaseObjectType::Trigger         => DatabaseError::TriggerNotFound,
                
                DatabaseObjectType::StoredProcedure => DatabaseError::StoredProcedureNotFound,
                
                DatabaseObjectType::Relationship |                 
                DatabaseObjectType::Index           => {
                    DatabaseError::EntityNotFound
                }

            });
            
        }

        self.entity_handles.remove(&resolved_key);
        self.bump_schema_epoch();

        Ok(())

    }

    pub fn drop_table(&mut self, table_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::Table, table_id)
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) -> DatabaseResult<()> {

        let entity_id = relationship.storage_key();

        if self.entity_handles().contains_key(&entity_id) {
            return Err(DatabaseError::DuplicateEntity);
        }

        self.insert_entity_handle(DatabaseEntity::Relationship(relationship))?;
        
        self.bump_schema_epoch();

        Ok(())

    }

    pub fn table(&self, table_id: &str) -> Option<DatabaseTable> {

        self.with_table_read(table_id, Clone::clone)

    }

    pub fn table_handle(&self, table_id: &str) -> Option<EntityHandle> {
        let key = self.resolve_entity_key(table_id)?;
        let handle = self.entity_handles().get(&key)?.clone();
        handle.read_table(|_| ())?;
        Some(handle)
    }

    pub fn index(&self, index_id: &str) -> Option<DatabaseIndex> {

        self.index_snapshot_by_id(index_id)

    }

    pub fn index_in_table(&self, table_id: &str, index_id: &str) -> Option<DatabaseIndex> {

        self.with_table_read(table_id, |table| table.indexes.get(index_id).cloned())
            .flatten()

    }

    pub fn create_index(
        &mut self,
        table_id: &str,
        index_name: Option<&str>,
        field_names: Vec<String>,
    ) -> DatabaseResult<String> {

        self.create_index_with_kind_and_origin(
            table_id,
            index_name,
            field_names,
            DatabaseIndexKind::Indexed,
            DatabaseIndexOrigin::UserDefined,
        )

    }

    pub fn create_index_with_kind_and_origin(
        &mut self,
        table_id: &str,
        index_name: Option<&str>,
        field_names: Vec<String>,
        index_kind: DatabaseIndexKind,
        index_origin: DatabaseIndexOrigin,
    ) -> DatabaseResult<String> {

        let normalized_table_id = common::normalize_identifier!(table_id);

        let normalized_fields = field_names
            .into_iter()
            .map(|field| common::normalize_identifier!(field))
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();

        if normalized_fields.is_empty() {
            return Err(DatabaseError::SchemaChange(
                crate::engine::database::schema::error::SchemaError::FieldNotFound,
            ));
        }

        let index_id = self.with_table_mut(&normalized_table_id, |table| {

            for field_name in &normalized_fields {
                if table.schema.field(field_name).is_none() {
                    return Err(DatabaseError::SchemaChange(
                        crate::engine::database::schema::error::SchemaError::FieldNotFound,
                    ));
                }
            }

            let mut index = DatabaseIndex::from_table_fields_with_origin(
                &normalized_table_id,
                index_kind,
                index_origin,
                None,
                normalized_fields,
            );

            if let Some(index_name) = index_name {
                let normalized_name = common::normalize_identifier!(index_name);
                if normalized_name.is_empty() {
                    return Err(DatabaseError::DuplicateEntity);
                }
                index.index_id = IndexId(normalized_name);
            }

            let index_id = index.index_id.0.clone();

            if table.indexes.contains_key(&index_id) {
                return Err(DatabaseError::DuplicateEntity);
            }

            table.indexes.insert(index_id.clone(), index);
            
            Ok(index_id)

        })?;

        self.bump_schema_epoch();

        Ok(index_id)

    }

    pub fn drop_index(
        &mut self,
        index_name: &str,
        table_id: Option<&str>,
    ) -> DatabaseResult<()> {

        let normalized_index_name = common::normalize_identifier!(index_name);

        if normalized_index_name.is_empty() {
            return Err(DatabaseError::EntityNotFound);
        }

        if let Some(table_id) = table_id {

            let normalized_table_id = common::normalize_identifier!(table_id);
            self.with_table_mut(&normalized_table_id, |table| {
                if table.indexes.remove(&normalized_index_name).is_none() {
                    return Err(DatabaseError::EntityNotFound);
                }
                Ok(())
            })?;

            self.bump_schema_epoch();
            return Ok(());

        }

        let mut removed = false;

        for handle in self.entity_handles().values() {

            removed = handle.mutate(|entity| {
                if let DatabaseEntity::Table(table) = entity {
                    table.indexes.remove(&normalized_index_name).is_some()
                } else {
                    false
                }
            });

            if removed {
                break;
            }

        }

        if removed {
            self.bump_schema_epoch();
            return Ok(());
        }

        Err(DatabaseError::EntityNotFound)

    }

    pub fn object(&self, object_type: DatabaseObjectType, object_id: &str) -> Option<DatabaseObjectRef> {

        let entity_key = self.resolve_entity_key(object_id);
        let entity = entity_key
            .as_deref()
            .and_then(|key| self.entity_snapshot_by_key(key));
        
        match object_type {

            DatabaseObjectType::Table |
            DatabaseObjectType::View |
            DatabaseObjectType::OlapView |
            DatabaseObjectType::Relationship |
            DatabaseObjectType::Trigger |
            DatabaseObjectType::StoredProcedure => entity
                .and_then(|entity| Self::object_ref_from_typed_entity(object_type, entity)),

            DatabaseObjectType::Index => self.index_snapshot_by_id(object_id).map(DatabaseObjectRef::Index),

        }
        
    }

    /// Return an object by id without requiring the caller to provide an
    /// object type. Entity ids are checked first, then table indexes.
    pub fn object_by_id(&self, object_id: &str) -> Option<DatabaseObjectRef> {

        let entity_key = self.resolve_entity_key(object_id);

        if let Some(entity) = entity_key
            .as_deref()
            .and_then(|key| self.entity_snapshot_by_key(key)) {

            return Some(Self::object_ref_from_entity(entity));

        }

        self.index_snapshot_by_id(object_id)
            .map(DatabaseObjectRef::Index)

    }

    pub fn entity(&self, entity_id: &str) -> Option<DatabaseEntity> {
        self.with_entity_read(entity_id, Clone::clone)
    }

    pub fn entity_handle(&self, entity_id: &str) -> Option<EntityHandle> {
        let key = self.resolve_entity_key(entity_id)?;
        self.entity_handles().get(&key).cloned()
    }

    pub fn entity_kind(&self, entity_id: &str) -> Option<DatabaseEntityKind> {
        self.with_entity_read(entity_id, DatabaseEntity::kind)
    }

    pub fn entity_status(&self, entity_id: &str) -> Option<ObjectStatus> {
        self.with_entity_read(entity_id, DatabaseEntity::status)
    }

    pub fn entity_metadata(&self, entity_id: &str) -> Option<crate::engine::database::entity::metadata::EntityMetadata> {
        self.with_entity_read(entity_id, |entity| entity.metadata().clone())
    }

    pub fn entity_name(&self, entity_id: &str) -> Option<String> {
        self.with_entity_read(entity_id, |entity| entity.name().to_string())
    }

    pub fn entity_wal_stream_id(&self, entity_id: &str) -> Option<String> {
        self.with_entity_read(entity_id, |entity| entity.wal_stream_id(&self.database_id.0))
    }

    pub fn entity_identity_id(&self, entity_id: &str) -> Option<String> {
        self.resolve_entity_key(entity_id)
    }

    pub fn entity_schema_revision(&self, entity_id: &str) -> Option<u64> {
        self.with_entity_read(entity_id, DatabaseEntity::schema_revision)
            .flatten()
    }

    pub fn relationships(&self) -> Vec<DatabaseRelationship> {

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {
                    DatabaseEntity::Relationship(relationship) => Some(relationship.clone()),
                    _ => None,
                })
            })
            .collect()

    }

    pub fn status(&self) -> ObjectStatus {
        self.status
    }

    pub fn schema_epoch(&self) -> u64 {
        self.schema_epoch
    }

    pub fn active_schema_change(&self) -> Option<ActiveSchemaChange> {
        self.active_schema_change.clone()
    }

    pub fn recursive_cte_execution_settings(&self) -> RecursiveCteExecutionSettings {
        self.recursive_cte_execution_settings.clone()
    }

    pub fn configure_recursive_cte_execution_settings(
        &mut self,
        settings: RecursiveCteExecutionSettings,
    ) {
        self.recursive_cte_execution_settings = settings.sanitized();
    }

    pub fn execute_schema_migration<E: SchemaMigrationExecutor>(
        &mut self,
        table_id: &str,
        executor: &E,
    ) -> DatabaseResult<()> {
        run_schema_migration(self, table_id, executor)
    }

    pub fn transition_status(&mut self, next: ObjectStatus) -> DatabaseResult<()> {

        let current = self.status;

        if !current.can_transition_to(next) {
            log::warn!(
                "database catalog status transition rejected: database_id={} current={} next={}",
                self.database_id.0,
                current,
                next,
            );
            return Err(DatabaseError::InvalidStatusTransition);
        }

        self.status = next;

        log::info!(
            "database catalog status changed: database_id={} previous={} next={}",
            self.database_id.0,
            current,
            next,
        );

        Ok(())

    }

    pub fn begin_indexing(&mut self) -> DatabaseResult<()> {

        if self.status == ObjectStatus::Indexing {

            for handle in self.entity_handles().values() {
                handle.mutate(|entity| {
                    if let DatabaseEntity::Table(table) = entity {
                        table.begin_indexing()?;
                    }
                    Ok(())
                })?;
            }

            return Ok(());

        }

        self.transition_status(ObjectStatus::Indexing)?;

        for handle in self.entity_handles().values() {
            handle.mutate(|entity| {
                if let DatabaseEntity::Table(table) = entity {
                    table.begin_indexing()?;
                }
                Ok(())
            })?;
        }

        Ok(())

    }

    pub fn complete_indexing(&mut self) -> DatabaseResult<()> {

        if self.status == ObjectStatus::Ready {

            for handle in self.entity_handles().values() {
                handle.mutate(|entity| {
                    if let DatabaseEntity::Table(table) = entity {
                        table.complete_indexing()?;
                    }
                    Ok(())
                })?;
            }

            return Ok(());

        }

        self.transition_status(ObjectStatus::Ready)?;

        for handle in self.entity_handles().values() {
            handle.mutate(|entity| {
                if let DatabaseEntity::Table(table) = entity
                    && table.status() == ObjectStatus::Indexing
                {
                    table.complete_indexing()?;
                }
                Ok(())
            })?;
        }

        Ok(())

    }

    pub fn table_schema(&self, table_id: &str) -> Option<TableSchema> {
        self.with_table_read(table_id, |table| table.schema().clone())
    }

    pub fn table_schema_revision(&self, table_id: &str) -> Option<u64> {
        self.with_table_read(table_id, DatabaseTable::schema_revision)
    }

    /// Lock `table_id` (`Ready -> Lock`) and return a [`SchemaChangeTx`] that
    /// owns the pending schema mutations. The table stays locked until the
    /// returned transaction is either committed or aborted.
    pub fn begin_schema_change(&mut self, table_id: &str) -> DatabaseResult<SchemaChangeTx> {

        let table_id = common::normalize_identifier!(table_id);

        if self.active_schema_change.is_some() {
            return Err(DatabaseError::SchemaChangeInProgress);
        }

        let (pending_schema, next_revision) = self.with_table_mut(&table_id, |table| {
            let pending_schema = table.schema().clone();
            let next_revision = table.schema_revision() + 1;
            table.lock()?;
            Ok((pending_schema, next_revision))
        })?;

        self.active_schema_change = Some(ActiveSchemaChange::begin(
            table_id.clone(),
            next_revision,
            self.schema_epoch.saturating_add(1),
        ));

        Ok(SchemaChangeTx::new(table_id, next_revision, pending_schema))

    }

    pub fn begin_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id);
        self.with_table_mut(&table_id, |table| {
            table.lock()?;
            Ok(())
        })?;
        Ok(())

    }

    pub fn finalize_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id);
        self.with_table_mut(&table_id, |table| {
            if table.status() != ObjectStatus::Lock {
                return Err(DatabaseError::TableNotLocked);
            }

            table.begin_sync()?;
            Ok(())
        })?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        self.with_table_mut(&table_id, |table| {
            table.complete_sync()?;
            Ok(())
        })?;
        Ok(())

    }

    pub fn abort_table_write(&mut self, table_id: &str) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(table_id);
        self.with_table_mut(&table_id, |table| {
            if table.status() != ObjectStatus::Lock {
                return Err(DatabaseError::TableNotLocked);
            }

            table.abort()?;
            Ok(())
        })?;
        Ok(())

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

        self.with_table_mut(&table_id, |table| {
            table.begin_sync()?;
            Ok(())
        })?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }
        
        self.transition_schema_change_phase(&table_id, SchemaChangePhase::Cutover)?;

        self.with_table_mut(&table_id, |table| {
            table.complete_sync()?;
            Ok(())
        })?;
        
        self.active_schema_change = None;

        Ok(())

    }

    /// Internal: release the lock without changing the schema (`Lock -> Ready`).
    /// Called only from `SchemaChangeTx::abort`.
    pub(crate) fn release_schema_lock(&mut self, table_id: &str) -> DatabaseResult<()> {
        
        let table_id = common::normalize_identifier!(table_id);

        self.with_table_mut(&table_id, |table| {
            table.abort()?;
            Ok(())
        })?;

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
        if !self.has_table(&table_id) {
            self.register_table_with_entity_id(
                table_id.clone(),
                payload.schema.clone(),
                payload.entity_id.clone(),
            )?;
        }

        let changed = self.with_table_mut(&table_id, |table| {
            
            if payload.schema_revision <= table.schema_revision() {
                return Ok(false);
            }

            let indexes =
                Self::merge_indexes_for_schema(&table_id, &payload.schema, &table.indexes);

            table.replace_schema(payload.schema_revision, payload.schema.clone(), indexes);
            
            Ok(true)

        })?;

        if !changed {
            return Ok(());
        }

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
                if !self.has_table(&table_id) {
                    self.register_table_with_entity_id(table_id, schema, payload.entity_id)?;
                }
                self.accept_schema_epoch(payload.schema_epoch);
                Ok(())
            },
            
            TableLifecycleAction::Drop => match self.drop_table(&table_id) {
                Ok(()) | Err(DatabaseError::TableNotFound) => {
                    self.accept_schema_epoch(payload.schema_epoch);
                    Ok(())
                }
                Err(e) => Err(e),
            },

        }

    }

    pub fn apply_index_lifecycle(&mut self, payload: IndexLifecyclePayload) -> DatabaseResult<()> {

        if !self.should_apply_schema_epoch(payload.schema_epoch) {
            return Ok(());
        }

        let table_id = common::normalize_identifier!(payload.table_id);
        let index_id = common::normalize_identifier!(payload.index_id);

        match payload.action {

            IndexLifecycleAction::Create => {

                let Some(mut index) = payload.index else {
                    return Err(DatabaseError::IndexPayloadDeserialize);
                };

                self.with_table_mut(&table_id, |table| {
                    if !index
                        .field_names
                        .iter()
                        .all(|field| table.schema.field(field).is_some())
                    {
                        return Err(DatabaseError::SchemaChange(
                            crate::engine::database::schema::error::SchemaError::FieldNotFound,
                        ));
                    }

                    index.table_id = table_id.clone();
                    index.index_id = IndexId(index_id.clone());
                    index.refresh_index_id();
                    index.index_id = IndexId(index_id.clone());
                    table.indexes.insert(index_id.clone(), index);
                    
                    Ok(())

                })?;

                self.accept_schema_epoch(payload.schema_epoch);
                Ok(())

            },

            IndexLifecycleAction::Drop => {

                self.with_table_mut(&table_id, |table| {
                    table.indexes.remove(&index_id);
                    Ok(())
                })?;

                self.accept_schema_epoch(payload.schema_epoch);
                Ok(())

            },

        }

    }

    pub fn apply_entity_metadata(&mut self, payload: EntityMetadataPayload) -> DatabaseResult<()> {

        let EntityMetadataPayload {
            entity_id,
            metadata,
        } = payload;

        let entity_id = common::normalize_identifier!(entity_id);
        
        let resolved_key = self
            .resolve_entity_key(&entity_id)
            .ok_or(DatabaseError::EntityNotFound)?;

        let handle = self
            .entity_handles()
            .get(&resolved_key)
            .cloned()
            .ok_or(DatabaseError::EntityNotFound)?;

        handle.mutate(|entity| {

            match entity {
                
                DatabaseEntity::Table(table)                          => table.metadata = metadata,
                
                DatabaseEntity::View(view)                             => view.metadata = metadata,
                
                DatabaseEntity::OlapView(view)                     => view.metadata = metadata,
                
                DatabaseEntity::Relationship(relationship)     => relationship.metadata = metadata,
                
                DatabaseEntity::Trigger(trigger)                    => trigger.metadata = metadata,
                
                DatabaseEntity::StoredProcedure(procedure)  => procedure.metadata = metadata,

            }

        });

        Ok(())

    }

    pub fn set_entity_metadata(
        &mut self,
        entity_id: impl Into<String>,
        metadata: crate::engine::database::entity::metadata::EntityMetadata,
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

                let existed_before = self.has_sql_object_kind(&object_id, payload.object_kind);

                let normalized_dependencies = payload
                    .dependencies
                    .into_iter()
                    .map(|dep| common::normalize_identifier!(dep))
                    .collect::<Vec<_>>();

                match payload.object_kind {

                    SqlObjectKind::View => {

                        if !existed_before {
                            self.register_view(
                                object_id.clone(),
                                payload.sql.clone(),
                                TableSchema::new(Vec::new()),
                            )?;

                            self.with_view_mut(&object_id, |view| {
                                view.dependencies = normalized_dependencies;
                                Ok(())
                            })?;

                            return Ok(());
                        }

                        self.with_view_mut(&object_id, |view| {
                            view.sql = payload.sql.clone();
                            view.dependencies = normalized_dependencies;
                            Ok(())
                        })?;
                        
                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }
                        
                        Ok(())
                        
                    },

                    SqlObjectKind::OlapView => {

                        if !existed_before {
                            self.register_olap_view(
                                object_id.clone(),
                                payload.sql.clone(),
                                Vec::new(),
                                TableSchema::new(Vec::new()),
                                normalized_dependencies,
                            )?;

                            return Ok(());
                        }

                        self.with_olap_view_mut(&object_id, |view| {
                            view.sql = payload.sql.clone();
                            view.dependencies = normalized_dependencies;
                            Ok(())
                        })?;

                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }

                        Ok(())

                    },

                    SqlObjectKind::Trigger => {

                        if !existed_before {
                            self.register_trigger(
                                object_id.clone(),
                                payload.sql.clone(),
                                normalized_dependencies,
                            )?;

                            return Ok(());
                        }

                        self.with_trigger_mut(&object_id, |trigger| {
                            trigger.set_sql(payload.sql.clone());
                            trigger.dependencies = normalized_dependencies;
                            Ok(())
                        })?;

                        if existed_before {
                            self.accept_schema_epoch(payload.schema_epoch);
                        }

                        Ok(())

                    },

                    SqlObjectKind::StoredProcedure => {

                        if !existed_before {
                            self.register_stored_procedure(
                                object_id.clone(),
                                payload.sql.clone(),
                                normalized_dependencies,
                            )?;

                            return Ok(());
                        }

                        self.with_stored_procedure_mut(&object_id, |procedure| {
                            procedure.set_sql(payload.sql.clone());
                            procedure.dependencies = normalized_dependencies;
                            Ok(())
                        })?;

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

                SqlObjectKind::OlapView => match self.drop_olap_view(&object_id) {
                    Ok(()) | Err(DatabaseError::OlapViewNotFound) => {
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

    pub fn replay_schema_changes_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {

        log.with_all_records(wal_id, |records| {
            
            let mut applied = 0usize;

            for record in records {

                if !matches!(record.kind, TransactionKind::SchemaChange) {
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

        })

    }

    pub fn replay_schema_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {
        self.replay_schema_changes_from_log(wal_id, log)
    }

    pub fn replay_entity_construction_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {

        log.with_all_records(wal_id, |records| {

            let mut applied = 0usize;

            for record in records {

                match record.kind {

                    TransactionKind::SchemaChange |
                    TransactionKind::TableLifecycle |
                    TransactionKind::IndexLifecycle |
                    TransactionKind::MetadataChange |
                    TransactionKind::SecurityChange |
                    TransactionKind::SqlDefinitionChange => {

                        if matches!(record.kind, TransactionKind::SecurityChange)
                            && crate::engine::database::entity::payload::EntityMetadataPayload::decode(
                                record.payload_logical().unwrap_or_default(),
                            )
                            .is_err()
                        {
                            // Security WAL can carry non-catalog payloads (for example
                            // server bootstrap auth changes). Those are replayed by
                            // server control flow, not catalog reconstruction.
                            continue;
                        }

                        let decoded = DecodedTransactionPayload::decode(
                            record.kind,
                            record
                                .payload_logical()
                                .ok_or_else(|| match record.kind {
                                    
                                    TransactionKind::SchemaChange |
                                    TransactionKind::TableLifecycle => {
                                        DatabaseError::SchemaPayloadDeserialize
                                    },

                                    TransactionKind::IndexLifecycle => {
                                        DatabaseError::IndexPayloadDeserialize
                                    },

                                    TransactionKind::MetadataChange |
                                    TransactionKind::SecurityChange => {
                                        DatabaseError::MetadataPayloadDeserialize
                                    },

                                    TransactionKind::SqlDefinitionChange => {
                                        DatabaseError::SqlDefinitionPayloadDeserialize
                                    },

                                    _ => unreachable!("payload decode dispatch should only map structured transaction kinds"),

                                })?,

                        )
                        .map_err(|_| match record.kind {

                            TransactionKind::SchemaChange | 
                            TransactionKind::TableLifecycle => {
                                DatabaseError::SchemaPayloadDeserialize
                            },

                            TransactionKind::IndexLifecycle => {
                                DatabaseError::IndexPayloadDeserialize
                            },

                            TransactionKind::MetadataChange | 
                            TransactionKind::SecurityChange => {
                                DatabaseError::MetadataPayloadDeserialize
                            },

                            TransactionKind::SqlDefinitionChange => {
                                DatabaseError::SqlDefinitionPayloadDeserialize
                            },

                            _ => unreachable!("payload decode dispatch should only map structured transaction kinds"),

                        })?;

                        match decoded {

                            DecodedTransactionPayload::SchemaChange(payload) => {
                                self.apply_schema_change(payload)?;
                            },

                            DecodedTransactionPayload::TableLifecycle(payload) => {
                                self.apply_table_lifecycle(payload)?;
                            },

                            DecodedTransactionPayload::IndexLifecycle(payload) => {
                                self.apply_index_lifecycle(payload)?;
                            },

                            DecodedTransactionPayload::EntityMetadata(payload) => {
                                self.apply_entity_metadata(payload)?;
                            },

                            DecodedTransactionPayload::SqlDefinition(payload) => {
                                self.apply_sql_definition(payload)?;
                            },

                        }

                        applied += 1;
                    
                    },

                    _ => {}

                }

            }

            Ok(applied)

        })

    }

    pub fn ensure_ready_for_write(&self, table_id: &str) -> DatabaseResult<()> {

        if self.status != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        let table = self
            .with_table_read(table_id, DatabaseTable::status)
            .ok_or(DatabaseError::TableNotFound)?;

        if table != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        Ok(())
        
    }

    pub fn table_status(&self, table_id: &str) -> Option<ObjectStatus> {
        self.with_table_read(table_id, DatabaseTable::status)
    }

    pub fn file_name(&self) -> String {
        FileKind::Catalog.file_name(common::normalize_identifier!(self.database_id.0.clone()))
    }

    pub fn from_file_stem(stem: &str) -> Self {
        Self::new(DatabaseId(common::normalize_identifier!(stem)))
    }

    pub fn table_ids(&self) -> Vec<String> {

        self.entity_handles()
            .values()
            .filter_map(|handle| handle.read_table(|table| table.table_id.clone()))
            .collect()

    }

    pub fn entities_iter(&self) -> impl Iterator<Item = (String, DatabaseEntity)> {

        self.materialize_entities().into_iter()

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
        self.insert_entity_handle(DatabaseEntity::View(view))?;

        self.bump_schema_epoch();

        Ok(())
    }

    pub fn view(&self, view_id: &str) -> Option<DatabaseView> {
        self.with_entity_read(view_id, |entity| match entity {
            DatabaseEntity::View(view) => Some(view.clone()),
            _ => None,
        })
        .flatten()

    }

    pub fn drop_view(&mut self, view_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::View, view_id)
    }

    /// Register a new OLAP view definition. The `z_dimension_columns` must
    /// name fields present in the SELECT schema. Schema validation is the
    /// caller's responsibility at `CREATE OLAPVIEW` time.
    pub fn register_olap_view(
        &mut self,
        view_id: impl Into<String>,
        sql: impl Into<String>,
        z_dimension_columns: Vec<String>,
        schema: TableSchema,
        dependencies: Vec<String>,
    ) -> DatabaseResult<()> {

        let view_id = common::normalize_identifier!(view_id.into());

        if self.resolve_entity_key(&view_id).is_some() {
            return Err(DatabaseError::DuplicateEntity);
        }

        let view = DatabaseOlapView::new(
            view_id.clone(),
            sql.into(),
            z_dimension_columns,
            schema,
            dependencies,
        );
        
        self.insert_entity_handle(DatabaseEntity::OlapView(view))?;

        self.bump_schema_epoch();

        Ok(())

    }

    pub fn olap_view(&self, view_id: &str) -> Option<DatabaseOlapView> {
        self.with_entity_read(view_id, |entity| match entity {
            DatabaseEntity::OlapView(view) => Some(view.clone()),
            _ => None,
        })
        .flatten()

    }

    pub fn olap_view_ids(&self) -> Vec<String> {

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {
                    DatabaseEntity::OlapView(view) => Some(view.name().to_string()),
                    _ => None,
                })
            })
            .collect()

    }

    pub fn drop_olap_view(&mut self, view_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::OlapView, view_id)
    }

    pub fn relationship(&self, relationship_id: &str) -> Option<DatabaseRelationship> {

        self.with_entity_read(relationship_id, |entity| match entity {
            DatabaseEntity::Relationship(relationship) => Some(relationship.clone()),
            _ => None,
        })
        .flatten()

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

        self.insert_entity_handle(DatabaseEntity::Trigger(trigger))?;
        self.bump_schema_epoch();

        Ok(())

    }

    pub fn trigger(&self, trigger_id: &str) -> Option<DatabaseTrigger> {
        
        self.with_entity_read(trigger_id, |entity| match entity {
            DatabaseEntity::Trigger(trigger) => Some(trigger.clone()),
            _ => None,
        })
        .flatten()

    }

    pub fn drop_trigger(&mut self, trigger_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::Trigger, trigger_id)
    }

    pub fn trigger_ids(&self) -> Vec<String> {

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {
                    DatabaseEntity::Trigger(trigger) => Some(trigger.name().to_string()),
                    _ => None,
                })
            })
            .collect()

    }

    pub fn triggers_for_event(
        &self,
        table_id: &str,
        timing: TriggerTiming,
        event: TriggerEventKind,
    ) -> Vec<DatabaseTrigger> {

        let normalized_table_id = common::normalize_identifier!(table_id);

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {
                    DatabaseEntity::Trigger(trigger) => trigger
                        .invocation_binding()
                        .filter(|binding| {
                            binding.table_id == normalized_table_id
                                && binding.timing == timing
                                && binding.event == event
                        })
                        .map(|_| trigger.clone()),
                    _ => None,
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
        self.insert_entity_handle(DatabaseEntity::StoredProcedure(procedure))?;

        self.bump_schema_epoch();

        Ok(())

    }

    pub fn stored_procedure(&self, procedure_id: &str) -> Option<DatabaseStoredProcedure> {
        self.with_entity_read(procedure_id, |entity| match entity {
            DatabaseEntity::StoredProcedure(procedure) => Some(procedure.clone()),
            _ => None,
        })
        .flatten()

    }

    pub fn drop_stored_procedure(&mut self, procedure_id: &str) -> DatabaseResult<()> {
        self.drop_object(DatabaseObjectType::StoredProcedure, procedure_id)
    }

    pub fn stored_procedure_ids(&self) -> Vec<String> {

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {

                    DatabaseEntity::StoredProcedure(procedure) => Some(procedure.name().to_string()),

                    _ => None,

                })
            })
            .collect()

    }

    pub fn view_ids(&self) -> Vec<String> {

        self.entity_handles()
            .values()
            .filter_map(|handle| {
                handle.read(|entity| match entity {

                    DatabaseEntity::View(view) => Some(view.name().to_string()),

                    _ => None,
                    
                })
            })
            .collect()

    }

    pub fn view_schema(&self, view_id: &str) -> Option<TableSchema> {
        self.with_entity_read(view_id, |entity| match entity {
            DatabaseEntity::View(view) => Some(view.schema.clone()),
            _ => None,
        })
        .flatten()
    }

    /// Returns `true` for tables, `false` for views. Used at the query
    /// routing layer to reject write operations against view sources before
    /// any execution begins.
    pub fn is_writable(&self, object_id: &str) -> bool {

        self.with_entity_read(object_id, |entity| matches!(entity, DatabaseEntity::Table(_)))
            .unwrap_or(false)

    }

    pub fn load_from_path(path: impl AsRef<Path>) -> DatabaseResult<Self> {

        let bytes = read_bytes(path).map_err(|_| DatabaseError::CatalogRead)?;

        common::helpers::format::verify_header(FileKind::Catalog, &bytes)
            .map_err(|_| DatabaseError::CatalogInvalidHeader)?;

        if bytes.len() <= common::helpers::format::HEADER_SIZE {
            return Err(DatabaseError::CatalogPayloadMissing);
        }

        let snapshot = bincode::deserialize::<DatabaseCatalogSnapshot>(&bytes[common::helpers::format::HEADER_SIZE..])
            .map_err(|_| DatabaseError::CatalogDeserialize)?;

        let mut catalog = Self {
            database_id: snapshot.database_id,
            database_name: snapshot.database_name,
            at_rest_encryption_key_ref: snapshot.at_rest_encryption_key_ref,
            at_rest_encryption_key_version: snapshot.at_rest_encryption_key_version,
            status: snapshot.status,
            schema_epoch: snapshot.schema_epoch,
            active_schema_change: snapshot.active_schema_change,
            account_acl_entries: snapshot.account_acl_entries,
            user_credentials: snapshot.user_credentials,
            recursive_cte_execution_settings: snapshot.recursive_cte_execution_settings,
            entity_handles: HashMap::new(),
        };

        if catalog.database_name.is_empty() {
            catalog.database_name = catalog.database_id.0.clone();
        }

        if catalog.at_rest_encryption_key_ref.is_some()
            && catalog.at_rest_encryption_key_version == 0
        {
            catalog.at_rest_encryption_key_version = 1;
        }

        catalog.normalize_loaded_entities(snapshot.entities)?;
        
        Ok(catalog)

    }

    pub fn save_in_directory(&self, directory: impl AsRef<Path>) -> DatabaseResult<()> {
        let path = directory.as_ref().join(self.file_name());
        self.save_to_path(path)
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn effective_account_acl_entry(&self, user_id: &str) -> Option<AccountAclEntry> {
        let key = normalize_acl_user_key(user_id);
        self.account_acl_entries.get(&key).cloned()
    }

    pub fn effective_account_acl_entries(&self) -> Vec<AccountAclEntry> {
        self.account_acl_entries.values().cloned().collect()
    }

    pub fn upsert_account_acl_entry(&mut self, entry: AccountAclEntry) {
        let key = normalize_acl_user_key(&entry.user_id.0);
        self.account_acl_entries.insert(key, entry);
    }

    pub fn effective_user_credential(&self, user_id: &str) -> Option<UserCredential> {
        let key = normalize_acl_user_key(user_id);
        self.user_credentials.get(&key).cloned()
    }

    pub fn effective_user_credentials(&self) -> Vec<UserCredential> {
        self.user_credentials.values().cloned().collect()
    }

    pub fn upsert_user_credential(&mut self, credential: UserCredential) {
        let key = normalize_acl_user_key(&credential.user_id.0);
        self.user_credentials.insert(key, credential);
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

        let mut snapshot = self.snapshot();
        snapshot.entities.retain(|_, entity| {
            !matches!(entity, DatabaseEntity::Table(table) if table.is_temporary())
        });

        let payload = bincode::serialize(&snapshot).map_err(|_| DatabaseError::CatalogSerialize)?;
        
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
                
                let index_kind = if field
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.unique)
                    .unwrap_or(false)
                {
                    DatabaseIndexKind::Unique
                } else {
                    DatabaseIndexKind::Indexed
                };

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

    fn merge_indexes_for_schema(
        table_id: &str,
        schema: &TableSchema,
        existing_indexes: &HashMap<String, DatabaseIndex>,
    ) -> HashMap<String, DatabaseIndex> {

        let derived = Self::indexes_for_schema(table_id, schema);
        let derived_ids = derived
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        let mut merged = HashMap::new();

        for (index_id, index) in existing_indexes {

            if index.is_temporary() {
                continue;
            }

            let is_schema_derived =
                index.origin == DatabaseIndexOrigin::Derived && derived_ids.contains(index_id);

            if is_schema_derived {
                continue;
            }

            let fields_are_valid = index
                .field_names
                .iter()
                .all(|field_name| schema.field(field_name).is_some());

            if !fields_are_valid {
                continue;
            }

            let mut preserved = index.clone();
            preserved.table_id = common::normalize_identifier!(table_id);
            merged.insert(index_id.clone(), preserved);

        }

        merged.extend(derived);
        merged

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

    fn normalize_loaded_entities(
        &mut self,
        entities: HashMap<String, DatabaseEntity>,
    ) -> DatabaseResult<()> {

        let mut normalized_entities = HashMap::with_capacity(entities.len());
        
        for (_legacy_key, mut entity) in entities {

            if entity.storage_key().is_empty() {
                entity.set_entity_id(common::helpers::utils::unique_id());
            }

            entity.normalize_in_place();

            if let DatabaseEntity::Table(table) = &mut entity {
                table.indexes = Self::merge_indexes_for_schema(
                    &table.table_id,
                    &table.schema,
                    &table.indexes,
                );
            }

            let key = entity.storage_key();
            if normalized_entities.insert(key, entity).is_some() {
                return Err(DatabaseError::CatalogDeserialize);
            }

        }

        self.entity_handles = normalized_entities
            .into_iter()
            .map(|(entity_id, entity)| (entity_id, EntityHandle::new(entity)))
            .collect();
        self.ensure_default_root_account_acl();
        
        Ok(())

    }

    fn ensure_default_root_account_acl(&mut self) {

        let root_key = normalize_acl_user_key(ROOT_USER_ID);

        if self.account_acl_entries.contains_key(&root_key) {
            return;
        }

        let mut root_acl = AccountAclEntry::new(
            UserId(ROOT_USER_ID.to_string()),
            self.database_name.clone(),
        );

        root_acl.append_grant_option_for_selector(&PrivilegeSelector::All);

        self.account_acl_entries
            .insert(root_key, root_acl);
        
    }

}



#[cfg(test)]
#[path = "catalog_test.rs"]
mod tests;
