use std::collections::HashMap;

use crate::{
    ConcurrentWalManager, DatabaseCatalog, TableSchema,
};
use crate::engine::execution::ConditionValueProvider;

use super::scoped_table::ScopedEphemeralTableScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcedureLocalEntity {
    TemporaryTable {
        logical_table_id: String,
        physical_table_id: String,
        dropped: bool,
    },
    Variable {
        value: Vec<u8>,
    },
    Argument {
        value: Vec<u8>,
    },
    Cursor,
}

pub type RoutineLocalEntity = ProcedureLocalEntity;

#[derive(Debug, Clone)]
pub struct ProcedureLocalEntityScope {
    table_scope: ScopedEphemeralTableScope,
    entities: HashMap<String, ProcedureLocalEntity>,
}

pub type RoutineLocalEntityScope = ProcedureLocalEntityScope;

impl ProcedureLocalEntityScope {

    pub fn new(scope_id: impl Into<String>) -> Self {

        Self {
            table_scope: ScopedEphemeralTableScope::new(scope_id),
            entities: HashMap::new(),
        }

    }

    pub fn create_temporary_table(
        &mut self,
        catalog: &mut DatabaseCatalog,
        wal: &ConcurrentWalManager,
        logical_table_id: impl Into<String>,
        schema: TableSchema,
    ) -> Result<String, String> {

        let logical_table_id = common::normalize_identifier!(logical_table_id.into());

        let physical_table_id = self.table_scope.create_table(
            catalog,
            wal,
            logical_table_id.clone(),
            schema,
        )?;

        self.entities.insert(
            logical_table_id.clone(),
            ProcedureLocalEntity::TemporaryTable {
                logical_table_id,
                physical_table_id: physical_table_id.clone(),
                dropped: false,
            },
        );

        Ok(physical_table_id)

    }

    pub fn set_variable(&mut self, variable_name: impl Into<String>, value: Vec<u8>) {

        let variable_name = common::normalize_identifier!(variable_name.into());

        self.entities
            .insert(variable_name, ProcedureLocalEntity::Variable { value });

    }

    pub fn variable_value(&self, variable_name: &str) -> Option<&Vec<u8>> {

        let normalized = common::normalize_identifier!(variable_name);

        match self.entities.get(&normalized) {
            Some(ProcedureLocalEntity::Variable { value }) => Some(value),
            _ => None,
        }

    }

    pub fn remove_variable(&mut self, variable_name: &str) -> bool {

        let normalized = common::normalize_identifier!(variable_name);

        matches!(
            self.entities.remove(&normalized),
            Some(ProcedureLocalEntity::Variable { .. })
        )

    }

    pub fn set_argument(&mut self, argument_name: impl Into<String>, value: Vec<u8>) {

        let argument_name = common::normalize_identifier!(argument_name.into());

        self.entities
            .insert(argument_name, ProcedureLocalEntity::Argument { value });

    }

    pub fn argument_value(&self, argument_name: &str) -> Option<&Vec<u8>> {

        let normalized = common::normalize_identifier!(argument_name);

        match self.entities.get(&normalized) {
            Some(ProcedureLocalEntity::Argument { value }) => Some(value),
            _ => None,
        }

    }

    pub fn resolve_value(&self, name: &str) -> Option<&Vec<u8>> {

        let normalized = common::normalize_identifier!(name);

        match self.entities.get(&normalized) {
            
            Some(ProcedureLocalEntity::Variable { value }) => Some(value),

            Some(ProcedureLocalEntity::Argument { value }) => Some(value),

            _ => None,

        }

    }

    pub fn materialize_value_bindings(&self) -> HashMap<String, Vec<u8>> {

        let mut values = HashMap::new();

        for (name, entity) in &self.entities {

            match entity {

                ProcedureLocalEntity::Variable { value } |
                ProcedureLocalEntity::Argument { value } => {
                    values.insert(name.clone(), value.clone());
                },

                _ => {}

            }

        }

        values

    }

    pub fn resolve_temporary_table_id(&self, logical_name: &str) -> Option<&str> {

        let normalized = common::normalize_identifier!(logical_name);

        match self.entities.get(&normalized) {

            Some(ProcedureLocalEntity::TemporaryTable {
                physical_table_id,
                dropped,
                ..
            }) if !*dropped => Some(physical_table_id.as_str()),

            _ => None,

        }

    }

    pub fn resolve_temporary_table_id_checked(
        &self,
        logical_name: &str,
    ) -> Result<Option<&str>, String> {

        let normalized = common::normalize_identifier!(logical_name);

        match self.entities.get(&normalized) {

            Some(ProcedureLocalEntity::TemporaryTable {
                logical_table_id,
                dropped: true,
                ..
            }) => Err(format!(
                "stored procedure local temporary table '{}' is no longer available",
                logical_table_id,
            )),

            Some(ProcedureLocalEntity::TemporaryTable {
                physical_table_id,
                dropped: false,
                ..
            }) => Ok(Some(physical_table_id.as_str())),

            _ => Ok(None),

        }

    }

    pub fn mark_temporary_table_dropped(&mut self, logical_name: &str) -> bool {

        let normalized = common::normalize_identifier!(logical_name);

        match self.entities.get_mut(&normalized) {

            Some(ProcedureLocalEntity::TemporaryTable {
                physical_table_id,
                dropped,
                ..
            }) if !*dropped => {
                *dropped = true;
                self.table_scope.mark_table_released(physical_table_id)
            },

            _ => false,

        }

    }

    pub fn has_temporary_tables(&self) -> bool {
        self.entities.values().any(|entity| {
            matches!(entity, ProcedureLocalEntity::TemporaryTable { dropped: false, .. })
        })
    }

    pub fn cleanup(
        &mut self,
        catalog: &mut DatabaseCatalog,
        wal: &ConcurrentWalManager,
    ) -> Result<(), String> {

        let result = self.table_scope.cleanup(catalog, wal);

        for entity in self.entities.values_mut() {
            if let ProcedureLocalEntity::TemporaryTable { dropped, .. } = entity {
                *dropped = true;
            }
        }

        self.entities.clear();

        result

    }

}

impl ConditionValueProvider for ProcedureLocalEntityScope {
    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {
        self.resolve_value(field_name)
    }
}

#[cfg(test)]
#[path = "procedure_local_entity_test.rs"]
mod tests;
