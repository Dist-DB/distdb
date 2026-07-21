use std::sync::{Arc, RwLock};

use crate::engine::database::entity::database_entity::DatabaseEntity;
use crate::engine::database::table::DatabaseTable;

#[derive(Debug)]
pub struct EntityHandle {
    entity: Arc<RwLock<DatabaseEntity>>,
}

impl EntityHandle {

    pub fn new(entity: DatabaseEntity) -> Self {
        Self {
            entity: Arc::new(RwLock::new(entity)),
        }
    }

    pub fn snapshot(&self) -> DatabaseEntity {
        self.entity
            .read()
            .expect("entity handle lock should not be poisoned")
            .clone()
    }

    pub fn table_snapshot(&self) -> Option<DatabaseTable> {
        match self.snapshot() {
            DatabaseEntity::Table(table) => Some(table),
            _ => None,
        }
    }

    pub fn read<R>(&self, apply: impl FnOnce(&DatabaseEntity) -> R) -> R {
        let guard = self
            .entity
            .read()
            .expect("entity handle lock should not be poisoned");
        apply(&guard)
    }

    pub fn read_table<R>(&self, apply: impl FnOnce(&DatabaseTable) -> R) -> Option<R> {
        self.read(|entity| match entity {
            DatabaseEntity::Table(table) => Some(apply(table)),
            _ => None,
        })
    }

    pub fn mutate<R>(&self, apply: impl FnOnce(&mut DatabaseEntity) -> R) -> R {
        let mut guard = self
            .entity
            .write()
            .expect("entity handle lock should not be poisoned");
        apply(&mut guard)
    }

}

impl Clone for EntityHandle {

    fn clone(&self) -> Self {
        Self {
            entity: Arc::clone(&self.entity),
        }
    }

}

impl PartialEq for EntityHandle {
    
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
    
}

impl Eq for EntityHandle {}