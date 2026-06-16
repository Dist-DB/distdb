use std::collections::{HashMap, HashSet};

use super::table::DatabaseTable;
use crate::{
    load_live_rows, ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseIndexOrigin,
    TransactionKind,
};

/// In-memory state for a single index.
/// Each entry is a composite key tuple in the index's field order.
#[derive(Debug, Clone, Default)]
pub struct RuntimeIndexState {
    pub index: Option<DatabaseIndex>,
    entries: HashSet<Vec<Vec<u8>>>,
}

impl RuntimeIndexState {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, pk_val: &[Vec<u8>]) -> bool {
        self.entries.contains(pk_val)
    }

    pub fn insert(&mut self, pk_val: Vec<Vec<u8>>) {
        self.entries.insert(pk_val);
    }

    pub fn remove(&mut self, pk_val: &[Vec<u8>]) {
        self.entries.remove(pk_val);
    }

    pub fn cardinality(&self) -> usize {
        self.entries.len()
    }

    pub fn rebuild(&mut self, entries: HashSet<Vec<Vec<u8>>>) {
        self.entries = entries;
    }

}

/// Runtime indexes for all tables across all databases.
#[derive(Debug, Clone, Default)]
pub struct RuntimeIndexStore {
    indexes: HashMap<String, RuntimeIndexState>,
}

impl RuntimeIndexStore {

    pub fn new() -> Self {
        Self::default()
    }

    fn key(index_id: &str) -> String {
        index_id.to_string()
    }

    pub fn index(&self, index_id: &str) -> Option<&RuntimeIndexState> {
        self.indexes.get(&Self::key(index_id))
    }

    #[expect(clippy::should_implement_trait, reason="Index access by string ID, not by reference")]
    pub fn index_mut(&mut self, index_id: &str) -> &mut RuntimeIndexState {
        self.indexes.entry(Self::key(index_id)).or_default()
    }

    pub fn cardinality(&self, index_id: &str) -> Option<usize> {
        self.index(index_id).map(|state| state.cardinality())
    }

    pub fn register_index(&mut self, index: DatabaseIndex) {
        let index_id = index.index_id.0.clone();
        self.indexes.entry(index_id).or_insert_with(|| RuntimeIndexState {
            index: Some(index),
            entries: HashSet::new(),
        });
    }

    pub fn record_row(&mut self, index: &DatabaseIndex, row_map: &HashMap<String, Vec<u8>>) {
        let key = index_value_tuple(index, row_map);
        self.index_mut(&index.index_id.0).insert(key);
    }

    pub fn record_table_row<'a, I>(&mut self, indexes: I, row_map: &HashMap<String, Vec<u8>>)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {
            self.record_row(index, row_map);
        }
    }

    pub fn remove_table_row<'a, I>(&mut self, indexes: I, row_map: &HashMap<String, Vec<u8>>)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {
            let key = index_value_tuple(index, row_map);
            self.index_mut(&index.index_id.0).remove(&key);
        }
    }

    pub fn apply_table_row_mutation<'a, I>(
        &mut self,
        indexes: I,
        kind: TransactionKind,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        match kind {
            TransactionKind::Ignore => {}
            TransactionKind::Delete => self.remove_table_row(indexes, row_map),
            TransactionKind::Insert | TransactionKind::Update => {
                self.record_table_row(indexes, row_map)
            }
            _ => {}
        }
    }

    /// Populate indexes for every table in every catalog by replaying their WALs.
    /// Should be called once during server bootstrap after catalogs are loaded.
    pub fn bootstrap_from_catalogs(
        &mut self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        wal: &ConcurrentWalManager,
    ) {

        for (database_id, catalog) in catalogs {
            
            for table_id in catalog.table_ids() {

                let Some(table) = catalog.table(&table_id) else {
                    continue;
                };

                if table.indexes.is_empty() {
                    continue;
                }

                for index in table.indexes.values() {
                    self.register_index(index.clone());
                }

                let live_rows = load_live_rows(wal, &table_id, &table.schema);
                for index in table.indexes.values() {
                    let state = self.index_mut(&index.index_id.0);
                    state.rebuild(
                        live_rows
                            .iter()
                            .map(|(_, row_map)| index_value_tuple(index, row_map))
                            .collect(),
                    );
                    state.index = Some(index.clone());
                }

                log::debug!(
                    "runtime index bootstrapped database={} table={} indexes={}",
                    database_id,
                    table_id,
                    table.indexes.len(),
                );
            
            }
        
        }
    
    }

}

pub fn index_value_tuple(index: &DatabaseIndex, row_map: &HashMap<String, Vec<u8>>) -> Vec<Vec<u8>> {

    let field_names = if index.field_names.is_empty() && !index.field_name.is_empty() {
        vec![index.field_name.clone()]
    } else {
        index.field_names.clone()
    };

    field_names
        .into_iter()
        .map(|field_name| row_map.get(&field_name).cloned().unwrap_or_default())
        .collect()

}


pub fn primary_key_index<'a>(table: &'a DatabaseTable) -> Option<&'a DatabaseIndex> {
    table.indexes.values().find(|index| index.is_primary_key())
}

pub fn derived_indexes_for_table(table: &DatabaseTable) -> impl Iterator<Item = &DatabaseIndex> + '_ {
    table.indexes.values().filter(|index| !matches!(index.origin, DatabaseIndexOrigin::Temporary))
}
