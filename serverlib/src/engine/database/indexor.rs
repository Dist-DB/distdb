use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;

use super::databaseindex::{DatabaseIndex, DatabaseIndexKind};

pub trait IndexorIndexSpec {
    fn index_id(&self) -> &str;
    fn index_kind(&self) -> DatabaseIndexKind;
    fn encode_index_key(&self, row_map: &HashMap<String, Vec<u8>>) -> Option<Vec<u8>>;
}

impl IndexorIndexSpec for DatabaseIndex {

    fn index_id(&self) -> &str {
        self.index_id.0.as_str()
    }

    fn index_kind(&self) -> DatabaseIndexKind {
        self.kind
    }

    fn encode_index_key(&self, row_map: &HashMap<String, Vec<u8>>) -> Option<Vec<u8>> {

        let tuple = if self.field_names.is_empty() {
            vec![
                row_map
                    .get(&self.field_name)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            ]
        } else {
            self
                .field_names
                .iter()
                .map(|field_name| row_map.get(field_name).map(Vec::as_slice).unwrap_or(&[]))
                .collect::<Vec<_>>()
        };

        common::helpers::bincode_compat::serialize(&tuple).ok()
        
    }

}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexorEntrySnapshot {
    pub key: Vec<u8>,
    pub row_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexorStorageSnapshot {
    pub index_name: String,
    pub kind: DatabaseIndexKind,
    pub entries: Vec<IndexorEntrySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexorSnapshot {
    pub format_version: u16,
    pub storages: Vec<IndexorStorageSnapshot>,
}

impl Default for IndexorSnapshot {
    fn default() -> Self {
        Self {
            format_version: 1,
            storages: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexorSnapshotError {
    SerializeFailed,
    DeserializeFailed,
    UnsupportedFormatVersion {
        found: u16,
    },
    DuplicateIndexName {
        index_name: String,
    },
    DuplicateKey {
        index_name: String,
        key: Vec<u8>,
    },
    UniqueIndexHasMultipleRows {
        index_name: String,
        key: Vec<u8>,
        row_count: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexorDefinitionError {
    ConflictingDesignation {
        index_name: String,
        current: DatabaseIndexKind,
        requested: DatabaseIndexKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexorInsertError {
    UniqueViolation {
        index_name: String,
        key: Vec<u8>,
        existing_row_id: u64,
        attempted_row_id: u64,
    },
    KeyEncodeFailed {
        index_name: String,
    },
    IndexDefinitionFailed {
        index_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexorRangeBound {
    pub key: Vec<u8>,
    pub inclusive: bool,
}

#[derive(Debug, Default)]
struct IndexorStorage {
    kind: DatabaseIndexKind,
    key_directory: BTreeMap<Vec<u8>, u32>,
    equality_lookup: HashMap<Vec<u8>, u32>,
    buckets: HashMap<u32, HashSet<u64>>,
    next_bucket_id: u32,
}

impl IndexorStorage {

    fn bucket_for_key_mut(&mut self, key: &[u8]) -> u32 {

        if let Some(bucket_id) = self.equality_lookup.get(key) {
            return *bucket_id;
        }

        let bucket_id = self.next_bucket_id;
        self.next_bucket_id = self.next_bucket_id.saturating_add(1);

        let owned_key = key.to_vec();
        
        self.equality_lookup.insert(owned_key.clone(), bucket_id);
        self.key_directory.insert(owned_key, bucket_id);
        self.buckets.entry(bucket_id).or_default();

        bucket_id
        
    }

    fn bucket_for_key(&self, key: &[u8]) -> Option<u32> {
        self.equality_lookup.get(key).copied()
    }

}

#[derive(Debug, Default)]
pub struct DatabaseIndexor {
    storages: HashMap<String, IndexorStorage>,
}

impl DatabaseIndexor {

    const SNAPSHOT_FORMAT_VERSION: u16 = 1;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_index(&mut self, index_name: &str) {
        let _ = self.ensure_index_with_kind(index_name, DatabaseIndexKind::Indexed);
    }

    pub fn ensure_index_with_kind(
        &mut self,
        index_name: &str,
        kind: DatabaseIndexKind,
    ) -> Result<(), IndexorDefinitionError> {

        let name = common::normalize_identifier!(index_name);

        match self.storages.get(&name) {

            Some(storage) if storage.kind != kind => {
                Err(IndexorDefinitionError::ConflictingDesignation {
                    index_name: name,
                    current: storage.kind,
                    requested: kind,
                })
            },

            Some(_) => Ok(()),

            None => {
                self.storages.insert(
                    name,
                    IndexorStorage {
                        kind,
                        ..IndexorStorage::default()
                    },
                );
                Ok(())
            }

        }
        
    }

    pub fn ensure_index_for(
        &mut self,
        index_spec: &impl IndexorIndexSpec,
    ) -> Result<(), IndexorDefinitionError> {
        self.ensure_index_with_kind(index_spec.index_id(), index_spec.index_kind())
    }

    pub fn index_kind(&self, index_name: &str) -> Option<DatabaseIndexKind> {
        let name = common::normalize_identifier!(index_name);
        self.storages.get(&name).map(|storage| storage.kind)
    }

    pub fn insert(&mut self, index_name: &str, key: &[u8], row_id: u64) -> Result<(), IndexorInsertError> {

        let name = common::normalize_identifier!(index_name);
        let storage = self.storages.entry(name.clone()).or_insert_with(|| IndexorStorage {
            kind: DatabaseIndexKind::Indexed,
            ..IndexorStorage::default()
        });

        let bucket_id = storage.bucket_for_key_mut(key);
        let bucket = storage.buckets.entry(bucket_id).or_default();

        if index_kind_is_unique(storage.kind)
            && let Some(existing_row_id) = bucket.iter().copied().next() {

            if existing_row_id != row_id {
                return Err(IndexorInsertError::UniqueViolation {
                    index_name: name,
                    key: key.to_vec(),
                    existing_row_id,
                    attempted_row_id: row_id,
                });
            }

            return Ok(());
        }

        bucket.insert(row_id);
        Ok(())

    }

    pub fn insert_indexed_row(
        &mut self,
        index_spec: &impl IndexorIndexSpec,
        row_map: &HashMap<String, Vec<u8>>,
        row_id: u64,
    ) -> Result<(), IndexorInsertError> {

        self.ensure_index_for(index_spec)
            .map_err(|_| IndexorInsertError::IndexDefinitionFailed {
                index_name: index_spec.index_id().to_string(),
            })?;

        let key = index_spec.encode_index_key(row_map).ok_or_else(|| {
            IndexorInsertError::KeyEncodeFailed {
                index_name: index_spec.index_id().to_string(),
            }
        })?;

        self.insert(index_spec.index_id(), &key, row_id)

    }

    pub fn has_hit(&self, index_name: &str, key: &[u8]) -> bool {

        let name = common::normalize_identifier!(index_name);

        self.storages
            .get(&name)
            .and_then(|storage| storage.bucket_for_key(key))
            .is_some()

    }

    pub fn query_eq(&self, index_name: &str, key: &[u8]) -> Vec<u64> {

        let name = common::normalize_identifier!(index_name);

        let Some(storage) = self.storages.get(&name) else {
            return Vec::new();
        };

        let Some(bucket_id) = storage.bucket_for_key(key) else {
            return Vec::new();
        };

        let mut row_ids = storage
            .buckets
            .get(&bucket_id)
            .map(|ids| ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        row_ids.sort_unstable();
        row_ids
        
    }

    pub fn query_range(
        &self,
        index_name: &str,
        lower: Option<&IndexorRangeBound>,
        upper: Option<&IndexorRangeBound>,
    ) -> Vec<u64> {

        let name = common::normalize_identifier!(index_name);

        let Some(storage) = self.storages.get(&name) else {
            return Vec::new();
        };

        let lower_bound = match lower {
            Some(bound) if bound.inclusive => Bound::Included(bound.key.as_slice()),
            Some(bound) => Bound::Excluded(bound.key.as_slice()),
            None => Bound::Unbounded,
        };

        let upper_bound = match upper {
            Some(bound) if bound.inclusive => Bound::Included(bound.key.as_slice()),
            Some(bound) => Bound::Excluded(bound.key.as_slice()),
            None => Bound::Unbounded,
        };

        let mut row_id_set = HashSet::new();

        for (_, bucket_id) in storage
            .key_directory
            .range::<[u8], _>((lower_bound, upper_bound))
        {
            if let Some(ids) = storage.buckets.get(bucket_id) {
                row_id_set.extend(ids.iter().copied());
            }
        }

        let mut row_ids = row_id_set.into_iter().collect::<Vec<_>>();
        row_ids.sort_unstable();
        row_ids

    }

    pub fn index_count(&self) -> usize {
        self.storages.len()
    }

    pub fn snapshot(&self) -> IndexorSnapshot {

        let mut index_names = self.storages.keys().cloned().collect::<Vec<_>>();
        index_names.sort_unstable();

        let mut storages = Vec::with_capacity(index_names.len());

        for index_name in index_names {

            let Some(storage) = self.storages.get(&index_name) else {
                continue;
            };

            let mut entries = Vec::with_capacity(storage.key_directory.len());

            for (key, bucket_id) in &storage.key_directory {
                let mut row_ids = storage
                    .buckets
                    .get(bucket_id)
                    .map(|rows| rows.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default();

                row_ids.sort_unstable();

                entries.push(IndexorEntrySnapshot {
                    key: key.clone(),
                    row_ids,
                });
            }

            storages.push(IndexorStorageSnapshot {
                index_name,
                kind: storage.kind,
                entries,
            });

        }

        IndexorSnapshot {
            format_version: Self::SNAPSHOT_FORMAT_VERSION,
            storages,
        }

    }

    pub fn from_snapshot(snapshot: &IndexorSnapshot) -> Result<Self, IndexorSnapshotError> {

        if snapshot.format_version != Self::SNAPSHOT_FORMAT_VERSION {
            return Err(IndexorSnapshotError::UnsupportedFormatVersion {
                found: snapshot.format_version,
            });
        }

        let mut storages = HashMap::new();

        for storage_snapshot in &snapshot.storages {

            let index_name = common::normalize_identifier!(&storage_snapshot.index_name);

            if storages.contains_key(&index_name) {
                return Err(IndexorSnapshotError::DuplicateIndexName { index_name });
            }

            let mut storage = IndexorStorage {
                kind: storage_snapshot.kind,
                ..IndexorStorage::default()
            };

            for entry in &storage_snapshot.entries {

                if storage.equality_lookup.contains_key(entry.key.as_slice()) {
                    return Err(IndexorSnapshotError::DuplicateKey {
                        index_name,
                        key: entry.key.clone(),
                    });
                }

                if index_kind_is_unique(storage.kind) && entry.row_ids.len() > 1 {
                    return Err(IndexorSnapshotError::UniqueIndexHasMultipleRows {
                        index_name,
                        key: entry.key.clone(),
                        row_count: entry.row_ids.len(),
                    });
                }

                let bucket_id = storage.next_bucket_id;
                
                storage.next_bucket_id = storage.next_bucket_id.saturating_add(1);

                storage
                    .equality_lookup
                    .insert(entry.key.clone(), bucket_id);
                
                storage.key_directory.insert(entry.key.clone(), bucket_id);

                storage
                    .buckets
                    .insert(bucket_id, entry.row_ids.iter().copied().collect());

            }

            storages.insert(index_name, storage);

        }

        Ok(Self { storages })

    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, IndexorSnapshotError> {
        common::helpers::bincode_compat::serialize(&self.snapshot()).map_err(|_| IndexorSnapshotError::SerializeFailed)
    }

    pub fn from_snapshot_bytes(payload: &[u8]) -> Result<Self, IndexorSnapshotError> {
        let snapshot = common::helpers::bincode_compat::deserialize::<IndexorSnapshot>(payload)
            .map_err(|_| IndexorSnapshotError::DeserializeFailed)?;
        Self::from_snapshot(&snapshot)
    }
    
}

fn index_kind_is_unique(kind: DatabaseIndexKind) -> bool {
    matches!(kind, DatabaseIndexKind::PrimaryKey | DatabaseIndexKind::Unique)
}

#[cfg(test)]
#[path = "indexor_test.rs"]
mod tests;
