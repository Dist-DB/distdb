use super::runtime_index::{RuntimeIndexRangeBound, RuntimeIndexState};

/// Common storage contract for datatype-specific runtime indexors.
/// Implementations own normalization, key layout, paging, and posting storage.
pub trait RuntimeIndexStorage {

    fn contains(&self, key: &[Vec<u8>]) -> bool;
    fn insert(&mut self, key: Vec<Vec<u8>>, row_ref: Option<u64>);
    fn remove(&mut self, key: &[Vec<u8>], row_ref: Option<u64>);
    fn cardinality(&self) -> usize;
    fn row_refs_for_key(&self, key: &[Vec<u8>], limit: Option<usize>) -> Vec<u64>;
    
    fn row_refs_for_key_range(
        &self,
        lower: Option<&RuntimeIndexRangeBound>,
        upper: Option<&RuntimeIndexRangeBound>,
        limit: Option<usize>,
    ) -> Vec<u64>;

}

impl RuntimeIndexStorage for RuntimeIndexState {
    
    fn contains(&self, key: &[Vec<u8>]) -> bool {
        RuntimeIndexState::contains(self, key)
    }
    fn insert(&mut self, key: Vec<Vec<u8>>, row_ref: Option<u64>) {

        RuntimeIndexState::insert_with_row_ref(self, key, row_ref);
    }

    fn remove(&mut self, key: &[Vec<u8>], row_ref: Option<u64>) {
        RuntimeIndexState::remove_with_row_ref(self, key, row_ref);
    }

    fn cardinality(&self) -> usize {
        RuntimeIndexState::cardinality(self)
    }

    fn row_refs_for_key(&self, key: &[Vec<u8>], limit: Option<usize>) -> Vec<u64> {
        RuntimeIndexState::row_refs_for_key(self, key, limit)
    }

    fn row_refs_for_key_range(
        &self,
        lower: Option<&RuntimeIndexRangeBound>,
        upper: Option<&RuntimeIndexRangeBound>,
        limit: Option<usize>,
    ) -> Vec<u64> {
        RuntimeIndexState::row_refs_for_key_range(self, lower, upper, limit)
    }
    
}
