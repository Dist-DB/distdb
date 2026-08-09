
use super::id::TransactionId;
use super::kind::TransactionKind;
use super::record::TransactionRecord;

pub trait TransactionLog {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str>;
    // When from is provided, return records after that transaction id (exclusive).
    // When from is None, return all records for the WAL stream.
    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord>;

    // Stream records after `from` (exclusive) without requiring callers to
    // materialize a returned Vec unless they actually need ownership.
    fn for_each_since<F>(&self, wal_id: &str, from: Option<TransactionId>, mut func: F)
    where
        F: FnMut(&TransactionRecord),
    {
        let records = self.since(wal_id, from);

        for record in &records {
            func(record);
        }
    }

    fn with_all_records<T, F>(&self, wal_id: &str, func: F) -> T
    where
        F: FnOnce(&[TransactionRecord]) -> T,
    {
        let records = self.since(wal_id, None);
        func(&records)
    }

    // Returns records filtered by transaction kind. Default implementation uses
    // `since` and filters in-memory; implementations may override for efficiency.
    fn since_kinds(
        &self,
        wal_id: &str,
        from: Option<TransactionId>,
        kinds: &[TransactionKind],
    ) -> Vec<TransactionRecord> {

        if kinds.is_empty() {
            return Vec::new();
        }

        let mut records = Vec::new();

        self.for_each_since(wal_id, from, |record| {
            if kinds.contains(&record.kind) {
                records.push(record.clone());
            }
        });

        records
            
    }

}
