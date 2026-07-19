
use super::id::TransactionId;
use super::kind::TransactionKind;
use super::record::TransactionRecord;

pub trait TransactionLog {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str>;
    // When from is provided, return records after that transaction id (exclusive).
    // When from is None, return all records for the WAL stream.
    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord>;

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

        self.since(wal_id, from)
            .into_iter()
            .filter(|record| kinds.contains(&record.kind))
            .collect()
            
    }

}
