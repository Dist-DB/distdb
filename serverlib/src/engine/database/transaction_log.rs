use super::transaction_id::TransactionId;
use super::transaction_record::TransactionRecord;

pub trait TransactionLog {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str>;
    // When from is provided, return records after that transaction id (exclusive).
    // When from is None, return all records for the WAL stream.
    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord>;

}
