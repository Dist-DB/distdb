
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionKind {
    Ignore,
    WriteBegin,
    WriteCommit,
    WriteAbort,
    Insert,
    Update,
    Delete,
    Truncate,
    TableLifecycle,
    IndexLifecycle,
    SchemaChange,
    MetadataChange,
    SqlDefinitionChange,
    SecurityChange,
}
