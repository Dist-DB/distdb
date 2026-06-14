
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    SchemaChange,
    MetadataChange,
    SqlDefinitionChange,
    SecurityChange,
}
