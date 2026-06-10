
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionKind {
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
