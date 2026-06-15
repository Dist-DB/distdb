#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AffinitySyncPhase {
    ControlPlane,
    SchemaCatalog,
    DataSnapshot,
    WalCatchup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinitySyncStep {
    pub phase: AffinitySyncPhase,
    pub database_id: Option<String>,
    pub schema_identifier: Option<u64>,
}