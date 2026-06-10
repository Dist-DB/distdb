
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlObjectKind {
    View,
    Trigger,
    StoredProcedure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlDefinitionAction {
    Upsert,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SqlDefinitionPayload {
    pub object_id: String,
    pub object_kind: SqlObjectKind,
    pub action: SqlDefinitionAction,
    pub sql: String,
    pub dependencies: Vec<String>,
}

impl SqlDefinitionPayload {
    
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        bincode::serialize(self).map_err(|_| "failed to serialize sql definition payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        bincode::deserialize(payload).map_err(|_| "failed to deserialize sql definition payload")
    }

}
