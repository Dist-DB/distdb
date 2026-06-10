
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityMetadata {
    pub created_by: Option<String>,
    pub created_at_epoch_ms: Option<u64>,
    pub updated_by: Option<String>,
    pub updated_at_epoch_ms: Option<u64>,
    pub tags: Vec<String>,
}

impl Default for EntityMetadata {
    fn default() -> Self {
        Self {
            created_by: None,
            created_at_epoch_ms: None,
            updated_by: None,
            updated_at_epoch_ms: None,
            tags: Vec::new(),
        }
    }
}

impl EntityMetadata {

    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.created_by = Some(creator.into());
        self
    }

    pub fn with_created_at(mut self, created_at_epoch_ms: u64) -> Self {
        self.created_at_epoch_ms = Some(created_at_epoch_ms);
        self
    }
    
}
