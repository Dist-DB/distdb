
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum SystemFieldVisibility {
    #[default]
    Visible,
    Hidden,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldMetadata {
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub original_sql_type: Option<String>,
    #[serde(default)]
    pub character_set: Option<String>,
    #[serde(default)]
    pub collation: Option<String>,
    #[serde(default)]
    pub system_visibility: SystemFieldVisibility,
}

impl FieldMetadata {
    pub fn is_hidden(&self) -> bool {
        matches!(self.system_visibility, SystemFieldVisibility::Hidden)
    }
}
