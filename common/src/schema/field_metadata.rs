
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldMetadata {
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub character_set: Option<String>,
    #[serde(default)]
    pub collation: Option<String>,
}
