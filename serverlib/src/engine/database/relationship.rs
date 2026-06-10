
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseRelationship {
    pub left_table_id: String,
    pub right_table_id: String,
    pub relation_name: String,
}
