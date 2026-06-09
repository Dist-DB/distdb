#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicKind {
    Database,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub kind: TopicKind,
    pub database_id: String,
    pub table_id: Option<String>,
}

impl Topic {
    pub fn as_key(&self) -> String {
        match (&self.kind, &self.table_id) {
            (TopicKind::Database, _) => self.database_id.clone(),
            (TopicKind::Table, Some(table)) => format!("{}:{}", self.database_id, table),
            (TopicKind::Table, None) => format!("{}:*", self.database_id),
        }
    }
}