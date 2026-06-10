
use super::relationship::DatabaseRelationship;
use super::table::DatabaseTable;
use super::view::DatabaseView;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DatabaseEntity {
    Table(DatabaseTable),
    View(DatabaseView),
    Relationship(DatabaseRelationship),
}
