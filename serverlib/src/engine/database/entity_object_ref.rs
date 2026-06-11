
use super::entity_object_type::DatabaseObjectType;
use super::index::DatabaseIndex;
use super::relationship::DatabaseRelationship;
use super::stored_procedure::DatabaseStoredProcedure;
use super::table::DatabaseTable;
use super::trigger::DatabaseTrigger;
use super::view::DatabaseView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseObjectRef<'a> {
    Table(&'a DatabaseTable),
    View(&'a DatabaseView),
    Relationship(&'a DatabaseRelationship),
    Trigger(&'a DatabaseTrigger),
    StoredProcedure(&'a DatabaseStoredProcedure),
    Index(&'a DatabaseIndex),
}

impl<'a> DatabaseObjectRef<'a> {

    pub fn object_type(&self) -> DatabaseObjectType {
        match self {
            Self::Table(_)              => DatabaseObjectType::Table,
            Self::View(_)               => DatabaseObjectType::View,
            Self::Relationship(_)       => DatabaseObjectType::Relationship,
            Self::Trigger(_)            => DatabaseObjectType::Trigger,
            Self::StoredProcedure(_)    => DatabaseObjectType::StoredProcedure,
            Self::Index(_)              => DatabaseObjectType::Index,
        }
    }

}
