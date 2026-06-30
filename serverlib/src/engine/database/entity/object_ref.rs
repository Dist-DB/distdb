
use crate::engine::database::entity::object_type::DatabaseObjectType;
use crate::engine::database::index::DatabaseIndex;
use crate::engine::database::relationship::DatabaseRelationship;
use crate::engine::database::stored_procedure::DatabaseStoredProcedure;
use crate::engine::database::table::DatabaseTable;
use crate::engine::database::trigger::DatabaseTrigger;
use crate::engine::database::view::DatabaseView;

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
            Self::Table(_) => DatabaseObjectType::Table,
            Self::View(_) => DatabaseObjectType::View,
            Self::Relationship(_) => DatabaseObjectType::Relationship,
            Self::Trigger(_) => DatabaseObjectType::Trigger,
            Self::StoredProcedure(_) => DatabaseObjectType::StoredProcedure,
            Self::Index(_) => DatabaseObjectType::Index,
        }
    }

}
