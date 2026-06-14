
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseObjectType {
    Table,
    View,
    Relationship,
    Trigger,
    StoredProcedure,
    Index,
}


#[cfg(test)]
#[path = "entity_object_type_test.rs"]
mod tests;
