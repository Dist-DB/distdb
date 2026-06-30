
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
#[path = "object_type_test.rs"]
mod object_type_test;
