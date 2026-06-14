
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEntityKind {
    Table,
    View,
    Relationship,
    Trigger,
    StoredProcedure,
}


#[cfg(test)]
#[path = "entity_kind_test.rs"]
mod tests;
