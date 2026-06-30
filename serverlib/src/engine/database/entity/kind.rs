
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEntityKind {
    Table,
    View,
    Relationship,
    Trigger,
    StoredProcedure,
}


#[cfg(test)]
#[path = "kind_test.rs"]
mod kind_test;
