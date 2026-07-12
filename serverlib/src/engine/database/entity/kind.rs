
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEntityKind {
    Table,
    View,
    OlapView,
    Relationship,
    Trigger,
    StoredProcedure,
}

impl std::fmt::Display for DatabaseEntityKind {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self {
            
            DatabaseEntityKind::Table => write!(f, "table"),
            
            DatabaseEntityKind::View => write!(f, "view"),
            
            DatabaseEntityKind::OlapView => write!(f, "olap_view"),
            
            DatabaseEntityKind::Relationship => write!(f, "relationship"),
            
            DatabaseEntityKind::Trigger => write!(f, "trigger"),
            
            DatabaseEntityKind::StoredProcedure => write!(f, "stored_procedure"),

        }
        
    }

}


#[cfg(test)]
#[path = "kind_test.rs"]
mod kind_test;
