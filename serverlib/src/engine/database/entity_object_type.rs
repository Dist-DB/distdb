
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
mod tests {

    use super::*;

    #[test]
    fn object_type_equality() {
        assert_eq!(DatabaseObjectType::Table, DatabaseObjectType::Table);
        assert_ne!(DatabaseObjectType::Table, DatabaseObjectType::Index);
    }

    #[test]
    fn object_type_is_copy() {
        let t = DatabaseObjectType::Index;
        let copy = t;
        assert_eq!(t, copy);
    }

}
