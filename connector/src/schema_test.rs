    use super::*;

    #[test]
    fn field_spec_builder_defaults_to_non_nullable_non_indexed() {
        let spec = FieldSpec::new("email", FieldKind::Text);
        assert!(!spec.nullable);
        assert_eq!(spec.indexed, FieldIndex::None);
        assert_eq!(spec.name, "email");
    }

    #[test]
    fn field_spec_builder_sets_flags() {
        let spec = FieldSpec::new("email", FieldKind::Text).nullable().indexed();
        assert!(spec.nullable);
        assert_eq!(spec.indexed, FieldIndex::Indexed);
    }

    #[test]
    fn field_spec_builder_sets_primary_key() {
        let spec = FieldSpec::new("id", FieldKind::UInt(64)).primary_key();
        assert_eq!(spec.indexed, FieldIndex::PrimaryKey);
    }

    #[test]
    fn schema_change_request_builder_collects_operations() {
        let req = SchemaChangeRequest::new("users")
            .add_field(FieldSpec::new("email", FieldKind::Text).indexed())
            .remove_field("legacy_col")
            .update_field(FieldSpec::new("name", FieldKind::Text).nullable());

        assert_eq!(req.table_id, "users");
        assert_eq!(req.add.len(), 1);
        assert_eq!(req.remove.len(), 1);
        assert_eq!(req.update.len(), 1);
    }