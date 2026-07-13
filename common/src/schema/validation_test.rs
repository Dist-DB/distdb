    use super::*;

    #[test]
    fn validate_field_kind_rejects_invalid_bit_width() {
        let err = validate_field_kind(&FieldKind::Int(7)).unwrap_err();
        assert_eq!(err, SchemaValidationError::InvalidBitWidth);
    }

    #[test]
    fn validate_field_kind_rejects_empty_enum() {
        let err = validate_field_kind(&FieldKind::Enum(Vec::new())).unwrap_err();
        assert_eq!(err, SchemaValidationError::EmptyEnumVariants);
    }

    #[test]
    fn normalize_field_name_rejects_empty() {
        let err = normalize_field_name("   ").unwrap_err();
        assert_eq!(err, SchemaValidationError::EmptyFieldName);
    }