    use super::normalize_identifier;

    #[test]
    fn normalize_identifier_strips_backtick_quotes() {
        assert_eq!(normalize_identifier("`__Account`"), "__account");
    }

    #[test]
    fn normalize_identifier_strips_double_quotes() {
        assert_eq!(normalize_identifier("\"Users\""), "users");
    }

    #[test]
    fn normalize_identifier_strips_single_quotes() {
        assert_eq!(normalize_identifier("'Orders'"), "orders");
    }