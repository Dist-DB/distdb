use super::*;

#[test]
fn database_id_is_obscured_from_normalized_name() {
    let id_a = DatabaseId::from_database_name("Sales").expect("valid database name");
    let id_b = DatabaseId::from_database_name("sales").expect("valid database name");

    assert_eq!(id_a, id_b);
    assert_ne!(id_a.0, "sales");
}
