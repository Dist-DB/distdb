use super::*;

#[test]
fn stream_key_for_table_normalizes_identifier() {
    let key = stream_key_for_table("USERS").expect("should get key");
    let key2 = stream_key_for_table("users").expect("should get key");
    assert_eq!(key, key2);
}

#[test]
fn stream_key_for_empty_table_fails() {
    let result = stream_key_for_table("");
    assert!(result.is_err());
}

#[test]
fn load_records_from_nonexistent_path_returns_empty() {
    let result = load_records_from_path(Path::new("/nonexistent/path")).expect("should load empty");
    assert_eq!(result.len(), 0);
}
