use crate::engine::database::inbuilt::is_inbuilt_function;

#[test]
fn geo_function_registry_exposes_expected_functions() {
    assert!(is_inbuilt_function("distance"));
}
