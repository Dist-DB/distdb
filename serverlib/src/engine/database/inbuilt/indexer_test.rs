use super::{is_inbuilt_function, registered_inbuilt_function_names};

#[test]
fn exposed_inbuilt_registry_contains_expected_entries() {

    let names = registered_inbuilt_function_names();

    for function_name in ["abs", "unix_timestamp", "concat_ws", "lookup", "newuuid"] {
        assert!(names.contains(&function_name));
        assert!(is_inbuilt_function(function_name));
    }
    
}
