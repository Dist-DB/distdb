use super::ProcedureLocalEntityScope;

#[test]
fn variable_storage_supports_normalized_lookup_and_remove() {

    let mut scope = ProcedureLocalEntityScope::new("proc_test");

    scope.set_variable("Counter", vec![1]);

    assert_eq!(scope.variable_value("counter"), Some(&vec![1]));
    assert_eq!(scope.resolve_value("COUNTER"), Some(&vec![1]));

    assert!(scope.remove_variable("counter"));
    assert_eq!(scope.variable_value("counter"), None);
    assert_eq!(scope.resolve_value("counter"), None);

}

#[test]
fn arguments_are_resolved_as_values_when_present() {

    let mut scope = ProcedureLocalEntityScope::new("proc_test");

    scope.set_argument("arg_input", vec![7, 8]);

    assert_eq!(scope.argument_value("ARG_INPUT"), Some(&vec![7, 8]));
    assert_eq!(scope.resolve_value("arg_input"), Some(&vec![7, 8]));

}
