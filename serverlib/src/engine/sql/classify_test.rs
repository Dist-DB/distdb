use super::*;

#[test]
fn fallback_extracts_parameterized_procedure_name_for_create() {
    let classified = classify_text_fallback(
        "create procedure p_arg_route(p_mode uint64) begin if p_mode = 1 then select 1; end if; end;",
    )
    .expect("create procedure should classify");

    assert_eq!(classified.1, SqlOperation::CreateStoredProcedure);
    assert_eq!(classified.2.as_deref(), Some("p_arg_route"));
}

#[test]
fn fallback_extracts_parameterized_function_name_for_create() {
    let classified = classify_text_fallback(
        "create function f_arg_route(p_mode uint64) returns int return p_mode;",
    )
    .expect("create function should classify");

    assert_eq!(classified.1, SqlOperation::CreateStoredProcedure);
    assert_eq!(classified.2.as_deref(), Some("f_arg_route"));
}

#[test]
fn fallback_extracts_olap_view_name_for_create() {
    let classified = classify_text_fallback(
        "create olapview sales_by_region using region, product as select id, region, product from orders;",
    )
    .expect("create olapview should classify");

    assert_eq!(classified.1, SqlOperation::CreateOlapView);
    assert_eq!(classified.2.as_deref(), Some("sales_by_region"));
}

#[test]
fn fallback_extracts_parameterized_procedure_name_for_call() {
    let classified = classify_text_fallback("call p_arg_route(1);")
        .expect("call procedure should classify");

    assert_eq!(classified.1, SqlOperation::CallStoredProcedure);
    assert_eq!(classified.2.as_deref(), Some("p_arg_route"));
}

#[test]
fn fallback_extracts_function_name_for_drop() {
    let classified = classify_text_fallback("drop function if exists f_arg_route;")
        .expect("drop function should classify");

    assert_eq!(classified.1, SqlOperation::DropStoredProcedure);
    assert_eq!(classified.2.as_deref(), Some("f_arg_route"));
}

#[test]
fn fallback_extracts_entity_name_for_debug() {
    let classified = classify_text_fallback("debug procedure p_sync;")
        .expect("debug should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::Select);
    assert_eq!(classified.2.as_deref(), Some("p_sync"));
}

#[test]
fn fallback_extracts_table_name_for_update() {
    let classified = classify_text_fallback(
        "update users set active = true order by id desc limit 1",
    )
    .expect("update should classify");

    assert_eq!(classified.0, SqlDirective::Update);
    assert_eq!(classified.1, SqlOperation::Update);
    assert_eq!(classified.2.as_deref(), Some("users"));
}
