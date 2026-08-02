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

#[test]
fn fallback_classifies_export_database() {
    let classified = classify_text_fallback("export database to '/tmp/distdb-export.sql';")
        .expect("export database should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("database"));
    assert_eq!(classified.3, Some(AccountPrivilege::BackupAdmin));
}

#[test]
fn fallback_classifies_export_view() {
    let classified = classify_text_fallback("export view sales_view to '/tmp/view.sql';")
        .expect("export view should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("sales_view"));
    assert_eq!(classified.3, Some(AccountPrivilege::BackupAdmin));
}

#[test]
fn fallback_classifies_export_olapview_alias() {
    let classified = classify_text_fallback("export olap_view cube_sales to '/tmp/olap.sql';")
        .expect("export olap_view should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("cube_sales"));
    assert_eq!(classified.3, Some(AccountPrivilege::BackupAdmin));
}

#[test]
fn fallback_classifies_export_procedure() {
    let classified = classify_text_fallback("export procedure p_sync to '/tmp/p_sync.sql';")
        .expect("export procedure should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("p_sync"));
    assert_eq!(classified.3, Some(AccountPrivilege::BackupAdmin));
}

#[test]
fn fallback_rejects_malformed_export_target() {
    let classified = classify_text_fallback("export table users '/tmp/users.sql';");
    assert!(classified.is_none());
}

#[test]
fn fallback_classifies_export_with_quoted_identifier_and_spaced_path() {
    let classified = classify_text_fallback("export table `Order` to '/tmp/folder/export file.sql';")
        .expect("export table with quoted name and spaced path should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("Order"));
    assert_eq!(classified.3, Some(AccountPrivilege::BackupAdmin));
}

#[test]
fn fallback_classifies_export_with_path_containing_to_substring() {
    let classified = classify_text_fallback("export view sales_view to '/tmp/to folder/export-to-file.sql';")
        .expect("export with path containing to should classify");

    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("sales_view"));
}

#[test]
fn fallback_classifies_export_stored_procedure_with_quoted_identifier() {
    let classified = classify_text_fallback("export stored procedure `p_sync` to '/tmp/p sync.sql';")
        .expect("export stored procedure with quoted identifier should classify");

    assert_eq!(classified.0, SqlDirective::Retrieve);
    assert_eq!(classified.1, SqlOperation::ExportDatabase);
    assert_eq!(classified.2.as_deref(), Some("p_sync"));
}

#[test]
fn fallback_rejects_export_without_destination_path() {
    let classified = classify_text_fallback("export database to");
    assert!(classified.is_none());
}
