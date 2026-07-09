
use super::*;

#[test]
fn parser_returns_directives_for_multiple_statements() {
    let requests = parse_mysql8_sql_requests(
        "select * from users; update users set active=1 where id=1",
        "main",
    )
    .expect("requests should parse");

    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    assert_eq!(requests[1].directive, SqlDirective::Update);
    assert_eq!(requests[1].operation, SqlOperation::Update);
    assert_eq!(requests[1].object_name.as_deref(), Some("users"));
    assert_eq!(
        requests[0].compatibility_target,
        SqlCompatibilityTarget::Mysql80
    );
}

#[test]
fn explain_insert_maps_to_insert_operation() {
    let requests = parse_mysql8_sql_requests("explain insert into users (id) values (1)", "main")
        .expect("explain insert should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Insert);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn explain_create_table_is_rejected() {
    let err = parse_mysql8_sql_requests("explain create table users (id bigint)", "main")
        .expect_err("explain create table should be rejected");

    assert!(matches!(err, SqlParseError::UnsupportedStatement(_)));
}

#[test]
fn explain_select_maps_to_retrieve_operation() {
    let requests = parse_mysql8_sql_requests("explain select * from users", "main")
        .expect("explain select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn explain_update_maps_to_update_operation() {
    let requests =
        parse_mysql8_sql_requests("explain update users set active = 1 where id = 1", "main")
            .expect("explain update should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Update);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn explain_delete_maps_to_delete_operation() {
    let requests = parse_mysql8_sql_requests("explain delete from users where id = 1", "main")
        .expect("explain delete should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Delete);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn drop_statement_maps_to_alter_schema_directive() {
    let requests =
        parse_mysql8_sql_requests("drop table users", "main").expect("drop statement should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropTable);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn create_database_operation_parses_object_name() {
    let requests = parse_mysql8_sql_requests("create database analytics", "main")
        .expect("create database should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].operation, SqlOperation::CreateDatabase);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn create_database_with_aes_suffix_parses_object_name() {
    let requests = parse_mysql8_sql_requests("create database analytics --aes", "main")
        .expect("create database with aes suffix should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateDatabase);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn create_schema_operation_maps_to_create_database() {
    let requests = parse_mysql8_sql_requests("create schema analytics", "main")
        .expect("create schema should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateDatabase);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn select_from_qualified_table_keeps_database_and_table_name() {
    let requests = parse_mysql8_sql_requests("select * from main.users", "main")
        .expect("qualified select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("main.users"));
}

#[test]
fn union_select_maps_to_union_directive() {
    let requests = parse_mysql8_sql_requests(
        "select id from users union select id from archived_users",
        "main",
    )
    .expect("union select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Union);
    assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn parenthesized_union_select_maps_to_union_directive() {
    let requests = parse_mysql8_sql_requests(
        "(select id as a from users) union (select id as b from archived_users)",
        "main",
    )
    .expect("parenthesized union select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Union);
    assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn except_select_maps_to_union_directive() {
    let requests = parse_mysql8_sql_requests(
        "select id from users except select id from archived_users",
        "main",
    )
    .expect("except select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Union);
    assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn intersect_select_maps_to_union_directive() {
    let requests = parse_mysql8_sql_requests(
        "select id from users intersect select id from archived_users",
        "main",
    )
    .expect("intersect select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Union);
    assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn create_view_operation_parses_object_name() {
    let requests =
        parse_mysql8_sql_requests("create view active_users as select * from users", "main")
            .expect("create view should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateView);
    assert_eq!(requests[0].object_name.as_deref(), Some("active_users"));
}

#[test]
fn drop_view_operation_maps_to_alter_schema() {
    let requests = parse_mysql8_sql_requests("drop view archived_users", "main")
        .expect("drop view should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropView);
    assert_eq!(requests[0].object_name.as_deref(), Some("archived_users"));
}

#[test]
fn drop_schema_operation_maps_to_drop_database() {
    let requests = parse_mysql8_sql_requests("drop schema analytics", "main")
        .expect("drop schema should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropDatabase);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn drop_database_operation_maps_to_drop_database() {
    let requests = parse_mysql8_sql_requests("drop database analytics", "main")
        .expect("drop database should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropDatabase);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn alter_view_operation_maps_to_alter_schema() {
    let requests =
        parse_mysql8_sql_requests("alter view active_users as select id from users", "main")
            .expect("alter view should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterView);
    assert_eq!(requests[0].object_name.as_deref(), Some("active_users"));
}

#[test]
fn insert_select_union_maps_to_insert_operation() {
    let requests = parse_mysql8_sql_requests(
        "insert into users (id) (select id from staged_users) union (select id from backup_users)",
        "main",
    )
    .expect("insert-select union should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::Insert);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn cte_union_select_maps_to_union_directive() {
    let requests = parse_mysql8_sql_requests(
            "with combined as (select id from users union select id from archived_users) select * from combined",
            "main",
        )
        .expect("cte union select should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Union);
    assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
    assert_eq!(requests[0].object_name.as_deref(), Some("combined"));
}

#[test]
fn truncate_table_maps_to_truncate_operation() {
    let requests = parse_mysql8_sql_requests("truncate table users", "main")
        .expect("truncate table should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Delete);
    assert_eq!(requests[0].operation, SqlOperation::TruncateTable);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn show_columns_maps_to_retrieve_operation() {
    let requests = parse_mysql8_sql_requests("show columns from users", "main")
        .expect("show columns should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn describe_table_maps_to_retrieve_operation() {
    let requests =
        parse_mysql8_sql_requests("describe users", "main").expect("describe should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn debug_entity_maps_to_retrieve_operation() {
    let requests =
        parse_mysql8_sql_requests("debug procedure p_sync", "main").expect("debug should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
}

#[test]
fn use_database_maps_to_alter_schema_other_operation() {
    let requests =
        parse_mysql8_sql_requests("use analytics", "main").expect("use database should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn create_index_maps_to_create_other_operation() {
    let requests = parse_mysql8_sql_requests("create index idx_users_id on users(id)", "main")
        .expect("create index should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("idx_users_id"));
}

#[test]
fn set_names_maps_to_alter_schema_other_operation() {
    let requests =
        parse_mysql8_sql_requests("set names utf8mb4", "main").expect("set names should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("utf8mb4"));
}

#[test]
fn set_variable_maps_to_alter_schema_other_operation() {
    let requests =
        parse_mysql8_sql_requests("set autocommit = 0", "main").expect("set variable should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("autocommit"));
}

#[test]
fn show_create_table_maps_to_retrieve_operation() {
    let requests = parse_mysql8_sql_requests("show create table users", "main")
        .expect("show create table should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn show_tables_from_database_maps_to_retrieve_operation() {
    let requests = parse_mysql8_sql_requests("show tables from analytics", "main")
        .expect("show tables from db should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::Select);
    assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
}

#[test]
fn start_transaction_maps_to_alter_schema_other_operation() {
    let requests = parse_mysql8_sql_requests("start transaction", "main")
        .expect("start transaction should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert!(requests[0].object_name.is_none());
}

#[test]
fn rollback_to_savepoint_maps_savepoint_name() {
    let requests = parse_mysql8_sql_requests("rollback to savepoint sp1", "main")
        .expect("rollback to savepoint should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("sp1"));
}

#[test]
fn grant_statement_maps_to_alter_schema_other_operation() {
    let requests = parse_mysql8_sql_requests("grant select on users to app_user", "main")
        .expect("grant should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn revoke_statement_maps_to_alter_schema_other_operation() {
    let requests = parse_mysql8_sql_requests("revoke select on users from app_user", "main")
        .expect("revoke should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::AlterOther);
    assert_eq!(requests[0].object_name.as_deref(), Some("users"));
}

#[test]
fn create_function_maps_to_create_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("create function f_add() returns int return 1", "main")
        .expect("create function should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("f_add"));
}

#[test]
fn drop_function_maps_to_drop_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("drop function f_add", "main")
        .expect("drop function should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("f_add"));
}

#[test]
fn create_procedure_maps_to_create_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("create procedure p_sync() as begin end;", "main")
        .expect("create procedure should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
}

#[test]
fn drop_procedure_maps_to_drop_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("drop procedure p_sync", "main")
        .expect("drop procedure should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
}

#[test]
fn drop_procedure_if_exists_maps_to_drop_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("drop procedure if exists p_sync", "main")
        .expect("drop procedure if exists should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
}

#[test]
fn call_procedure_maps_to_call_stored_procedure_operation() {
    let requests = parse_mysql8_sql_requests("call p_sync()", "main")
        .expect("call procedure should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Retrieve);
    assert_eq!(requests[0].operation, SqlOperation::CallStoredProcedure);
    assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
}

#[test]
fn create_trigger_maps_to_create_trigger_operation() {
    let requests = parse_mysql8_sql_requests(
            "create trigger trg_users_bi before insert on users for each row execute function audit_users()",
            "main",
        )
        .expect("create trigger should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateTrigger);
    assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
}

#[test]
fn drop_trigger_maps_to_drop_trigger_operation() {
    let requests = parse_mysql8_sql_requests("drop trigger trg_users_bi on users", "main")
        .expect("drop trigger should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropTrigger);
    assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
}

#[test]
fn create_or_replace_trigger_maps_to_create_trigger_operation() {
    let requests = parse_mysql8_sql_requests(
        "create or replace trigger trg_users_bi before insert on users for each row set @x = 1",
        "main",
    )
    .expect("create or replace trigger should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::Create);
    assert_eq!(requests[0].operation, SqlOperation::CreateTrigger);
    assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
}

#[test]
fn drop_trigger_if_exists_maps_to_drop_trigger_operation() {
    let requests = parse_mysql8_sql_requests("drop trigger if exists trg_users_bi", "main")
        .expect("drop trigger if exists should parse");

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
    assert_eq!(requests[0].operation, SqlOperation::DropTrigger);
    assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
}
