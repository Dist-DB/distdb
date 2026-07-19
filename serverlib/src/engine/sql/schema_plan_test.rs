
use super::*;
use crate::{FieldIndex, FieldType};

#[test]
fn create_table_schema_helper_maps_fields() {
    let (table_id, schema) = create_table_schema_from_statement(
        "create table users (id bigint not null primary key, email varchar(255) not null, age int)",
    )
    .expect("create table schema should parse");

    assert_eq!(table_id, "users");
    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].field_name, "id");
    assert_eq!(schema.fields[0].field_type, FieldType::Int(64));
    assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);
    assert!(!schema.fields[0].nullable);

    assert_eq!(schema.fields[1].field_name, "email");
    assert_eq!(schema.fields[1].field_type, FieldType::StringFixed(255));
    assert!(!schema.fields[1].nullable);

    assert_eq!(schema.fields[2].field_name, "age");
    assert_eq!(schema.fields[2].field_type, FieldType::Int(32));
    assert!(schema.fields[2].nullable);
}

#[test]
fn create_table_plan_detects_temporary_flag() {
    let plan = create_table_plan_from_statement(
        "create temporary table tmp_users (id bigint primary key)",
    )
    .expect("temporary create table should parse");

    assert_eq!(plan.table_id, "tmp_users");
    assert!(plan.temporary);
    assert_eq!(plan.schema.fields.len(), 1);
}

#[test]
fn create_table_schema_maps_varchar_with_length() {
    let (_, schema) =
        create_table_schema_from_statement("create table users (email varchar(34) not null)")
            .expect("create table schema should parse");

    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].field_name, "email");
    assert_eq!(schema.fields[0].field_type, FieldType::StringFixed(34));
    assert!(!schema.fields[0].nullable);
}

#[test]
fn create_table_schema_captures_auto_increment_and_encoding_metadata() {
    let (_, schema) = create_table_schema_from_statement(
            "create table users (id bigint not null auto_increment primary key, username varchar(34) character set utf8mb3 collate utf8mb3_general_ci comment 'login handle')",
        )
        .expect("auto increment and encoding metadata should parse");

    assert_eq!(schema.fields.len(), 2);

    let id_metadata = schema.fields[0]
        .metadata
        .as_ref()
        .expect("id field should include metadata");
    assert!(id_metadata.auto_increment);

    let username_metadata = schema.fields[1]
        .metadata
        .as_ref()
        .expect("username field should include metadata");
    assert_eq!(
        username_metadata.original_sql_type.as_deref(),
        Some("VARCHAR(34)")
    );
    assert_eq!(username_metadata.character_set.as_deref(), Some("utf8mb3"));
    assert_eq!(
        username_metadata.collation.as_deref(),
        Some("utf8mb3_general_ci")
    );
    assert_eq!(username_metadata.comment.as_deref(), Some("login handle"));
}

#[test]
fn create_table_schema_maps_temporal_types_and_preserves_original_sql_type() {
    let (_, schema) = create_table_schema_from_statement(
        "create table events (created_on date, created_at datetime, updated_at timestamp)",
    )
    .expect("temporal types should parse");

    assert_eq!(schema.fields.len(), 3);
    assert_eq!(schema.fields[0].field_type, FieldType::Date);
    assert_eq!(schema.fields[1].field_type, FieldType::DateTime);
    assert_eq!(schema.fields[2].field_type, FieldType::Timestamp);

    assert_eq!(
        schema.fields[0]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_deref()),
        Some("DATE")
    );
    assert_eq!(
        schema.fields[1]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_deref()),
        Some("DATETIME")
    );
    assert_eq!(
        schema.fields[2]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_deref()),
        Some("TIMESTAMP")
    );
}

#[test]
fn create_table_schema_maps_uuid_type_and_preserves_original_sql_type() {
    let (_, schema) =
        create_table_schema_from_statement("create table users (id UUID not null primary key)")
            .expect("uuid type should parse");

    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].field_name, "id");
    assert_eq!(schema.fields[0].field_type, FieldType::Uuid);
    assert!(!schema.fields[0].nullable);
    assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);

    assert_eq!(
        schema.fields[0]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_deref()),
        Some("UUID")
    );
}

#[test]
fn create_table_schema_tracks_table_level_keys_defaults_and_enum() {
    let sql = "CREATE TABLE `__account` (
          `uid` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
          `id_person` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
          `id_device` varchar(34) DEFAULT NULL,
          `id_organization` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
          `role` enum('user','admin') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT 'user',
          `date_created` bigint NOT NULL DEFAULT '0',
          `date_updated` bigint NOT NULL DEFAULT '0',
          `date_lastlogin` bigint NOT NULL DEFAULT '0',
          `is_verified` tinyint unsigned NOT NULL DEFAULT '0',
          `is_deleted` tinyint unsigned NOT NULL DEFAULT '0',
          PRIMARY KEY (`uid`),
          KEY `id_device` (`id_device`),
          KEY `id_person` (`id_person`),
          CONSTRAINT `__account_ibfk_1` FOREIGN KEY (`id_device`) REFERENCES `__devices` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE,
          CONSTRAINT `__account_ibfk_2` FOREIGN KEY (`id_person`) REFERENCES `__person` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;";

    let (table_id, schema) = create_table_schema_from_statement(sql).expect("schema should parse");

    assert_eq!(table_id, "__account");

    let uid = schema.field("uid").expect("uid field should exist");
    assert_eq!(uid.indexed, FieldIndex::PrimaryKey);
    assert_eq!(uid.default_value.as_deref(), Some(&b""[..]));

    let id_person = schema
        .field("id_person")
        .expect("id_person field should exist");
    assert_eq!(id_person.indexed, FieldIndex::Indexed);
    assert!(id_person.default_value.is_none());

    let id_device = schema
        .field("id_device")
        .expect("id_device field should exist");
    assert_eq!(id_device.indexed, FieldIndex::Indexed);
    assert!(id_device.default_value.is_none());

    let role = schema.field("role").expect("role field should exist");
    assert_eq!(
        role.field_type,
        FieldType::Enum(vec!["user".to_string(), "admin".to_string()])
    );
    assert_eq!(role.default_value.as_deref(), Some(&b"user"[..]));
    assert_eq!(
        role.metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_deref()),
        Some("ENUM('user', 'admin')")
    );
}

#[test]
fn create_table_schema_marks_unique_columns_in_metadata() {
    let (_, schema) = create_table_schema_from_statement(
        "create table users (id bigint primary key, email varchar(255) unique, login varchar(255), unique key uq_login (login))",
    )
    .expect("schema should parse");

    let email = schema.field("email").expect("email field should exist");
    assert_eq!(email.indexed, FieldIndex::Indexed);
    assert!(email
        .metadata
        .as_ref()
        .map(|metadata| metadata.unique)
        .unwrap_or(false));

    let login = schema.field("login").expect("login field should exist");
    assert_eq!(login.indexed, FieldIndex::Indexed);
    assert!(login
        .metadata
        .as_ref()
        .map(|metadata| metadata.unique)
        .unwrap_or(false));
}

#[test]
fn alter_table_change_plan_parses_add_drop_and_rename() {
    let plan = parse_alter_table_change_plan_from_statement(
            "alter table users add column status varchar(20) not null default 'active', drop column legacy, rename column email to login_email",
        )
        .expect("alter table should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.operations.len(), 3);

    match &plan.operations[0] {
        AlterTableChangeOp::AddField(field) => {
            assert_eq!(field.field_name, "status");
            assert_eq!(field.default_value.as_deref(), Some(&b"active"[..]));
        }
        _ => panic!("expected add field operation"),
    }

    match &plan.operations[1] {
        AlterTableChangeOp::DropField(name) => assert_eq!(name, "legacy"),
        _ => panic!("expected drop field operation"),
    }

    match &plan.operations[2] {
        AlterTableChangeOp::RenameField { from, to } => {
            assert_eq!(from, "email");
            assert_eq!(to, "login_email");
        }
        _ => panic!("expected rename field operation"),
    }
}

#[test]
fn alter_table_change_plan_parses_modify_column() {
    let plan = parse_alter_table_change_plan_from_statement(
        "alter table users modify column email varchar(512) not null",
    )
    .expect("alter table modify should parse");

    assert_eq!(plan.table_id, "users");
    assert_eq!(plan.operations.len(), 1);

    match &plan.operations[0] {
        AlterTableChangeOp::ModifyField {
            field_name,
            new_type,
        } => {
            assert_eq!(field_name, "email");
            assert_eq!(new_type, &FieldType::StringFixed(512));
        }
        _ => panic!("expected modify field operation"),
    }
}
