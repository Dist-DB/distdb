
use super::*;
use crate::{DatabaseIndexKind, FieldIndex, FieldType};

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
fn create_table_plan_infers_columns_from_as_select_projection() {
    let plan = create_table_plan_from_statement(
        "create temporary table tmp_places (index(longitude), index(latitude)) engine = memory as (select place.uid, place.longitude, place.latitude from places place)",
    )
    .expect("temporary CTAS plan should parse");

    assert_eq!(plan.table_id, "tmp_places");
    assert!(plan.temporary);

    let field_names = plan
        .schema
        .fields
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["uid", "longitude", "latitude"]);
}

#[test]
fn create_table_schema_keeps_every_mysql_dump_column() {
    let (_, schema) = create_table_schema_from_statement(
        "CREATE TABLE `places` (\n\
          `uid` bigint  NOT NULL AUTO_INCREMENT,\n\
          `id` bigint NOT NULL,\n\
          `uni_id` bigint NOT NULL,\n\
          `form` varchar(3) NOT NULL DEFAULT '',\n\
          `class` varchar(10) DEFAULT NULL,\n\
          `type` varchar(1) DEFAULT NULL,\n\
          `latitude` decimal(10,7) NOT NULL,\n\
          `longitude` decimal(10,7) NOT NULL,\n\
          `elevation` int NOT NULL,\n\
          `display_name` varchar(120) NOT NULL,\n\
          `country_code` varchar(10) NOT NULL,\n\
          `id_region` int  NOT NULL,\n\
          `date_updated` bigint NOT NULL,\n\
          PRIMARY KEY (`uid`),\n\
          UNIQUE KEY `id` (`uni_id`,`form`),\n\
          KEY `id_region` (`id_region`),\n\
          KEY `latitude` (`latitude`),\n\
          KEY `longitude` (`longitude`),\n\
          KEY `display_name` (`display_name`),\n\
          KEY `form` (`form`),\n\
          KEY `class` (`class`),\n\
          KEY `country_code` (`country_code`),\n\
          KEY `type` (`type`),\n\
          KEY `uni_id` (`uni_id`),\n\
          KEY `id_2` (`id`),\n\
          KEY `country_code_2` (`country_code`,`class`),\n\
          KEY `class_2` (`class`,`longitude`,`latitude`),\n\
          CONSTRAINT `places_ibfk_1` FOREIGN KEY (`id_region`) REFERENCES `regions` (`id`)\n\
        ) ENGINE=InnoDB AUTO_INCREMENT=4989267 DEFAULT CHARSET=utf8mb3",
    )
    .expect("mysql dump create table should parse");

    let field_names = schema
        .fields
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        field_names,
        vec![
            "uid",
            "id",
            "uni_id",
            "form",
            "class",
            "type",
            "latitude",
            "longitude",
            "elevation",
            "display_name",
            "country_code",
            "id_region",
            "date_updated",
        ]
    );
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
fn create_table_schema_does_not_mark_composite_unique_columns_as_individually_unique() {
    let (_, schema) = create_table_schema_from_statement(
        "create table places (uid bigint primary key, uni_id bigint not null, form varchar(3) not null default '', unique key uq_uni_id_form (uni_id, form))",
    )
    .expect("schema should parse");

    let uni_id = schema.field("uni_id").expect("uni_id field should exist");
    assert_eq!(uni_id.indexed, FieldIndex::Indexed);
    assert!(!uni_id
        .metadata
        .as_ref()
        .map(|metadata| metadata.unique)
        .unwrap_or(false));

    let form = schema.field("form").expect("form field should exist");
    assert_eq!(form.indexed, FieldIndex::Indexed);
    assert!(!form
        .metadata
        .as_ref()
        .map(|metadata| metadata.unique)
        .unwrap_or(false));
}

#[test]
fn create_table_plan_captures_composite_unique_index_definition() {
    let plan = create_table_plan_from_statement(
        "create table places (uid bigint primary key, uni_id bigint not null, form varchar(3) not null default '', unique key uq_uni_id_form (uni_id, form))",
    )
    .expect("schema should parse");

    assert!(plan
        .composite_indexes
        .iter()
        .any(|(kind, fields)| {
            *kind == DatabaseIndexKind::Unique
                && fields == &vec!["uni_id".to_string(), "form".to_string()]
        }));
}

#[test]
fn create_table_plan_captures_composite_non_unique_index_definition() {
    let plan = create_table_plan_from_statement(
        "create table geo_points (uid bigint primary key, latitude double not null, longitude double not null, key idx_lat_lon (latitude, longitude))",
    )
    .expect("schema should parse");

    assert!(plan
        .composite_indexes
        .iter()
        .any(|(kind, fields)| {
            *kind == DatabaseIndexKind::Indexed
                && fields == &vec!["latitude".to_string(), "longitude".to_string()]
        }));
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
        AlterTableChangeOp::ModifyField(field) => {
            assert_eq!(field.field_name, "email");
            assert_eq!(field.field_type, FieldType::StringFixed(512));
        }
        _ => panic!("expected modify field operation"),
    }
}

#[test]
fn alter_table_change_plan_parses_modify_bigint_with_default() {
    let plan = parse_alter_table_change_plan_from_statement(
        "alter table places modify date_updated bigint default 0",
    )
    .expect("alter table bigint modify should parse");

    assert_eq!(plan.operations.len(), 1);
    match &plan.operations[0] {
        AlterTableChangeOp::ModifyField(field) => {
            assert_eq!(field.field_name, "date_updated");
            assert_eq!(field.field_type, FieldType::Int(64));
            assert_eq!(field.default_value.as_deref(), Some(&b"0"[..]));
        }
        _ => panic!("expected modify field operation"),
    }
}
