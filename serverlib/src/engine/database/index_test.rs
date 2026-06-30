
use super::*;
use crate::engine::database::field_types::{FieldIndex, FieldType};

#[test]
fn index_id_is_normalized_from_kind_and_field() {
    let field = FieldDef {
        seqno: 1,
        field_name: "UserId".to_string(),
        field_type: FieldType::UInt(64),
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    };

    let index = DatabaseIndex::from_table_field("UserAccounts", &field);

    assert_eq!(index.table_id, "useraccounts");
    assert_eq!(index.field_name, "userid");
    assert_eq!(index.kind, DatabaseIndexKind::Indexed);
    assert_eq!(index.origin, DatabaseIndexOrigin::Derived);
    assert_eq!(index.index_id.0, "ind:useraccounts:userid");
    assert_eq!(index.temp_id, None);
}

#[test]
fn primary_key_index_uses_pri_prefix() {
    
    let field = FieldDef {
        seqno: 1,
        field_name: "Uid".to_string(),
        field_type: FieldType::UInt(64),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    };

    let index = DatabaseIndex::from_table_field("UserAccounts", &field);

    assert_eq!(index.kind, DatabaseIndexKind::PrimaryKey);
    assert_eq!(index.origin, DatabaseIndexOrigin::Derived);
    assert_eq!(index.index_id.0, "pri:useraccounts:uid");
}

#[test]
fn composite_index_id_uses_field_list() {
    let index = DatabaseIndex::from_table_fields_with_origin(
        "UserAccounts",
        DatabaseIndexKind::Indexed,
        DatabaseIndexOrigin::Relationship,
        None,
        vec!["Uid".to_string(), "IdPerson".to_string()],
    );

    assert_eq!(index.origin, DatabaseIndexOrigin::Relationship);
    assert_eq!(index.index_id.0, "rel:ind:useraccounts:uid,idperson");
}

#[test]
fn temporary_index_uses_temp_id_in_identity() {
    let index = DatabaseIndex::temporary(
        "UserAccounts",
        DatabaseIndexKind::Indexed,
        "join-1",
        vec!["Uid".to_string()],
    );

    assert_eq!(index.origin, DatabaseIndexOrigin::Temporary);
    assert_eq!(index.temp_id.as_deref(), Some("join-1"));
    assert_eq!(index.index_id.0, "tmp:join-1:ind:useraccounts:uid");
}
