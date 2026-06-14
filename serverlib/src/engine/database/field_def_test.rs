
use super::*;

fn field_with_type(field_type: FieldType) -> FieldDef {
    FieldDef {
        seqno: 1,
        field_name: "value".to_string(),
        field_type,
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }
}

#[test]
fn to_sql_string_marks_signed_integer_fields() {
    assert_eq!(
        field_with_type(FieldType::Int(64)).to_sql_string(),
        "value BIGINT SIGNED NOT NULL"
    );
}

#[test]
fn to_sql_string_marks_unsigned_integer_fields() {
    assert_eq!(
        field_with_type(FieldType::UInt(8)).to_sql_string(),
        "value TINYINT UNSIGNED NOT NULL"
    );
}

#[test]
fn to_sql_string_skips_signedness_for_non_integer_fields() {
    assert_eq!(
        field_with_type(FieldType::Float(32)).to_sql_string(),
        "value FLOAT32 NOT NULL"
    );
}

#[test]
fn to_sql_string_includes_collation_from_metadata() {
    let mut field = field_with_type(FieldType::StringFixed(32));
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("LONGTEXT".to_string()),
        collation: Some("utf8mb4_general_ci".to_string()),
        ..FieldMetadata::default()
    });

    assert_eq!(
        field.to_sql_string(),
        "value LONGTEXT NOT NULL COLLATE utf8mb4_general_ci"
    );
}

#[test]
fn to_sql_string_includes_character_set_from_metadata() {
    let mut field = field_with_type(FieldType::StringFixed(32));
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("TEXT".to_string()),
        character_set: Some("utf8mb4".to_string()),
        ..FieldMetadata::default()
    });

    assert_eq!(
        field.to_sql_string(),
        "value TEXT NOT NULL CHARACTER SET utf8mb4"
    );
}

#[test]
fn to_sql_string_includes_character_set_and_collation_together() {
    let mut field = field_with_type(FieldType::StringFixed(32));
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("LONGTEXT".to_string()),
        character_set: Some("utf8mb4".to_string()),
        collation: Some("utf8mb4_bin".to_string()),
        ..FieldMetadata::default()
    });

    assert_eq!(
        field.to_sql_string(),
        "value LONGTEXT NOT NULL CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
    );
}

#[test]
fn to_sql_string_includes_auto_increment_from_metadata() {
    let mut field = field_with_type(FieldType::UInt(64));
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("BIGINT UNSIGNED".to_string()),
        auto_increment: true,
        ..FieldMetadata::default()
    });

    assert_eq!(
        field.to_sql_string(),
        "value BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"
    );
}

#[test]
fn to_sql_string_includes_collation_and_auto_increment_together() {
    let mut field = field_with_type(FieldType::UInt(64));
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("BIGINT UNSIGNED".to_string()),
        auto_increment: true,
        collation: Some("utf8mb4_bin".to_string()),
        ..FieldMetadata::default()
    });

    assert_eq!(
        field.to_sql_string(),
        "value BIGINT UNSIGNED NOT NULL COLLATE utf8mb4_bin AUTO_INCREMENT"
    );
}

#[test]
fn to_sql_string_uses_original_sql_type_for_temporal_fields() {
    let mut field = field_with_type(FieldType::DateTime);
    field.metadata = Some(FieldMetadata {
        original_sql_type: Some("DATETIME".to_string()),
        ..FieldMetadata::default()
    });

    assert_eq!(field.to_sql_string(), "value DATETIME NOT NULL");
}
