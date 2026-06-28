use std::collections::HashMap;

use super::{
    execute_sql_cursor, CursorDirective, SelectReadPlanCursorSource,
    SqlCursorFrame, SqlCursorSource,
    VecSqlCursorSource,
};
use crate::engine::execution::ConditionValueProvider;
use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, parse_select_read_plan_from_statement,
    ConcurrentWalManager, DatabaseCatalog, FieldDef, FieldIndex, FieldType,
    RuntimeIndexStore, TableSchema, TransactionId, TransactionKind,
    TransactionRecord, UserId,
};

#[derive(Debug, Default)]
struct TrackingCursorSource {
    rows: Vec<HashMap<String, Vec<u8>>>,
    index: usize,
    open_count: usize,
    close_count: usize,
    fail_fetch_at: Option<usize>,
}

impl TrackingCursorSource {
    fn from_rows(rows: Vec<HashMap<String, Vec<u8>>>) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }
}

impl SqlCursorSource for TrackingCursorSource {
    fn open(&mut self) -> Result<(), String> {
        self.index = 0;
        self.open_count += 1;
        Ok(())
    }

    fn fetch_next(&mut self) -> Result<Option<HashMap<String, Vec<u8>>>, String> {
        if self.fail_fetch_at.is_some_and(|idx| idx == self.index) {
            return Err("forced fetch failure".to_string());
        }

        if self.index >= self.rows.len() {
            return Ok(None);
        }

        let row = self.rows[self.index].clone();
        self.index += 1;

        Ok(Some(row))
    }

    fn close(&mut self) -> Result<(), String> {
        self.close_count += 1;
        Ok(())
    }
}

fn row(field_name: &str, value: &str) -> HashMap<String, Vec<u8>> {
    let mut row = HashMap::new();
    row.insert(field_name.to_string(), value.as_bytes().to_vec());
    row
}

fn table_schema(fields: Vec<(&str, u32, FieldType, FieldIndex, bool)>) -> TableSchema {
    TableSchema::new(
        fields
            .into_iter()
            .map(|(field_name, seqno, field_type, indexed, nullable)| FieldDef {
                seqno,
                field_name: field_name.to_string(),
                field_type,
                nullable,
                indexed,
                default_value: None,
                metadata: None,
            })
            .collect(),
    )
}

fn seed_cursor_rows(catalog: &mut DatabaseCatalog, wal: &ConcurrentWalManager) {
    let users_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::None, false),
    ]);
    catalog
        .register_table("users", users_schema.clone())
        .expect("users table should register");

    let profiles_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("user_id", 2, FieldType::UInt(64), FieldIndex::Indexed, false),
        ("name", 3, FieldType::Text, FieldIndex::None, false),
    ]);
    catalog
        .register_table("profiles", profiles_schema.clone())
        .expect("profiles table should register");

    let actor = UserId("test-user".to_string());

    let mut user_row = HashMap::new();
    user_row.insert("id".to_string(), b"1".to_vec());
    user_row.insert("email".to_string(), b"sam@example.com".to_vec());
    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&users_schema, &user_row)
                .expect("user row should encode"),
        ),
    )
    .expect("user row should append");

    let mut other_user_row = HashMap::new();
    other_user_row.insert("id".to_string(), b"2".to_vec());
    other_user_row.insert("email".to_string(), b"alex@example.com".to_vec());
    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(2),
            None,
            None,
            2,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&users_schema, &other_user_row)
                .expect("user row should encode"),
        ),
    )
    .expect("user row should append");

    let mut profile_row = HashMap::new();
    profile_row.insert("id".to_string(), b"10".to_vec());
    profile_row.insert("user_id".to_string(), b"1".to_vec());
    profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(10),
            None,
            None,
            10,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &profile_row)
                .expect("profile row should encode"),
        ),
    )
    .expect("profile row should append");
}

#[test]
fn execute_sql_cursor_iterates_all_rows_and_sets_not_found() {
    let mut source = VecSqlCursorSource::new(vec![row("id", "1"), row("id", "2")]);
    let mut frame = SqlCursorFrame::new();
    let mut seen = Vec::new();

    let result = execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
        seen.push(
            String::from_utf8(cursor_frame.value("id").cloned().expect("id must exist"))
                .expect("id must be utf8"),
        );
        Ok(CursorDirective::<()>::Next)
    })
    .expect("cursor execution should succeed");

    assert!(result.is_none());
    assert_eq!(seen, vec!["1".to_string(), "2".to_string()]);
    assert_eq!(frame.diagnostics.fetched_rows, 2);
    assert!(frame.diagnostics.not_found);
    assert!(frame.diagnostics.opened);
    assert!(frame.diagnostics.closed);
}

#[test]
fn execute_sql_cursor_supports_break_directive() {
    let mut source = VecSqlCursorSource::new(vec![row("id", "1"), row("id", "2")]);
    let mut frame = SqlCursorFrame::new();
    let mut seen = 0usize;

    let result = execute_sql_cursor(&mut source, &mut frame, &mut |_cursor_frame| {
        seen += 1;
        Ok(CursorDirective::<()>::Break)
    })
    .expect("cursor execution should succeed");

    assert!(result.is_none());
    assert_eq!(seen, 1);
    assert_eq!(frame.diagnostics.fetched_rows, 1);
    assert!(!frame.diagnostics.not_found);
}

#[test]
fn execute_sql_cursor_supports_return_directive() {
    let mut source = VecSqlCursorSource::new(vec![row("id", "1"), row("id", "2")]);
    let mut frame = SqlCursorFrame::new();

    let result = execute_sql_cursor(&mut source, &mut frame, &mut |_cursor_frame| {
        Ok(CursorDirective::Return("done".to_string()))
    })
    .expect("cursor execution should succeed");

    assert_eq!(result, Some("done".to_string()));
    assert_eq!(frame.diagnostics.fetched_rows, 1);
    assert!(!frame.diagnostics.not_found);
    assert!(frame.diagnostics.closed);
}

#[test]
fn execute_sql_cursor_closes_source_when_callback_fails() {
    let mut source = TrackingCursorSource::from_rows(vec![row("id", "1")]);
    let mut frame = SqlCursorFrame::new();

    let err = execute_sql_cursor(&mut source, &mut frame, &mut |_cursor_frame| {
        Result::<CursorDirective<()>, String>::Err("forced callback failure".to_string())
    })
    .expect_err("cursor execution should fail");

    assert!(err.contains("forced callback failure"));
    assert_eq!(source.open_count, 1);
    assert_eq!(source.close_count, 1);
    assert!(frame.diagnostics.closed);
}

#[test]
fn execute_sql_cursor_closes_source_when_fetch_fails() {
    let mut source = TrackingCursorSource::from_rows(vec![row("id", "1")]);
    source.fail_fetch_at = Some(0);

    let mut frame = SqlCursorFrame::new();

    let err = execute_sql_cursor(&mut source, &mut frame, &mut |_cursor_frame| {
        Ok(CursorDirective::<()>::Next)
    })
    .expect_err("cursor execution should fail");

    assert!(err.contains("forced fetch failure"));
    assert_eq!(source.open_count, 1);
    assert_eq!(source.close_count, 1);
    assert!(frame.diagnostics.closed);
}

#[test]
fn sql_cursor_frame_resolves_qualified_and_local_bindings() {
    let mut source = VecSqlCursorSource::new(vec![row("users.id", "7")]);
    let mut frame = SqlCursorFrame::new();
    frame.set_local_binding("item_count", b"42".to_vec());

    execute_sql_cursor(&mut source, &mut frame, &mut |_cursor_frame| {
        Ok(CursorDirective::<()>::Break)
    })
    .expect("cursor execution should succeed");

    assert_eq!(frame.value("users.id"), Some(&b"7".to_vec()));
    assert_eq!(frame.value("id"), Some(&b"7".to_vec()));
    assert_eq!(frame.value("item_count"), Some(&b"42".to_vec()));
}

#[test]
fn select_read_plan_cursor_source_supports_projection_only_plans() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let plan = parse_select_read_plan_from_statement("select concat('sa', 'm') as value")
        .expect("projection-only read plan should parse");

    let mut source = SelectReadPlanCursorSource::from_read_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &plan,
    )
    .expect("projection-only cursor source should materialize");

    let mut frame = SqlCursorFrame::new();
    let mut seen = Vec::new();

    execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
        seen.push(
            String::from_utf8(cursor_frame.value("value").cloned().expect("value must exist"))
                .expect("value must be utf8"),
        );
        Ok(CursorDirective::<()>::Next)
    })
    .expect("cursor execution should succeed");

    assert_eq!(seen, vec!["sam".to_string()]);
}

#[test]
fn select_read_plan_cursor_source_supports_relation_plans() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_cursor_rows(&mut catalog, &wal);

    let plan = parse_select_read_plan_from_statement("select id, email from users where id = 1")
        .expect("relation read plan should parse");

    let mut source = SelectReadPlanCursorSource::from_read_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &plan,
    )
    .expect("relation cursor source should materialize");

    let mut frame = SqlCursorFrame::new();
    let mut seen = Vec::new();

    execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
        let id = String::from_utf8(cursor_frame.value("id").cloned().expect("id exists"))
            .expect("id utf8");
        let email = String::from_utf8(
            cursor_frame
                .value("email")
                .cloned()
                .expect("email exists"),
        )
        .expect("email utf8");
        seen.push((id, email));
        Ok(CursorDirective::<()>::Next)
    })
    .expect("cursor execution should succeed");

    assert_eq!(seen, vec![("1".to_string(), "sam@example.com".to_string())]);
}

#[test]
fn select_read_plan_cursor_source_supports_joined_plans() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_cursor_rows(&mut catalog, &wal);

    let plan = parse_select_read_plan_from_statement(
        "select u.email as email, p.name as name from users u left join profiles p on u.id = p.user_id",
    )
    .expect("join read plan should parse");

    let mut source = SelectReadPlanCursorSource::from_read_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &plan,
    )
    .expect("join cursor source should materialize");

    let mut frame = SqlCursorFrame::new();
    let mut seen = Vec::new();

    execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
        let email = String::from_utf8(
            cursor_frame
                .value("email")
                .cloned()
                .expect("email exists"),
        )
        .expect("email utf8");

        let name = String::from_utf8(
            cursor_frame
                .value("name")
                .cloned()
                .expect("name exists"),
        )
        .expect("name utf8");

        seen.push((email, name));

        Ok(CursorDirective::<()>::Next)
    })
    .expect("cursor execution should succeed");

    seen.sort();

    assert_eq!(
        seen,
        vec![
            ("alex@example.com".to_string(), "NULL".to_string()),
            ("sam@example.com".to_string(), "Sam".to_string()),
        ]
    );
}
