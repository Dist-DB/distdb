use super::*;

#[test]
fn noop_executor_returns_empty_progress() {
    let executor = NoopSchemaMigrationExecutor;
    let progress = executor
        .rewrite_rows(
            &DatabaseCatalog::create_empty_from_name("test").unwrap(),
            "test_table",
        )
        .expect("should return progress");

    assert_eq!(progress.rows_rewritten, 0);
    assert_eq!(progress.rows_total, Some(0));
    assert!(progress.resume_token.is_none());
}

#[test]
fn disk_executor_set_rules_normalizes_table_id() {
    let temp_dir = std::env::temp_dir().join("schema-migration-test");
    let executor = DiskToMemorySchemaMigrationExecutor::new(temp_dir);

    executor
        .set_rules_for_table("USERS", SchemaMutationRuleSet::default())
        .expect("should set rules");

    executor
        .set_rules_for_table("users", SchemaMutationRuleSet::default())
        .expect("should set rules");
}
