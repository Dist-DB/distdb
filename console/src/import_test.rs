use super::*;
use std::io::BufReader;

fn new_transaction_state() -> super::super::ImportTransactionState {
    super::super::ImportTransactionState {
        enabled: false,
        active: false,
        dml_statements_in_batch: 0,
        committed_batches: 0,
        batch_started_at: None,
        statement_calls: 0,
        execute_statement_ms: 0,
        begin_statement_ms: 0,
        commit_statement_ms: 0,
        query_statement_ms: 0,
        max_statement_ms: 0,
        max_statement_kind: None,
        max_statement_bytes: 0,
    }
}

fn split_import_insert_values_statement(
    statement: &str,
    max_bytes: usize,
    max_tuples_per_chunk: usize,
) -> Vec<String> {
    let mut chunks = Vec::<String>::new();
    stream_import_insert_values_statements(statement, max_bytes, max_tuples_per_chunk, |chunk| {
        chunks.push(chunk.to_string());
        Ok(())
    })
    .expect("import chunk splitting should not fail when collecting chunks");

    chunks
}

#[test]
fn import_reader_splits_and_executes_statements() {
    let input = "\
        -- file header\n\
        use sample;\n\
        create table people (id int, name text);\n\
        insert into people values (1, 'alice;demo');\n\
        # footer\n\
    ";

    let mut executed = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |db, statement, _transaction_state| {
            executed.push(format!("{}:{}", db, statement.trim()));
            Ok(())
        },
    )
    .expect("import reader should succeed");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(executed.len(), 2);
    assert!(executed[0].contains("create table people"));
    assert!(executed[1].contains("insert into people"));
}

#[test]
fn import_reader_populates_mock_table_structures() {
    let input = "\
        create table users (id int, name text);\n\
        insert into users values (1, 'alice');\n\
        insert into users values (2, 'bob');\n\
        create table regions (id int);\n\
        insert into regions values (10);\n\
    ";

    let mut row_counts = std::collections::HashMap::<String, usize>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            let normalized = statement.trim().to_ascii_lowercase();

            if let Some(rest) = normalized.strip_prefix("create table ") {
                let table_name = rest.split_whitespace().next().unwrap_or("");
                if !table_name.is_empty() {
                    row_counts.entry(table_name.to_string()).or_insert(0);
                }
                return Ok(());
            }

            if let Some(rest) = normalized.strip_prefix("insert into ") {
                let table_name = rest.split_whitespace().next().unwrap_or("");
                if table_name.is_empty() {
                    return Err("insert statement did not include table name".to_string());
                }

                let entry = row_counts.entry(table_name.to_string()).or_insert(0);
                *entry += 1;
                return Ok(());
            }

            Err(format!("unexpected statement in import: {}", statement))
        },
    )
    .expect("import reader should succeed");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(row_counts.get("users"), Some(&2));
    assert_eq!(row_counts.get("regions"), Some(&1));
}

#[test]
fn import_reader_skips_drop_table_not_found_errors() {
    let input = "\
        drop table ip_lookup;\n\
        create table ip_lookup (id int);\n\
        insert into ip_lookup values (1);\n\
    ";

    let mut executed = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            let normalized = statement.trim().to_ascii_lowercase();
            if normalized.starts_with("drop table") {
                return Err("drop table failed: 'ip_lookup' not found".to_string());
            }

            executed.push(statement.trim().to_string());
            Ok(())
        },
    )
    .expect("import reader should continue past non-fatal drop errors");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(executed.len(), 2);
}

#[test]
fn normalize_import_statement_removes_mysql_using_clauses() {
    let statement =
        "create table t (id int, primary key (id) USING BTREE, key idx (id) USING HASH)";
    let normalized = normalize_import_statement(statement);

    assert!(!normalized.to_ascii_lowercase().contains("using btree"));
    assert!(!normalized.to_ascii_lowercase().contains("using hash"));
    assert!(normalized.to_ascii_lowercase().contains("primary key (id)"));
    assert!(normalized.to_ascii_lowercase().contains("key idx (id)"));
}

#[test]
fn import_reader_normalizes_mysql_using_clauses_before_execute() {
    let input = "create table t (id int, primary key (id) USING BTREE);";
    let mut transaction_state = new_transaction_state();

    let mut executed_count = 0usize;
    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            if statement.to_ascii_lowercase().contains("using btree") {
                return Err("statement still contains unsupported USING BTREE".to_string());
            }

            executed_count += 1;

            Ok(())
        },
    )
    .expect("import reader should normalize unsupported USING clauses");

    assert_eq!(executed_count, 1);
    assert_eq!(transaction_state.committed_batches, 0);
}

#[test]
fn normalize_import_statement_removes_unsigned_modifier_for_create_table() {
    let statement = "create table t (`is_deleted` tinyint unsigned not null default '0')";
    let normalized = normalize_import_statement(statement);

    assert!(!normalized.to_ascii_lowercase().contains(" unsigned"));
    assert!(normalized.to_ascii_lowercase().contains("tinyint"));
}

#[test]
fn normalize_import_statement_keeps_unsigned_in_non_create_text() {
    let statement = "insert into t values ('unsigned value')";
    let normalized = normalize_import_statement(statement);

    assert_eq!(normalized, statement);
}

#[test]
fn import_reader_skips_mysql_dump_directives() {
    let input = "\
        set @old_foreign_key_checks=@@foreign_key_checks;\n\
        lock tables `ip_lookup` write;\n\
        insert into ip_lookup values (1);\n\
        unlock tables;\n\
    ";

    let mut executed = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            executed.push(statement.trim().to_string());
            Ok(())
        },
    )
    .expect("import reader should skip dump directives");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(executed, vec!["insert into ip_lookup values (1)"]);
}

#[test]
fn import_reader_skips_delimiter_directive_without_space() {
    let input = "DELIMITER$$;insert into ip_lookup values (1);";

    let mut executed = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            executed.push(statement.trim().to_string());
            Ok(())
        },
    )
    .expect("import reader should skip delimiter directives with or without a trailing space");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(executed, vec!["insert into ip_lookup values (1)"]);
}

#[test]
fn import_transport_error_retry_classifier_matches_expected_errors() {
    assert!(import_transport_error_is_retryable(
        "transport error: failed to read response length: Resource temporarily unavailable (os error 35)"
    ));
    assert!(import_transport_error_is_retryable(
        "transport error: no queued response for request_id"
    ));
    assert!(!import_transport_error_is_retryable(
        "command rejected: sql parse failed"
    ));
}

#[test]
fn import_duplicate_key_error_classifier_matches_unique_key_validation_errors() {
    assert!(import_duplicate_key_error_is_skippable(
        "transaction validation failed at staged statement 1: insert failed: duplicate unique key (form=GNS)"
    ));
    assert!(import_duplicate_key_error_is_skippable(
        "insert failed: duplicate primary key (id=1)"
    ));
    assert!(!import_duplicate_key_error_is_skippable(
        "insert failed: unknown column 'form'"
    ));
}

#[test]
fn import_batchable_dml_classifier_matches_expected_statements() {
    assert!(statement_is_import_batchable_dml("insert into x values (1)"));
    assert!(statement_is_import_batchable_dml(" update users set a=1"));
    assert!(statement_is_import_batchable_dml("delete from users"));
    assert!(statement_is_import_batchable_dml("replace into users values (1)"));
    assert!(!statement_is_import_batchable_dml("create table users (id int)"));
    assert!(!statement_is_import_batchable_dml("alter table users add key (id)"));
}

#[test]
fn split_import_insert_values_statement_splits_large_insert_values() {
    let statement = "insert into users values (1,'alice'),(2,'bob'),(3,'charlie')";
    let chunks = split_import_insert_values_statement(statement, 48, 16);

    assert!(chunks.len() >= 2);
    assert!(chunks
        .iter()
        .all(|chunk| chunk.to_ascii_lowercase().starts_with("insert into users values ")));
    assert!(chunks.iter().all(|chunk| chunk.contains("(")));
}

#[test]
fn split_import_insert_values_statement_keeps_non_insert_statement() {
    let statement = "create table users (id int, name text)";
    let chunks = split_import_insert_values_statement(statement, 32, 16);

    assert_eq!(chunks, vec![statement.to_string()]);
}

#[test]
fn split_import_insert_values_statement_respects_tuple_cap() {
    let statement = "insert into users values (1,'alice'),(2,'bob'),(3,'charlie'),(4,'dana')";
    let chunks = split_import_insert_values_statement(statement, 4_096, 2);

    assert_eq!(chunks.len(), 2);
    assert!(chunks[0].contains("(1,'alice')"));
    assert!(chunks[0].contains("(2,'bob')"));
    assert!(chunks[1].contains("(3,'charlie')"));
    assert!(chunks[1].contains("(4,'dana')"));
}
