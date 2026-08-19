use super::*;
use std::io::BufReader;

fn new_transaction_state() -> crate::session::ImportTransactionState {
    crate::session::ImportTransactionState {
        enabled: false,
        active: false,
        dml_statements_in_batch: 0,
        committed_batches: 0,
        batch_started_at: None,
        current_statement_line: 0,
        batch_first_line: 0,
        pending_statements: Vec::new(),
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
    assert!(executed[1].contains("insert ignore into people"));
}

#[test]
fn import_reader_keeps_delimited_routine_body_intact() {

    let input = "\
DROP FUNCTION IF EXISTS fnnearesttown;\n\
\n\
delimiter //\n\
\n\
CREATE FUNCTION `fnnearesttown`(lon DECIMAL(10,7), lat DECIMAL(10,7)) RETURNS varchar(120) CHARSET utf8mb3\n\
    DETERMINISTIC\n\
BEGIN\n\
\n\
SET @offset = 0.02;\n\
SET @out = \"\";\n\
\n\
SELECT plc.display_name INTO @out\n\
FROM locations.places plc\n\
WHERE plc.longitude > (@lon - @offset)\n\
ORDER BY distance(@lon, @lat, plc.longitude, plc.latitude)\n\
LIMIT 0,1;\n\
\n\
RETURN @out;\n\
\n\
END //\n\
\n\
delimiter ;\n\
";

    let mut executed = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "locations",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            executed.push(statement.trim().to_string());
            Ok(())
        },
    )
    .expect("import reader should succeed");

    assert_eq!(executed.len(), 2, "unexpected statements: {:#?}", executed);
    assert!(executed[0].to_ascii_lowercase().starts_with("drop function"));

    let create = executed[1].to_ascii_lowercase();
    assert!(create.starts_with("create function"), "got: {}", executed[1]);
    assert!(create.contains("begin"), "body lost BEGIN: {}", executed[1]);
    assert!(create.contains("return @out"), "body lost RETURN: {}", executed[1]);
    assert!(create.trim_end().ends_with("end"), "body lost END: {}", executed[1]);

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

            if let Some(rest) = normalized.strip_prefix("insert ignore into ") {
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
    let mut dispatched = Vec::<String>::new();
    let mut transaction_state = new_transaction_state();

    execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, _transaction_state| {
            dispatched.push(statement.trim().to_string());

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
    assert_eq!(dispatched.len(), 3);
    assert_eq!(dispatched[0], "drop table ip_lookup");
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
fn normalize_import_statement_removes_mysql_definer_clause_from_routines() {
    let statement = "CREATE DEFINER=`root`@`%` FUNCTION `fndistance`() RETURNS int RETURN 1";
    let normalized = normalize_import_statement(statement);

    assert_eq!(
        normalized,
        "create FUNCTION `fndistance`() RETURNS int RETURN 1"
    );
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
    assert_eq!(executed, vec!["insert ignore into ip_lookup values (1)"]);
}

#[test]
fn normalize_import_statement_keeps_every_create_table_column() {
    let statement = "CREATE TABLE `places` (\n\
      `uid` bigint unsigned NOT NULL AUTO_INCREMENT,\n\
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
      `id_region` int unsigned NOT NULL,\n\
      `date_updated` bigint NOT NULL,\n\
      PRIMARY KEY (`uid`),\n\
      UNIQUE KEY `id` (`uni_id`,`form`) USING BTREE,\n\
      KEY `class_2` (`class`,`longitude`,`latitude`)\n\
    ) ENGINE=InnoDB AUTO_INCREMENT=4989267 DEFAULT CHARSET=utf8mb3";

    let normalized = normalize_import_statement(statement);

    for column in [
        "`uid`",
        "`id`",
        "`uni_id`",
        "`form`",
        "`class`",
        "`type`",
        "`latitude`",
        "`longitude`",
        "`elevation`",
        "`display_name`",
        "`country_code`",
        "`id_region`",
        "`date_updated`",
    ] {
        assert!(
            normalized.contains(column),
            "normalized create table lost column {column}: {normalized}"
        );
    }

    assert!(!normalized.to_ascii_lowercase().contains("unsigned"));
    assert!(!normalized.to_ascii_uppercase().contains("USING BTREE"));
}

#[test]
fn import_reader_reports_statement_line_in_failures() {
    let input = "-- header comment\ninsert into ip_lookup values (1);\n\ninsert into ip_lookup values (2);\n";

    let mut lines = Vec::<usize>::new();
    let mut transaction_state = new_transaction_state();

    let result = execute_import_from_reader(
        BufReader::new(input.as_bytes()),
        "main",
        &mut transaction_state,
        |_db, statement, transaction_state| {
            lines.push(transaction_state.current_statement_line);

            if statement.contains("(2)") {
                return Err("boom".to_string());
            }

            Ok(())
        },
    );

    assert_eq!(lines, vec![2, 4]);
    assert_eq!(result, Err("boom (at line 4)".to_string()));
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
    assert_eq!(executed, vec!["insert ignore into ip_lookup values (1);"]);
}

#[test]
fn import_reader_skips_mysql_routine_ddl_statements() {
    let input = "\
        /*!50003 DROP FUNCTION IF EXISTS `fndistance` */;\n\
        DELIMITER ;;\n\
        CREATE DEFINER=`root`@`%` FUNCTION `fndistance`() RETURNS decimal(15,7)\n\
        BEGIN\n\
            RETURN 0;\n\
        END ;;\n\
        DELIMITER ;\n\
        insert into ip_lookup values (1);\n\
        drop procedure if exists `sp_placesnearby`;\n\
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
    .expect("import reader should preserve mysql routine ddl as a single statement");

    assert_eq!(transaction_state.committed_batches, 0);
    assert_eq!(executed.len(), 3);
    assert!(executed[0].starts_with("create FUNCTION `fndistance`() RETURNS decimal(15,7)"));
    assert!(executed[0].contains("BEGIN"));
    assert!(executed[0].contains("RETURN 0;"));
    assert!(executed[0].ends_with("END"));
    assert_eq!(executed[1], "insert ignore into ip_lookup values (1)");
    assert_eq!(executed[2], "drop procedure if exists `sp_placesnearby`");
}

#[test]
fn import_transport_error_retry_classifier_matches_expected_errors() {
    assert!(import_transport_error_is_retryable(
        "transport error: failed to read response length: Resource temporarily unavailable (os error 35)"
    ));
    assert!(import_transport_error_is_retryable(
        "transport error: no queued response for request_id"
    ));
    assert!(import_transport_error_is_retryable(
        "transport error: failed to connect to provision.cloud.distdb.com:4001: Connection refused (os error 61)"
    ));
    assert!(import_transport_error_is_retryable(
        "transport error: failed to read response length: peer closed connection without sending TLS close_notify: https://docs.rs/rustls/latest/rustls/manual/_03_howto/index.html#unexpected-eof"
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
        .all(|chunk| chunk.to_ascii_lowercase().starts_with("insert ignore into users values ")));
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

#[test]
fn split_import_insert_values_statement_keeps_insert_ignore_unchanged() {
    let statement = "insert ignore into users values (1,'alice'),(2,'bob')";
    let chunks = split_import_insert_values_statement(statement, 4_096, 16);

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], statement);
}

#[test]
fn split_import_insert_values_statement_keeps_on_duplicate_update_unchanged() {
    let statement = "insert into users values (1,'alice') on duplicate key update name='alice'";
    let chunks = split_import_insert_values_statement(statement, 4_096, 16);

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], statement);
}
