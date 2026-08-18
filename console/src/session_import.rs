use super::{ConsoleSession, ImportTransactionState};
use crate::{import, IMPORT_BEGIN_STATEMENT, IMPORT_TRANSPORT_RETRY_LIMIT};
use connector::{ConnectorCommand, ConnectorRequest, ConnectorResult, ConnectorTransport, DataQuery};
use std::io::BufReader;
use std::path::Path;
use std::time::Duration;

pub(super) fn execute_import_file(
    session: &mut ConsoleSession,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(database_id) = session.current_database.clone() else {
        return Err("no active database selected; run `use <database>;` first".into());
    };

    let path = Path::new(file_name);
    let file = std::fs::File::open(path)
        .map_err(|err| format!("failed to open import file '{}': {}", path.display(), err))?;

    log::info!(
        "import started: file={} target_database={}",
        path.display(),
        database_id
    );

    let mut transaction_state = ImportTransactionState {
        enabled: true,
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
    };

    import::execute_import_from_reader(
        BufReader::new(file),
        &database_id,
        &mut transaction_state,
        |database_id, statement, transaction_state| {
            execute_import_with_batching(session, database_id, statement, transaction_state)
        },
    )
    .map_err(|err| {
        log::warn!(
            "import failed: file={} target_database={} error={}",
            path.display(),
            database_id,
            err,
        );
        let boxed: Box<dyn std::error::Error> = err.into();
        boxed
    })?;

    finalize_import_batching(session, &database_id, &mut transaction_state)
        .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;

    log::info!(
        "import completed: committed_batches={} exec_ms={} begin_ms={} commit_ms={} query_ms={} stmt_calls={} max_stmt_ms={} max_stmt_kind={} max_stmt_bytes={}",
        transaction_state.committed_batches,
        transaction_state.execute_statement_ms,
        transaction_state.begin_statement_ms,
        transaction_state.commit_statement_ms,
        transaction_state.query_statement_ms,
        transaction_state.statement_calls,
        transaction_state.max_statement_ms,
        transaction_state.max_statement_kind.map(|kind| kind.as_str()).unwrap_or("<none>"),
        transaction_state.max_statement_bytes,
    );

    session.push_log(format!(
        "import file={} db={} committed_batches={} exec_ms={} begin_ms={} commit_ms={} query_ms={} stmt_calls={} max_stmt_ms={} max_stmt_kind={} max_stmt_bytes={}",
        path.display(),
        database_id,
        transaction_state.committed_batches,
        transaction_state.execute_statement_ms,
        transaction_state.begin_statement_ms,
        transaction_state.commit_statement_ms,
        transaction_state.query_statement_ms,
        transaction_state.statement_calls,
        transaction_state.max_statement_ms,
        transaction_state.max_statement_kind.map(|kind| kind.as_str()).unwrap_or("<none>"),
        transaction_state.max_statement_bytes,
    ));

    Ok(())
}

pub(super) fn execute_import_statement(
    session: &mut ConsoleSession,
    database_id: &str,
    statement: &str,
    transaction_state: &mut ImportTransactionState,
) -> Result<(), String> {

    let statement_kind = import::classify_import_statement(statement);

    for attempt in 0..=IMPORT_TRANSPORT_RETRY_LIMIT {

        match execute_import_statement_once(
            session,
            database_id,
            statement,
            statement_kind,
            transaction_state,
        ) {

            Ok(outcome) => return outcome,

            Err(message) => {
                let is_retryable = import::import_transport_error_is_retryable(&message);

                if !is_retryable || attempt >= IMPORT_TRANSPORT_RETRY_LIMIT {
                    log::warn!(
                        "import transport failed: db={} line={} kind={} statement_bytes={} preview='{}' error={}",
                        database_id,
                        transaction_state.current_statement_line,
                        statement_kind.as_str(),
                        statement.len(),
                        import::statement_preview(statement),
                        message,
                    );
                    return Err(message);
                }

                recover_import_transport(session)?;
                replay_pending_batch(session, database_id, transaction_state)?;
            }
        }
    }

    Err("import transport retry loop exhausted".to_string())

}

/// `Ok(Ok(()))`/`Ok(Err(..))` carry the server outcome; the outer `Err` is a transport failure.
fn execute_import_statement_once(
    session: &mut ConsoleSession,
    database_id: &str,
    statement: &str,
    statement_kind: import::ImportStatementKind,
    transaction_state: &mut ImportTransactionState,
) -> Result<Result<(), String>, String> {

    let request_id = session.next_request_id();

    let request = ConnectorRequest::new(
        request_id,
        ConnectorCommand::Query {
            query: DataQuery {
                database_id: database_id.to_string(),
                sql: statement.to_string(),
            },
        },
    );

    let execute_started_at = std::time::Instant::now();
    let result = session.runtime.transport().request(&request);
    let elapsed_ms = execute_started_at.elapsed().as_millis();

    import::record_import_statement_timing(
        transaction_state,
        statement_kind,
        statement.len(),
        elapsed_ms,
    );

    match result {
        Ok(response) => Ok(match response.result {
            ConnectorResult::Error(message) => {
                log::warn!(
                    "import execution failed: db={} line={} kind={} statement_bytes={} preview='{}' error={}",
                    database_id,
                    transaction_state.current_statement_line,
                    statement_kind.as_str(),
                    statement.len(),
                    import::statement_preview(statement),
                    message,
                );
                Err(message)
            }
            _ => Ok(()),
        }),

        Err(err) => Err(err.to_string()),
    }

}

/// Re-opens the batch on a replacement connection: the server discards the transaction
/// with the old stream, so `begin` plus every buffered DML must be re-issued.
fn replay_pending_batch(
    session: &mut ConsoleSession,
    database_id: &str,
    transaction_state: &mut ImportTransactionState,
) -> Result<(), String> {

    if !transaction_state.active {
        return Ok(());
    }

    let pending = transaction_state.pending_statements.clone();

    log::info!(
        "import replaying batch after transport recovery: db={} statements={}",
        database_id,
        pending.len(),
    );

    execute_import_statement_once(
        session,
        database_id,
        IMPORT_BEGIN_STATEMENT,
        import::ImportStatementKind::Begin,
        transaction_state,
    )
    .map_err(|err| format!("batch replay failed to begin transaction: {err}"))?
    .map_err(|err| format!("batch replay failed to begin transaction: {err}"))?;

    for statement in &pending {

        let outcome = execute_import_statement_once(
            session,
            database_id,
            statement,
            import::classify_import_statement(statement),
            transaction_state,
        )
        .map_err(|err| format!("batch replay transport failure: {err}"))?;

        // A replay after a timed-out commit can re-apply rows the server already
        // persisted, so treat duplicates as already-applied.
        if let Err(err) = outcome
            && !import::import_duplicate_key_error_is_skippable(&err) {
                return Err(format!("batch replay failed: {err}"));
            }

    }

    Ok(())

}

fn reset_active_batch_state(transaction_state: &mut ImportTransactionState) {
    transaction_state.active = false;
    transaction_state.dml_statements_in_batch = 0;
    transaction_state.batch_started_at = None;
    transaction_state.batch_first_line = 0;
    transaction_state.pending_statements.clear();
}

fn mark_batch_committed(transaction_state: &mut ImportTransactionState) {
    transaction_state.committed_batches += 1;
    reset_active_batch_state(transaction_state);
}

fn rollback_active_batch(
    session: &mut ConsoleSession,
    database_id: &str,
    transaction_state: &mut ImportTransactionState,
) {
    let _ = execute_import_statement(session, database_id, "rollback", transaction_state);
    reset_active_batch_state(transaction_state);
}

pub(super) fn execute_import_with_batching(
    session: &mut ConsoleSession,
    database_id: &str,
    statement: &str,
    transaction_state: &mut ImportTransactionState,
) -> Result<(), String> {

    let is_dml = import::statement_is_import_batchable_dml(statement);

    if transaction_state.enabled && is_dml {
        
        if !transaction_state.active {
            match execute_import_statement(session, database_id, IMPORT_BEGIN_STATEMENT, transaction_state)
            {
                Ok(()) => {
                    transaction_state.active = true;
                    transaction_state.batch_started_at = Some(std::time::Instant::now());
                    transaction_state.batch_first_line = transaction_state.current_statement_line;
                }

                Err(err) => {
                    transaction_state.enabled = false;
                    log::warn!(
                        "import transactional batching disabled: failed to begin transaction: {}",
                        err
                    );
                }
            }
        }

        match execute_import_statement(session, database_id, statement, transaction_state) {

            Ok(()) => {},

            Err(err) => {
                if transaction_state.active && import::import_duplicate_key_error_is_skippable(&err) {
                    rollback_active_batch(session, database_id, transaction_state);
                }

                log::warn!(
                    "import batch failed: db={} lines={}-{} statement_bytes={} preview='{}' error={}",
                    database_id,
                    transaction_state.batch_first_line,
                    transaction_state.current_statement_line,
                    statement.len(),
                    import::statement_preview(statement),
                    err,
                );

                return Err(err);
            }

        }

        if transaction_state.active {

            transaction_state.pending_statements.push(statement.to_string());
            transaction_state.dml_statements_in_batch += 1;

            let should_commit_by_size =
                transaction_state.dml_statements_in_batch >= import::import_transaction_batch_size();

            let should_commit_by_age = transaction_state
                .batch_started_at
                .map(|started_at| {
                    started_at.elapsed().as_millis() >= import::import_transaction_batch_max_age_ms()
                })
                .unwrap_or(false);

            if should_commit_by_size || should_commit_by_age {
                match execute_import_statement(session, database_id, "commit", transaction_state) {
                    Ok(()) => {
                        mark_batch_committed(transaction_state);
                    }

                    Err(err) => {
                        log::warn!(
                            "import batch commit failed: db={} lines={}-{} queued_dml={} error={}",
                            database_id,
                            transaction_state.batch_first_line,
                            transaction_state.current_statement_line,
                            transaction_state.dml_statements_in_batch,
                            err,
                        );

                        if import::import_duplicate_key_error_is_skippable(&err) {
                            rollback_active_batch(session, database_id, transaction_state);
                            return Err(err);
                        }

                        return Err(err);
                    }
                }
            }
        }

        return Ok(());

    }

    if transaction_state.active {
        execute_import_statement(session, database_id, "commit", transaction_state)?;
        mark_batch_committed(transaction_state);
    }

    match execute_import_statement(session, database_id, statement, transaction_state) {

        Ok(()) => Ok(()),
        
        Err(err) => {
            log::warn!(
                "import statement failed outside batching: db={} line={} statement_bytes={} preview='{}' error={}",
                database_id,
                transaction_state.current_statement_line,
                statement.len(),
                import::statement_preview(statement),
                err,
            );
            Err(err)
        }

    }

}

pub(super) fn finalize_import_batching(
    session: &mut ConsoleSession,
    database_id: &str,
    transaction_state: &mut ImportTransactionState,
) -> Result<(), String> {

    if !transaction_state.active {
        return Ok(());
    }

    match execute_import_statement(session, database_id, "commit", transaction_state) {
        
        Ok(()) => {
            mark_batch_committed(transaction_state);
            Ok(())
        },

        Err(err) => {
            if import::import_duplicate_key_error_is_skippable(&err) {
                rollback_active_batch(session, database_id, transaction_state);
                log::warn!(
                    "import finalize skipped duplicate-key batch after rollback: {}",
                    err
                );
                Ok(())
            } else {
                log::warn!(
                    "import finalize failed: db={} lines={}-{} queued_dml={} error={}",
                    database_id,
                    transaction_state.batch_first_line,
                    transaction_state.current_statement_line,
                    transaction_state.dml_statements_in_batch,
                    err,
                );
                Err(err)
            }
        }

    }
    
}

pub(super) fn recover_import_transport(session: &mut ConsoleSession) -> Result<(), String> {

    session.runtime.transport().disconnect_active_peer();

    session
        .runtime
        .transport_mut()
        .connect_active_peer()
        .map_err(|err| format!("transport reconnect failed: {err}"))?;

    std::thread::sleep(Duration::from_millis(25));
    Ok(())

}

#[cfg(test)]
#[path = "session_import_test.rs"]
mod tests;
