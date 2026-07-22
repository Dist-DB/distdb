use super::*;
use std::borrow::Borrow;

fn encode_write_group_terminal_payload(expected_count: u64) -> Vec<u8> {
    expected_count.to_le_bytes().to_vec()
}

fn grouped_stream_expected_record_count(
    wal: &ConcurrentWalManager,
    stream_id: &str,
    group_id: TransactionId,
) -> u64 {
    wal.with_records(stream_id, |records| {
        records
            .iter()
            .filter(|record| {
                record.groupid == Some(group_id)
                    && !matches!(
                        record.kind,
                        TransactionKind::WriteBegin |
                        TransactionKind::WriteCommit |
                        TransactionKind::WriteAbort
                    )
            })
            .count() as u64
    })
    .unwrap_or(0)
}

fn summarize_sql_for_error_log(sql: &str) -> String {
    const MAX_CHARS: usize = 240;

    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalized.chars().count() <= MAX_CHARS {
        return normalized;
    }

    let truncated = normalized.chars().take(MAX_CHARS).collect::<String>();
    format!("{truncated}...")
}

fn response_error_message(response: &ConnectorResponse) -> &str {
    match &response.result {
        ConnectorResult::Error(message) => message.as_str(),
        _ => "<no error payload>",
    }
}

fn should_rebuild_runtime_indexes_after_statement_rejection(response: &ConnectorResponse) -> bool {
    let message = response_error_message(response).to_ascii_lowercase();
    message.contains("wal append failed")
}

fn apply_runtime_index_and_equality_batch<R>(
    runtime_indexes: &mut RuntimeIndexStore,
    cache_scope_id: usize,
    stream_id: &str,
    latest_tx_id: u64,
    first_row_id: u64,
    kind: TransactionKind,
    derived_indexes: &[&serverlib::DatabaseIndex],
    row_maps: &[R],
)
where
    R: Borrow<HashMap<String, Vec<u8>>>,
{

    match kind {

        TransactionKind::Delete => {
            runtime_indexes.remove_table_rows_batch(stream_id, derived_indexes, row_maps);
        },

        TransactionKind::Insert | 
        TransactionKind::Update => {
            runtime_indexes.record_table_rows_batch(stream_id, derived_indexes, row_maps);
        },

        _ => {}

    }

    serverlib::apply_equality_cache_row_mutation_batch(
        cache_scope_id,
        stream_id,
        latest_tx_id,
        kind,
        first_row_id,
        row_maps,
    );

}

pub(super) fn append_payload_record(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
) -> Result<(), String> {
    append_payload_record_with_group(wal, wal_id, kind, payload, timestamp_epoch_ms, None)
        .map(|_| ())
}

fn append_payload_record_with_group(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    group_id: Option<TransactionId>,
) -> Result<TransactionId, String> {

    let last_id = wal.latest_transaction_id(wal_id);
    let next_id = TransactionId(last_id.map(|id| id.0 + 1).unwrap_or(1));
    let refid = last_id;
    
    let record_group_id = group_id.or({
        if matches!(kind, TransactionKind::WriteBegin) {
            Some(next_id)
        } else {
            None
        }
    });

    wal.append(
        wal_id,
        TransactionRecord::with_payload(
            next_id,
            record_group_id,
            refid,
            timestamp_epoch_ms,
            UserId::from_username("server"),
            kind,
            payload,
        ),
    )
    .map_err(|e| e.to_string())?;

    Ok(next_id)

}

pub(in super::super) fn append_row_payload_record<T>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: T,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    group_id: Option<TransactionId>,
) -> Result<(), String>
where
    T: Borrow<serverlib::DatabaseTable>,
{

    append_row_payload_record_with_live_row_ids_and_prepared_row_map(
        catalog,
        wal,
        wal_id,
        table.borrow(),
        runtime_indexes,
        kind,
        payload,
        timestamp_epoch_ms,
        refid,
        None,
        None,
        group_id,
    )

}

pub(in super::super) fn append_row_payload_record_with_prepared_row_map<T>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: T,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    prepared_row_map: Option<&HashMap<String, Vec<u8>>>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    group_id: Option<TransactionId>,
) -> Result<(), String>
where
    T: Borrow<serverlib::DatabaseTable>,
{

    append_row_payload_record_with_live_row_ids_and_prepared_row_map(
        catalog,
        wal,
        wal_id,
        table.borrow(),
        runtime_indexes,
        kind,
        payload,
        timestamp_epoch_ms,
        refid,
        None,
        prepared_row_map,
        group_id,
    )

}

pub(super) fn append_row_payload_record_with_live_row_ids<T>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    _wal_id: &str,
    table: T,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    expected_live_row_ids: Option<&HashSet<u64>>,
    group_id: Option<TransactionId>,
) -> Result<(), String>
where
    T: Borrow<serverlib::DatabaseTable>,
{

    append_row_payload_record_with_live_row_ids_and_prepared_row_map(
        catalog,
        wal,
        _wal_id,
        table.borrow(),
        runtime_indexes,
        kind,
        payload,
        timestamp_epoch_ms,
        refid,
        expected_live_row_ids,
        None,
        group_id,
    )

}

pub(super) fn append_row_payload_record_with_live_row_ids_and_prepared_row_map(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    _wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    expected_live_row_ids: Option<&HashSet<u64>>,
    prepared_row_map: Option<&HashMap<String, Vec<u8>>>,
    group_id: Option<TransactionId>,
) -> Result<(), String> {

    let mutation_start = Instant::now();
    let stream_id = table_stream_id(catalog, &table.table_id);
    let payload_context = payload_context_for_table(catalog, &table.table_id);

    if let Some(expected_refid) = refid {

        let live_row_check_start = Instant::now();
        let live_row_exists = if let Some(live_row_ids) = expected_live_row_ids {
            live_row_ids.contains(&expected_refid.0)
        } else {
            let live_row_ids = load_live_rows_with_context(wal, &stream_id, table.schema(), &payload_context)
                .map_err(|err| format!("live row load failed: {err}"))?
                .into_iter()
                .map(|(row_id, _)| row_id)
                .collect::<HashSet<_>>();
            live_row_ids.contains(&expected_refid.0)
        };

        let live_row_check_ms = live_row_check_start.elapsed().as_millis() as u64;

        if !live_row_exists {
            return Err(format!(
                "row mutation references stale or missing live transaction id {}",
                expected_refid.0
            ));
        }

        if live_row_check_ms > 0 {
            log::info!(
                "mutation live-row check timing table={} kind={:?} live_row_check_ms={}",
                table.table_id,
                kind,
                live_row_check_ms,
            );
        }

    }

    let last_id = wal.latest_transaction_id(&stream_id);
    let next_id = TransactionId(last_id.map(|id| id.0 + 1).unwrap_or(1));
    let refid = refid.or(last_id);

    let row_decode_start = Instant::now();
    let mut derived_indexes = derived_indexes_for_table(table).peekable();
    let track_runtime_indexes = derived_indexes.peek().is_some();
    let apply_row_index_mutation = track_runtime_indexes
        && matches!(
            kind,
            TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
        );

    let mut decoded_row_map = None;

    if apply_row_index_mutation && prepared_row_map.is_none() {
        decoded_row_map = Some(
            decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?,
        );
    }

    let row_decode_ms = row_decode_start.elapsed().as_millis() as u64;

    let record = TransactionRecord::with_payload(
        next_id,
        group_id,
        refid,
        timestamp_epoch_ms,
        UserId::from_username("server"),
        kind,
        payload,
    );

    let wal_append_start = Instant::now();

    wal.append_with_context(&stream_id, record, &payload_context)
        .map_err(|e| e.to_string())?;

    let wal_append_ms = wal_append_start.elapsed().as_millis() as u64;
    let latest_tx_id = next_id.0;

    let index_apply_start = Instant::now();

    if apply_row_index_mutation {

        let row_map = prepared_row_map
            .or(decoded_row_map.as_ref())
            .ok_or_else(|| "row payload decode failed: missing row map for index mutation".to_string())?;

        runtime_indexes.apply_table_row_mutation(&stream_id, derived_indexes, kind, row_map);
        
        serverlib::apply_equality_cache_row_mutation(
            wal.cache_scope_id(),
            &stream_id,
            latest_tx_id,
            kind,
            next_id.0,
            row_map,
        );

    }

    let index_apply_ms = index_apply_start.elapsed().as_millis() as u64;
    let total_ms = mutation_start.elapsed().as_millis() as u64;

    log::debug!(
        "mutation timing table={} kind={:?} track_runtime_indexes={} row_decode_ms={} wal_append_ms={} index_apply_ms={} total_ms={}",
        table.table_id,
        kind,
        track_runtime_indexes,
        row_decode_ms,
        wal_append_ms,
        index_apply_ms,
        total_ms,
    );

    Ok(())

}

pub(super) fn append_row_payload_records_batch<R>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payloads: Vec<Vec<u8>>,
    prepared_row_maps: Option<Vec<R>>,
    timestamp_epoch_ms: u64,
    group_id: Option<TransactionId>,
) -> Result<(), String>
where
    R: Borrow<HashMap<String, Vec<u8>>>,
{
    
    if payloads.is_empty() {
        return Ok(());
    }

    let batch_start = Instant::now();
    let stream_id = wal_id.to_string();
    let payload_context = payload_context_for_table(catalog, &table.table_id);
    let derived_indexes = derived_indexes_for_table(table).collect::<Vec<_>>();
    let track_runtime_indexes = !derived_indexes.is_empty();
    let apply_row_index_mutation = track_runtime_indexes
        && matches!(
            kind,
            TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
        );
    let payload_count = payloads.len();

    let last_id = wal.latest_transaction_id(&stream_id);
    let first_row_id = last_id.map(|id| id.0.saturating_add(1)).unwrap_or(1);
    let mut next_id = first_row_id;
    let mut refid = last_id;
    let actor = UserId::from_username("server");

    let mut records = Vec::with_capacity(payloads.len());
    let prepared_row_maps = prepared_row_maps.unwrap_or_default();
    let use_prepared_row_maps = apply_row_index_mutation && prepared_row_maps.len() == payload_count;
    let mut decoded_row_maps: Vec<HashMap<String, Vec<u8>>> = Vec::new();

    if apply_row_index_mutation && !use_prepared_row_maps {
        decoded_row_maps = Vec::with_capacity(payload_count);
    }

    let materialize_start = Instant::now();

    for payload in payloads {

        if apply_row_index_mutation && !use_prepared_row_maps {
            let row_map = decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?;
            decoded_row_maps.push(row_map);
        }

        let tx_id = TransactionId(next_id);
        next_id = next_id.saturating_add(1);

        records.push(TransactionRecord::with_payload(
            tx_id,
            group_id,
            refid,
            timestamp_epoch_ms,
            actor.clone(),
            kind,
            payload,
        ));

        refid = Some(tx_id);

    }

    let materialize_us = materialize_start.elapsed().as_micros() as u64;

    let wal_append_start = Instant::now();
    wal.append_batch_with_context(&stream_id, records, &payload_context)
        .map_err(|err| err.to_string())?;
    let wal_append_us = wal_append_start.elapsed().as_micros() as u64;
    let latest_tx_id = next_id.saturating_sub(1);

    let index_apply_start = Instant::now();
    let pre_index_stats = apply_row_index_mutation.then(|| {
        derived_indexes
            .iter()
            .map(|index| runtime_indexes.stats_for_table(&stream_id, &index.index_id.0))
            .collect::<Vec<_>>()
    });

    if apply_row_index_mutation {
        
        if !use_prepared_row_maps && decoded_row_maps.len() != payload_count {
            return Err("row map preparation mismatch for batch mutation".to_string());
        }

        if use_prepared_row_maps {

            apply_runtime_index_and_equality_batch(
                runtime_indexes,
                wal.cache_scope_id(),
                &stream_id,
                latest_tx_id,
                first_row_id,
                kind,
                &derived_indexes,
                &prepared_row_maps,
            );

        } else {

            apply_runtime_index_and_equality_batch(
                runtime_indexes,
                wal.cache_scope_id(),
                &stream_id,
                latest_tx_id,
                first_row_id,
                kind,
                &derived_indexes,
                &decoded_row_maps,
            );

        }
        
    }

    let index_apply_us = index_apply_start.elapsed().as_micros() as u64;
    let total_us = batch_start.elapsed().as_micros() as u64;

    if apply_row_index_mutation && index_apply_us >= 5_000 {
        let mut index_stats_before = Vec::with_capacity(derived_indexes.len());
        let mut index_stats_after = Vec::with_capacity(derived_indexes.len());
        let mut index_stats_delta = Vec::with_capacity(derived_indexes.len());
        let mut reset_candidates = Vec::new();

        let before_stats = pre_index_stats
            .as_ref()
            .expect("pre index stats should be present when runtime indexes are tracked");

        for (position, index) in derived_indexes.iter().enumerate() {

            let index_id = &index.index_id.0;
            let before = before_stats.get(position).copied().flatten();
            let after = runtime_indexes
                .stats_for_table(&stream_id, index_id)
                .map(Some)
                .unwrap_or(None);

            let before_display = before
                .map(|(cardinality, capacity)| format!("{cardinality}/{capacity}"))
                .unwrap_or_else(|| "missing".to_string());

            let after_display = after
                .map(|(cardinality, capacity)| format!("{cardinality}/{capacity}"))
                .unwrap_or_else(|| "missing".to_string());

            index_stats_before.push(format!("{}:{}", index_id, before_display));
            index_stats_after.push(format!("{}:{}", index_id, after_display));

            let delta_display = match (before, after) {

                (Some((before_cardinality, before_capacity)), Some((after_cardinality, after_capacity))) => {
                    let cardinality_drop = before_cardinality.saturating_sub(after_cardinality);
                    let capacity_drop = before_capacity.saturating_sub(after_capacity);
                    let steep_cardinality_drop = before_cardinality >= 100_000
                        && (after_cardinality <= 2_048 || cardinality_drop * 10 >= before_cardinality * 9);
                    let steep_capacity_drop = before_capacity >= 100_000
                        && (after_capacity <= 16_384 || capacity_drop * 10 >= before_capacity * 9);

                    if steep_cardinality_drop && steep_capacity_drop {
                        reset_candidates.push(index_id.clone());
                    }

                    format!(
                        "{}:{:+}/{:+}",
                        index_id,
                        after_cardinality as i64 - before_cardinality as i64,
                        after_capacity as i64 - before_capacity as i64,
                    )
                },

                (None, Some(_)) => format!("{}:+new", index_id),

                (Some(_), None) => format!("{}:-missing", index_id),

                (None, None) => format!("{}:0/0", index_id),

            };

            index_stats_delta.push(delta_display);

        }

        let reset_hint = !reset_candidates.is_empty();
        let reset_candidates_display = if reset_hint {
            reset_candidates.join(",")
        } else {
            "none".to_string()
        };

        log::warn!(
            "batch mutation index spike table={} stream={} kind={:?} rows={} index_apply_us={} reset_hint={} reset_candidates={} before=[{}] after=[{}] delta=[{}]",
            table.table_id,
            stream_id,
            kind,
            payload_count,
            index_apply_us,
            reset_hint,
            reset_candidates_display,
            index_stats_before.join(", "),
            index_stats_after.join(", "),
            index_stats_delta.join(", "),
        );

    }

    log::debug!(
        "batch mutation timing table={} kind={:?} rows={} track_runtime_indexes={} materialize_us={} wal_append_us={} index_apply_us={} total_us={}",
        table.table_id,
        kind,
        payload_count,
        track_runtime_indexes,
        materialize_us,
        wal_append_us,
        index_apply_us,
        total_us,
    );

    Ok(())
    
}

pub(super) fn with_statement_write_batch<F>(
    request_id: &str,
    statement_sql: &str,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    execute: F,
) -> ConnectorResponse
where
    F: FnOnce(Option<TransactionId>, &mut RuntimeIndexStore) -> ConnectorResponse,
{

    let sql_summary = summarize_sql_for_error_log(statement_sql);

    let table_stream_id = table_stream_id(catalog, &table.table_id);

    if let Some(write_group_id) = external_write_group_id {
        if let Some(touched_tables) = touched_tables {
            touched_tables.insert(table_stream_id.clone());
        }

        return execute(Some(write_group_id), runtime_indexes);
    }

    let response = execute(None, runtime_indexes);

    if matches!(response.status, connector::ResponseStatus::Applied) {
        if let Err(err) = runtime_indexes.persist_table_snapshot_on_commit(table, &table_stream_id, wal) {
            log::warn!(
                "runtime index incremental persistence skipped table={} stream={} reason={}",
                table.table_id,
                table_stream_id,
                err,
            );
        }

        response

    } else {

        let should_rebuild =
            should_rebuild_runtime_indexes_after_statement_rejection(&response);

        log::warn!(
            "runtime index rebuild triggered table={} request_id={} reason=statement_response_rejected error={} sql=\"{}\"",
            table.table_id,
            request_id,
            response_error_message(&response),
            sql_summary,
        );

        if should_rebuild {
            rebuild_runtime_indexes_for_table(catalog, table, wal, runtime_indexes);
        } else {
            log::debug!(
                "runtime index rebuild skipped table={} request_id={} reason=statement_rejected_without_wal_append_failure",
                table.table_id,
                request_id,
            );
        }

        response
    
    }

}

pub(crate) fn commit_external_write_group(
    wal: &ConcurrentWalManager,
    catalogs: Option<&HashMap<String, DatabaseCatalog>>,
    mut runtime_indexes: Option<&mut RuntimeIndexStore>,
    table_ids: &HashSet<String>,
    group_id: TransactionId,
) -> Result<(), String> {

    for table_id in table_ids {

        let expected_count = grouped_stream_expected_record_count(wal, table_id, group_id);

        append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteCommit,
            encode_write_group_terminal_payload(expected_count),
            common::epoch_nanos!(),
            Some(group_id),
        )?;

        if let (Some(catalogs), Some(runtime_indexes)) = (catalogs, runtime_indexes.as_deref_mut())
            && let Some((_, table)) = catalogs
                .values()
                .find_map(|catalog| {
                    catalog
                        .table_ids()
                        .into_iter()
                        .find(|candidate| {
                            catalog
                                .entity_wal_stream_id(candidate)
                                .as_deref()
                                .is_some_and(|stream_id| stream_id == table_id)
                        })
                        .and_then(|candidate| catalog.table(&candidate).map(|table| (catalog, table)))
                })
            && let Err(err) = runtime_indexes.persist_table_snapshot_on_commit(&table, table_id, wal)
        {
            log::warn!(
                "runtime index incremental persistence skipped table={} reason={}",
                table_id,
                err,
            );
        }

    }

    Ok(())

}

pub(crate) fn abort_external_write_group(
    wal: &ConcurrentWalManager,
    catalogs: &HashMap<String, DatabaseCatalog>,
    runtime_indexes: &mut RuntimeIndexStore,
    table_ids: &HashSet<String>,
    group_id: TransactionId,
) {

    for table_id in table_ids {

        let expected_count = grouped_stream_expected_record_count(wal, table_id, group_id);

        let _ = append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteAbort,
            encode_write_group_terminal_payload(expected_count),
            common::epoch_nanos!(),
            Some(group_id),
        );

        log::warn!(
            "runtime index rebuild triggered table={} reason=external_write_group_abort",
            table_id
        );

        if let Some((catalog, table)) = catalogs
            .values()
            .find_map(|catalog| {
                catalog
                    .table_ids()
                    .into_iter()
                    .find(|candidate| {
                        catalog
                            .entity_wal_stream_id(candidate)
                            .as_deref()
                            .is_some_and(|stream_id| stream_id == table_id)
                    })
                    .and_then(|candidate| catalog.table(&candidate).map(|table| (catalog, table)))
            }) {
            rebuild_runtime_indexes_for_table(catalog, &table, wal, runtime_indexes);
        }

    }

}

pub(super) fn rebuild_runtime_indexes_for_table(
    catalog: &DatabaseCatalog,
    table: &serverlib::DatabaseTable,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
) {
    
    let payload_context = payload_context_for_table(catalog, &table.table_id);
    let stream_id = table_stream_id(catalog, &table.table_id);
    
    let live_rows = match load_live_rows_with_context(wal, &stream_id, table.schema(), &payload_context) {

        Ok(rows) => rows,

        Err(err) => {
            log::warn!(
                "runtime index rebuild skipped table={} reason=payload_context_resolution_failed error={}",
                table.table_id,
                err
            );
            return;
        }

    };

    for index in derived_indexes_for_table(table) {
        if !runtime_indexes.should_track_index(index) {
            continue;
        }

        runtime_indexes.index_mut_for_table(&stream_id, &index.index_id.0).rebuild(
            live_rows
                .iter()
                .map(|(_, row_map)| index_value_tuple(index, row_map))
                .collect(),
        );

    }

}

pub(super) fn table_stream_id(catalog: &DatabaseCatalog, table_id: &str) -> String {
    catalog
        .entity_wal_stream_id(table_id)
        .unwrap_or_else(|| common::normalize_identifier!(table_id))
}

pub(super) fn payload_context_for_table(
    catalog: &DatabaseCatalog,
    table_id: &str,
) -> serverlib::TransactionPayloadContext {

    let mut context = serverlib::TransactionPayloadContext::new()
        .with_database_id(catalog.database_id.0.clone())
        .with_table_id(table_id.to_string());

    if let Some(key_ref) = catalog.at_rest_encryption_key_ref() {
        context = context.with_at_rest_encryption(
            key_ref.to_string(),
            catalog.at_rest_encryption_key_version(),
        );
    }

    context

}

#[cfg(test)]
mod tests {
    use super::summarize_sql_for_error_log;

    #[test]
    fn summarize_sql_for_error_log_truncates_ascii_with_ellipsis() {
        let sql = format!("insert into t values ('{}')", "a".repeat(300));
        let summary = summarize_sql_for_error_log(&sql);
        assert!(summary.ends_with("..."));
        assert_eq!(summary.chars().count(), 243);
    }

    #[test]
    fn summarize_sql_for_error_log_truncates_multibyte_without_panicking() {
        let sql = format!("insert into t values ('{}')", "ē".repeat(300));
        let summary = summarize_sql_for_error_log(&sql);
        assert!(summary.ends_with("..."));
        assert_eq!(summary.chars().count(), 243);
    }
}
