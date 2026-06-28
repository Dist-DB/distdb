use super::*;

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

pub(in super::super) fn append_row_payload_record(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    append_row_payload_record_with_live_row_ids(
        catalog,
        wal,
        wal_id,
        table,
        runtime_indexes,
        kind,
        payload,
        timestamp_epoch_ms,
        refid,
        None,
        group_id,
    )
}

pub(super) fn append_row_payload_record_with_live_row_ids(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    expected_live_row_ids: Option<&HashSet<u64>>,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    let mutation_start = Instant::now();
    let payload_context = payload_context_for_table(catalog, wal_id);

    if let Some(expected_refid) = refid {
        let live_row_check_start = Instant::now();
        let live_row_exists = if let Some(live_row_ids) = expected_live_row_ids {
            live_row_ids.contains(&expected_refid.0)
        } else {
            let live_row_ids = load_live_rows_with_context(wal, wal_id, table.schema(), &payload_context)
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

    let last_id = wal.latest_transaction_id(wal_id);
    let next_id = TransactionId(last_id.map(|id| id.0 + 1).unwrap_or(1));
    let refid = refid.or(last_id);

    let row_decode_start = Instant::now();
    let derived_indexes = derived_indexes_for_table(table).collect::<Vec<_>>();
    let track_runtime_indexes = !derived_indexes.is_empty();

    let row_map = if track_runtime_indexes {
        Some(
            decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?,
        )
    } else {
        None
    };
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
    wal.append(wal_id, record).map_err(|e| e.to_string())?;
    let wal_append_ms = wal_append_start.elapsed().as_millis() as u64;

    let index_apply_start = Instant::now();

    if let Some(row_map) = row_map.as_ref() {
        runtime_indexes.apply_table_row_mutation(derived_indexes.iter().copied(), kind, row_map);
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

pub(super) fn append_row_payload_records_batch(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payloads: Vec<Vec<u8>>,
    prepared_row_maps: Option<Vec<HashMap<String, Vec<u8>>>>,
    timestamp_epoch_ms: u64,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    
    if payloads.is_empty() {
        return Ok(());
    }

    let batch_start = Instant::now();
    let derived_indexes = derived_indexes_for_table(table).collect::<Vec<_>>();
    let track_runtime_indexes = !derived_indexes.is_empty();
    let payload_count = payloads.len();

    let last_id = wal.latest_transaction_id(wal_id);
    let mut next_id = last_id.map(|id| id.0.saturating_add(1)).unwrap_or(1);
    let mut refid = last_id;
    let actor = UserId::from_username("server");

    let mut records = Vec::with_capacity(payloads.len());
    let mut row_maps = prepared_row_maps.unwrap_or_default();
    let use_prepared_row_maps = track_runtime_indexes && row_maps.len() == payload_count;

    if track_runtime_indexes && !use_prepared_row_maps {
        row_maps = Vec::with_capacity(payload_count);
    }

    let materialize_start = Instant::now();

    for payload in payloads {
        if track_runtime_indexes && !use_prepared_row_maps {
            let row_map = decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?;
            row_maps.push(row_map);
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
    wal.append_batch(wal_id, records)
        .map_err(|err| err.to_string())?;
    let wal_append_us = wal_append_start.elapsed().as_micros() as u64;

    let index_apply_start = Instant::now();

    if track_runtime_indexes {
        match kind {
            TransactionKind::Delete => {
                runtime_indexes.remove_table_rows_batch(&derived_indexes, &row_maps);
            }

            TransactionKind::Insert | TransactionKind::Update => {
                runtime_indexes.record_table_rows_batch(&derived_indexes, &row_maps);
            }

            _ => {}
        }
    }

    let index_apply_us = index_apply_start.elapsed().as_micros() as u64;
    let total_us = batch_start.elapsed().as_micros() as u64;

    if track_runtime_indexes && index_apply_us >= 5_000 {
        let mut index_stats = Vec::with_capacity(derived_indexes.len());

        for index in &derived_indexes {
            let index_id = &index.index_id.0;
            let stats = runtime_indexes
                .stats(index_id)
                .map(|(cardinality, capacity)| format!("{}:{}/{}", index_id, cardinality, capacity))
                .unwrap_or_else(|| format!("{}:missing", index_id));
            index_stats.push(stats);
        }

        log::warn!(
            "batch mutation index spike table={} kind={:?} rows={} index_apply_us={} index_stats=[{}]",
            table.table_id,
            kind,
            payload_count,
            index_apply_us,
            index_stats.join(", "),
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
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    execute: F,
) -> ConnectorResponse
where
    F: FnOnce(TransactionId, &mut RuntimeIndexStore) -> ConnectorResponse,
{

    if let (Some(write_group_id), Some(touched_tables)) = (external_write_group_id, touched_tables)
    {
        if touched_tables.insert(table.table_id.clone())
            && let Err(err) = append_payload_record_with_group(
                wal,
                &table.table_id,
                TransactionKind::WriteBegin,
                request_id.as_bytes().to_vec(),
                common::epoch_nanos!(),
                Some(write_group_id),
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("transaction write begin failed: {err}"),
                );
            }

        return execute(write_group_id, runtime_indexes);
    }

    let write_group_id = match append_payload_record_with_group(
        wal,
        &table.table_id,
        TransactionKind::WriteBegin,
        request_id.as_bytes().to_vec(),
        common::epoch_nanos!(),
        None,
    ) {
        Ok(group_id) => group_id,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("statement write begin failed: {err}"),
            )
        }
    };

    let response = execute(write_group_id, runtime_indexes);

    if matches!(response.status, connector::ResponseStatus::Applied) {

        if let Err(err) = append_payload_record_with_group(
            wal,
            &table.table_id,
            TransactionKind::WriteCommit,
            Vec::new(),
            common::epoch_nanos!(),
            Some(write_group_id),
        ) {
            log::warn!(
                "runtime index rebuild triggered table={} reason=write_commit_failed error={}",
                table.table_id,
                err
            );
            let _ = append_payload_record_with_group(
                wal,
                &table.table_id,
                TransactionKind::WriteAbort,
                Vec::new(),
                common::epoch_nanos!(),
                Some(write_group_id),
            );
            rebuild_runtime_indexes_for_table(catalog, table, wal, runtime_indexes);
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("statement write commit failed: {err}"),
            );
        }

        response

    } else {

        log::warn!(
            "runtime index rebuild triggered table={} reason=statement_response_rejected",
            table.table_id
        );
        
        let _ = append_payload_record_with_group(
            wal,
            &table.table_id,
            TransactionKind::WriteAbort,
            Vec::new(),
            common::epoch_nanos!(),
            Some(write_group_id),
        );

        rebuild_runtime_indexes_for_table(catalog, table, wal, runtime_indexes);
        response
    
    }

}

pub(crate) fn commit_external_write_group(
    wal: &ConcurrentWalManager,
    table_ids: &HashSet<String>,
    group_id: TransactionId,
) -> Result<(), String> {

    for table_id in table_ids {
        append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteCommit,
            Vec::new(),
            common::epoch_nanos!(),
            Some(group_id),
        )?;
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

        let _ = append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteAbort,
            Vec::new(),
            common::epoch_nanos!(),
            Some(group_id),
        );

        log::warn!(
            "runtime index rebuild triggered table={} reason=external_write_group_abort",
            table_id
        );

        if let Some((catalog, table)) = catalogs
            .values()
            .find_map(|catalog| catalog.table(table_id).map(|table| (catalog, table))) {
            rebuild_runtime_indexes_for_table(catalog, table, wal, runtime_indexes);
        }

    }
}

fn rebuild_runtime_indexes_for_table(
    catalog: &DatabaseCatalog,
    table: &serverlib::DatabaseTable,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
) {
    let payload_context = payload_context_for_table(catalog, &table.table_id);
    let live_rows = match load_live_rows_with_context(wal, &table.table_id, table.schema(), &payload_context) {
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

        runtime_indexes.index_mut(&index.index_id.0).rebuild(
            live_rows
                .iter()
                .map(|(_, row_map)| index_value_tuple(index, row_map))
                .collect(),
        );

    }
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
