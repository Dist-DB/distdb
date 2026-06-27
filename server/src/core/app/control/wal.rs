use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    ConcurrentWalManager, TransactionId, TransactionKind, TransactionRecord,
    UserId,
};

use crate::core::app::ServerApp;
use crate::core::app::helpers::SessionTxMarkerType;

impl ServerApp {
    pub(super) fn seed_sandbox_wal(&self, sandbox_wal: &ConcurrentWalManager) -> Result<(), String> {
        for catalog in self.catalogs.values() {
            for table_id in catalog.table_ids() {
                let Some(table) = catalog.table(&table_id) else {
                    continue;
                };

                self.seed_table_stream_from_live_rows(
                    &table.table_id,
                    table.schema(),
                    sandbox_wal,
                )?;
            }
        }

        Ok(())
    }

    fn seed_table_stream_from_live_rows(
        &self,
        table_id: &str,
        schema: &serverlib::TableSchema,
        sandbox_wal: &ConcurrentWalManager,
    ) -> Result<(), String> {
        if !self.wal.is_stream_replicable(table_id) {
            return Ok(());
        }

        let live_rows = serverlib::load_live_rows(&self.wal, table_id, schema);
        if live_rows.is_empty() {
            return Ok(());
        }

        let timestamp_epoch_ms = common::epoch_nanos!();
        let mut records = Vec::with_capacity(live_rows.len());

        for (idx, (_, row_map)) in live_rows.into_iter().enumerate() {
            let payload = serverlib::encode_row_payload(schema, &row_map)
                .map_err(|err| format!("failed to encode snapshot row for table '{}': {}", table_id, err))?;

            records.push(TransactionRecord {
                id: TransactionId((idx as u64) + 1),
                groupid: None,
                refid: None,
                timestamp_epoch_ms,
                actor: UserId::from_username("snapshot"),
                kind: TransactionKind::Insert,
                payload,
            });
        }

        sandbox_wal
            .append_batch(table_id, records)
            .map_err(|err| format!("failed to seed snapshot live rows for stream '{}': {}", table_id, err))
    }

    fn copy_wal_stream(&self, wal_id: &str, sandbox_wal: &ConcurrentWalManager) -> Result<(), String> {
        if !self.wal.is_stream_replicable(wal_id) {
            return Ok(());
        }

        let records = self.wal.since(wal_id, None);
        sandbox_wal
            .append_batch(wal_id, records)
            .map_err(|err| format!("failed to seed sandbox WAL for stream '{}': {}", wal_id, err))?;

        Ok(())
    }

    pub fn export_wal_records_for_database(
        &self,
        database_id: &str,
        from: Option<TransactionId>,
        from_stream_transaction_ids: Option<&std::collections::HashMap<String, TransactionId>>,
    ) -> Result<Vec<(String, TransactionRecord)>, String> {
        let database_key = self
            .resolve_catalog_key(database_id)
            .ok_or_else(|| format!("database '{}' not found", database_id))?;

        let Some(catalog) = self.catalogs.get(&database_key) else {
            return Err(format!("database '{}' not found", database_id));
        };

        let mut stream_ids: Vec<String> = vec![database_key];
        stream_ids.extend(catalog.table_ids());

        let mut frames = Vec::new();
        for stream_id in stream_ids {
            if !self.wal.is_stream_replicable(&stream_id) {
                continue;
            }

            let stream_from = match from_stream_transaction_ids {
                Some(map) => map.get(&stream_id).copied(),
                None => from,
            };
            let mut records = self.wal.since(&stream_id, stream_from);
            records.sort_by_key(|record| record.id.0);
            for record in records {
                frames.push((stream_id.clone(), record));
            }
        }

        frames.sort_by(|a, b| {
            a.1.timestamp_epoch_ms
                .cmp(&b.1.timestamp_epoch_ms)
                .then_with(|| a.1.id.0.cmp(&b.1.id.0))
        });

        Ok(frames)
    }

    pub fn import_wal_records(
        &mut self,
        database_id: &str,
        records: Vec<(String, TransactionRecord)>,
    ) -> Result<(), String> {
        self.begin_affinity_sync_lock(database_id)?;

        let import_result = (|| {
            let mut appended_any = false;

            for (stream_id, record) in records {

                if self
                    .wal
                    .since(&stream_id, None).contains(&record)
                {
                    continue;
                }

                match self.wal.append(&stream_id, record) {

                    Ok(()) => {
                        appended_any = true;
                    },

                    Err(err) if err.contains("out-of-order") => {
                        // Duplicate or older record already present locally; skip.
                        continue;
                    },

                    Err(err) => {
                        return Err(format!(
                            "failed importing WAL record stream='{}': {}",
                            stream_id,
                            err
                        ));
                    }
                    
                }

            }

            if !appended_any {
                return Ok(());
            }

            // Rebuild in-memory structures from newly imported WAL records.
            self.replay_catalog_state_from_wal()
                .map_err(|err| format!("failed replaying imported WAL records: {}", err))?;
            self.runtime_indexes
                .bootstrap_from_catalogs(&self.catalogs, &self.wal);

            Ok(())
        })();

        let release_result = self.finish_affinity_sync_lock(database_id);

        match (import_result, release_result) {
            (Err(import_err), Err(release_err)) => {
                Err(format!("{}; cleanup failed: {}", import_err, release_err))
            }
            (Err(import_err), Ok(())) => Err(import_err),
            (Ok(()), Err(release_err)) => Err(release_err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn rollback_session_transaction(&mut self, session_id: &str) -> bool {

        let rolled_back = self
            .transaction_coordinator
            .rollback(session_id)
            .unwrap_or(false);

        if rolled_back {
            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                "__disconnect__",
                SessionTxMarkerType::DisconnectRollback,
                0,
            ) {
                log::warn!("failed to append disconnect rollback marker: {}", err);
            }
        }

        rolled_back

    }

    pub(super) fn append_session_tx_marker(
        &self,
        session_id: &str,
        request_id: &str,
        marker_type: SessionTxMarkerType,
        staged_count: usize,
    ) -> Result<(), String> {

        log::debug!(
            "session tx marker session_id={} request_id={} marker={} staged_count={}",
            session_id,
            request_id,
            marker_type.as_str(),
            staged_count
        );

        Ok(())
    }
}
