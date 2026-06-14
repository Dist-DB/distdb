use serverlib::engine::database::transaction::{TransactionLog, TransactionRecord};
use serverlib::{ConcurrentWalManager, TransactionId, TransactionKind, UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalProbeResult {
    pub active_workers: usize,
    pub records_in_primary_table: usize,
}

pub fn run_wal_probe(wal: &ConcurrentWalManager) -> Result<WalProbeResult, &'static str> {
    let orders_wal_id = "orders";
    let inventory_wal_id = "inventory";

    let actor = UserId::from_username("probe-user");

    wal.append(
        orders_wal_id,
        TransactionRecord {
            id: TransactionId(1),
            groupid: None,
            refid: None,
            timestamp_epoch_ms: 1,
            actor: actor.clone(),
            kind: TransactionKind::Insert,
            payload: vec![1, 2, 3],
        },
    )?;

    wal.append(
        orders_wal_id,
        TransactionRecord {
            id: TransactionId(2),
            groupid: None,
            refid: None,
            timestamp_epoch_ms: 2,
            actor: actor.clone(),
            kind: TransactionKind::Update,
            payload: vec![4, 5, 6],
        },
    )?;

    wal.append(
        inventory_wal_id,
        TransactionRecord {
            id: TransactionId(1),
            groupid: None,
            refid: None,
            timestamp_epoch_ms: 3,
            actor,
            kind: TransactionKind::Insert,
            payload: vec![7],
        },
    )?;

    let replay = wal.since(orders_wal_id, Some(TransactionId(1)));

    Ok(WalProbeResult {
        active_workers: wal.active_worker_count(),
        records_in_primary_table: replay.len(),
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn wal_replay_without_lower_bound_returns_all_records() {
        let wal = ConcurrentWalManager::new();
        run_wal_probe(&wal).expect("probe should append records");

        let all_orders = wal.since("orders", None);
        assert_eq!(all_orders.len(), 2);
    }

    #[test]
    fn wal_replay_with_lower_bound_is_exclusive() {
        let wal = ConcurrentWalManager::new();
        run_wal_probe(&wal).expect("probe should append records");

        let replay = wal.since("orders", Some(TransactionId(1)));
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].id, TransactionId(2));
    }

    #[test]
    fn wal_accepts_out_of_order_append_and_keeps_sorted_order() {
        let wal = ConcurrentWalManager::new();
        let actor = UserId::from_username("integrity-user");

        wal.append(
            "events",
            TransactionRecord {
                id: TransactionId(4),
                groupid: None,
                refid: None,
                timestamp_epoch_ms: 1,
                actor: actor.clone(),
                kind: TransactionKind::Insert,
                payload: vec![],
            },
        )
        .expect("first append should succeed");

        let out_of_order = wal.append(
            "events",
            TransactionRecord {
                id: TransactionId(3),
                groupid: None,
                refid: None,
                timestamp_epoch_ms: 2,
                actor,
                kind: TransactionKind::Update,
                payload: vec![],
            },
        );

        assert!(out_of_order.is_ok());

        let replay = wal.since("events", None);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].id, TransactionId(3));
        assert_eq!(replay[1].id, TransactionId(4));
    }
}
