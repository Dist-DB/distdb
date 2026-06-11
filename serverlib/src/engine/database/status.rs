
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectStatus {
    Load,
    Sync,
    Ready,
    Lock,
}

/*

The state machine for database objects (databases, tables, indexes) is as follows:
    
    - Load: the object is being loaded from disk or created for the first time. During this phase, 
        the object is not fully initialized and cannot be used for queries or mutations

        -> sync (e.g. when loading from disk is complete but pending mutations or schema changes need to be applied)
        -> ready (e.g. when loading from disk is complete and the object is ready to use)
        -> lock (e.g. when creating a new object that requires an exclusive lock during initialization, 
            such as a new database or table)

    - Sync: the object is synchronizing with the latest state, such as applying pending mutations or schema changes. 
        During this phase, the object may be temporarily unavailable for queries or mutations

        -> ready (e.g. when pending mutations or schema changes are applied and the object is synchronized)
        -> lock (e.g. when a long-running mutation or schema change is being applied)

    - Ready: the object is fully initialized and synchronized, and can be used for queries and mutations

        -> sync (e.g. when a new mutation is applied or a schema change is pending)
        -> lock (e.g. when a long-running mutation or schema change is being applied)

    - Lock: the object is locked for exclusive access, such as during a schema change or a long-running mutation. 
        During this phase, the object is not available for queries or mutations, and may transition back to 
        Sync or Ready when the operation is complete, or to Ready directly if the operation is aborted

        -> sync (e.g. when a long-running mutation or schema change is complete and the object needs to synchronize)
        -> ready (e.g. when a long-running mutation or schema change is complete and the object is already 
            synchronized, such as in an abort path)

 */

impl ObjectStatus {

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Load, Self::Sync)
            | (Self::Load, Self::Ready)
            | (Self::Load, Self::Lock)
            | (Self::Sync, Self::Ready)
            | (Self::Sync, Self::Lock)
            | (Self::Ready, Self::Sync)
            | (Self::Ready, Self::Lock)
            | (Self::Lock, Self::Sync)
            | (Self::Lock, Self::Ready)
        )
    }

}

#[cfg(test)]
mod tests {
    
    use super::ObjectStatus;

    #[test]
    fn object_status_lock_to_ready_is_valid_for_abort_path() {
        assert!(ObjectStatus::Lock.can_transition_to(ObjectStatus::Ready));
    }

}
