#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectStatus {
    Load,
    Sync,
    Ready,
    Lock,
}

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
