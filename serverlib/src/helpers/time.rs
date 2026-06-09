use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now_epoch_ms() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0));
    duration.as_millis() as u64
}