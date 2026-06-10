#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTimingThresholds {
    pub stress_iterations: usize,
    pub p95_max_ms: u128,
    pub p99_max_ms: u128,
    pub batch_max_ms: u128,
}

impl Default for QueryTimingThresholds {
    fn default() -> Self {
        Self {
            stress_iterations: 1000,
            p95_max_ms: 50,
            p99_max_ms: 150,
            batch_max_ms: 15_000,
        }
    }
}

impl QueryTimingThresholds {
    pub fn from_env() -> Self {
        let defaults = Self::default();

        Self {
            stress_iterations: env_usize(
                "DISTDB_QUERY_STRESS_ITERATIONS",
                defaults.stress_iterations,
            ),
            p95_max_ms: env_u128("DISTDB_QUERY_P95_MAX_MS", defaults.p95_max_ms),
            p99_max_ms: env_u128("DISTDB_QUERY_P99_MAX_MS", defaults.p99_max_ms),
            batch_max_ms: env_u128("DISTDB_QUERY_BATCH_MAX_MS", defaults.batch_max_ms),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u128(name: &str, default: u128) -> u128 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(default)
}
