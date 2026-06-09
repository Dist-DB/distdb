#![allow(dead_code)]

pub mod core;
pub mod helpers;
pub mod engine;

use crate::core::app::ServerApp;
use crate::core::config::ServerRuntimeConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let data_dir = std::env::args()
        .find_map(|arg| arg.strip_prefix("datadir=").map(std::path::PathBuf::from))
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));

    let listen_addr = std::env::args()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    log::info!("using data directory: {}", data_dir.display());
    log::info!("using listen address host: {}", listen_addr);

    let config = ServerRuntimeConfig::default_local_with_listen_addr(
        data_dir,
        format!("/ip4/{listen_addr}/tcp/{}", common::DEFAULT_SERVER_PORT),
    );

    let mut app = ServerApp::new(config)?;
    app.bootstrap()?;

    let result = app.run_wal_smoke_test()?;
    
    log::info!(
        "server runtime initialized for node={} with {} active WAL worker(s) and {} probe records",
        app.node_id(),
        result.active_workers,
        result.records_in_primary_table
    );

    app.shutdown()?;
    Ok(())
}
