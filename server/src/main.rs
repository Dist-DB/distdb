#![allow(dead_code)]

pub mod core;
pub mod helpers;
pub mod engine;

use crate::core::app::ServerApp;
use crate::core::config::ServerRuntimeConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let config = ServerRuntimeConfig::default_local();
    
    let mut app = ServerApp::new(config)?;
    app.bootstrap()?;

    let result = app.run_wal_smoke_test()?;

    println!(
        "server runtime initialized for node={} with {} active WAL worker(s) and {} probe records",
        app.node_id(),
        result.active_workers,
        result.records_in_primary_table
    );

    app.shutdown()?;
    Ok(())

}
