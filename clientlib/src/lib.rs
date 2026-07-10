use std::sync::{Arc, Mutex};

mod config;
mod error;
mod models;
mod runtime;

pub use error::ClientError;
pub use models::{
    ClientOptions, ConnectionInfo, ExecuteResponse, QueryColumnDef, QueryResponse, QueryRow,
    QueryTimings, QueryValue, TlsMode,
};

#[derive(Debug, Clone)]
pub struct DistDbClient {
    inner: Arc<Mutex<runtime::ClientInner>>,
}
