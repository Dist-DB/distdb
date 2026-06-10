use server::core::app::ServerApp;
use server::core::config::ServerRuntimeConfig;

use connector::{ConnectorRequest, ConnectorResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

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

    let tcp_bind_addr = format!("{}:{}", listen_addr, common::DEFAULT_SERVER_PORT);
    let listener = TcpListener::bind(&tcp_bind_addr).await?;
    log::info!("connector request listener bound at {}", tcp_bind_addr);

    let app = Arc::new(Mutex::new(app));
    let app_for_listener = Arc::clone(&app);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_connections_for_listener = Arc::clone(&active_connections);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let connection_id =
                        active_connections_for_listener.fetch_add(1, Ordering::SeqCst) + 1;
                    log::info!(
                        "connector peer connected from {} (active_connections={})",
                        peer_addr,
                        connection_id
                    );
                    let app = Arc::clone(&app_for_listener);
                    let active_connections = Arc::clone(&active_connections_for_listener);
                    tokio::spawn(async move {
                        if let Err(err) =
                            handle_connector_stream(stream, app, peer_addr.to_string()).await
                        {
                            log::warn!(
                                "connector stream handling failed for {}: {}",
                                peer_addr,
                                err
                            );
                        }
                        let remaining = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                        log::info!(
                            "connector peer disconnected from {} (active_connections={})",
                            peer_addr,
                            remaining
                        );
                    });
                }
                Err(err) => {
                    log::warn!("listener accept failed: {}", err);
                }
            }
        }
    });

    log::info!("server process is running; press Ctrl+C to shutdown");
    tokio::signal::ctrl_c().await?;
    log::info!("shutdown signal received");

    app.lock().await.shutdown()?;
    Ok(())
}

async fn handle_connector_stream(
    mut stream: TcpStream,
    app: Arc<Mutex<ServerApp>>,
    peer_addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(err) = stream.read_exact(&mut len_buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(());
            }
            return Err(Box::new(err));
        }

        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; frame_len];
        stream.read_exact(&mut payload).await?;

        let request = bincode::deserialize::<ConnectorRequest>(&payload)?;
        log::debug!(
            "server handling connector request_id={} from {}",
            request.request_id,
            peer_addr
        );

        let response = {
            let mut app = app.lock().await;
            app.handle_connector_request(&request)
        };

        write_response_frame(&mut stream, response).await?;
    }
}

async fn write_response_frame(
    stream: &mut TcpStream,
    response: ConnectorResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = bincode::serialize(&response)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}
