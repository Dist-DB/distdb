
use server::core::app::ServerApp;
use server::core::config::ServerRuntimeConfig;

use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult,
    MutationResult,
};
use common::helpers::{aes_decrypt, aes_encrypt};
use common::helpers::utils::md5_hash;
use common::{PeerSession, SessionLog, SessionLogEventType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";

// we change these later (accessing the database structures...)

const SERVER_TEMP_PASSWORD: &str = "password";
const SERVER_TEMP_USER: &str = "root";
const SERVER_TEMP_TOKEN_SALT: &[u8; 8] = b"distdbv1";


#[derive(Debug)]
struct ServerConnectionSession {
    peer_addr: String,
    challenge_id: String,
    shared_authorization_token: String,
    session: PeerSession,
    log: SessionLog,
    authenticated: bool,
    encrypted_password_md5_token: String,
}

impl ServerConnectionSession {

    fn new(peer_addr: String, connection_id: usize) -> Self {

        let challenge_id = format!("challenge-{}-{connection_id}", now_millis());
        let shared_authorization_token = md5_hash(
            format!("{}:{}:{}", SERVER_TEMP_USER, peer_addr, challenge_id).as_str(),
        );
        let session = PeerSession::new().with_user_id(SERVER_TEMP_USER);
        let expected_md5_token = md5_hash(SERVER_TEMP_PASSWORD);
        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let encrypted_password_md5_token =
            aes_encrypt(&expected_md5_token, &security_secret, SERVER_TEMP_TOKEN_SALT);
        let mut log = SessionLog::new();
        
        log.add_entry(
            SessionLogEventType::Connect,
            format!("connector peer connected from {peer_addr}"),
            true,
        );
        
        log.add_entry(
            SessionLogEventType::Authenticate,
            format!(
                "password challenge issued id={challenge_id} user={}",
                SERVER_TEMP_USER
            ),
            true,
        );

        Self {
            peer_addr,
            challenge_id,
            shared_authorization_token,
            session,
            log,
            authenticated: false,
            encrypted_password_md5_token,
        }

    }

    fn challenge_message(&self) -> String {
        format!(
            "password challenge required challenge_id={} shared_authorization={} peer={}"
            ,
            self.challenge_id,
            self.shared_authorization_token,
            self.peer_addr
        )
    }

    fn record_request(&mut self, request: &ConnectorRequest) {

        let event_type = match &request.command {

            ConnectorCommand::Query { query } => {
                self.session.current_database = Some(query.database_id.clone());
                SessionLogEventType::QueryExecute
            }

            ConnectorCommand::Schema { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::SchemaChange
            }

            ConnectorCommand::Mutation { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::Other
            }

            ConnectorCommand::CreateDatabase { database_name } => {
                self.session.current_database = Some(database_name.clone());
                SessionLogEventType::Other
            }

        };

        self.log.add_entry(
            event_type,
            format!(
                "request_id={} routed by server session db={}",
                request.request_id,
                self.session
                    .current_database
                    .as_deref()
                    .unwrap_or("<none>")
            ),
            true,
        );

    }

    fn mark_disconnect(&mut self) {
        self.log.add_entry(
            SessionLogEventType::Disconnect,
            "connector peer disconnected",
            true,
        );
    }

    fn authenticate_if_valid_token(&mut self, candidate_password_md5_token: &str) -> bool {

        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let expected_password_md5_token = aes_decrypt(&self.encrypted_password_md5_token, &security_secret);

        if candidate_password_md5_token == expected_password_md5_token {

            self.authenticated = true;
            self.session.auth_token = Some(format!("{}-authenticated", SERVER_TEMP_USER));
            
            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password accepted user={} token={}",
                    SERVER_TEMP_USER,
                    candidate_password_md5_token
                ),
                true,
            );

            true

        } else {

            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password rejected user={} token={}",
                    SERVER_TEMP_USER,
                    candidate_password_md5_token
                ),
                false,
            );

            false

        }

    }

}

fn security_context_secret(user_id: &str, database_id: &str) -> String {
    format!("distdb-security:{}:{}", user_id, database_id)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let data_dir = std::env::args()
        .find_map(|arg| arg.strip_prefix("datadir=").map(std::path::PathBuf::from))
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));

    let listen_addr = std::env::args()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let port: u16 = std::env::args()
        .find_map(|arg| {
            arg.strip_prefix("port=")
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(common::DEFAULT_SERVER_PORT);

    log::info!("using data directory: {}", data_dir.display());
    log::info!("using listen address host: {}", listen_addr);
    log::info!("using port: {}", port);

    let config = ServerRuntimeConfig::default_local_with_listen_addr(
        data_dir,
        format!("/ip4/{listen_addr}/tcp/{port}"),
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

    let tcp_bind_addr = format!("{}:{}", listen_addr, port);
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
                            handle_connector_stream(stream, app, peer_addr.to_string(), connection_id).await
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
    connection_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut session = ServerConnectionSession::new(peer_addr.clone(), connection_id);
    
    write_response_frame(
        &mut stream,
        ConnectorResponse::rejected(
            SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
            session.challenge_message(),
        ),
    )
    .await?;

    loop {

        let mut len_buf = [0u8; 4];
        if let Err(err) = stream.read_exact(&mut len_buf).await {
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                session.mark_disconnect();
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

        if !session.authenticated {
            let auth_outcome = match &request.command {
                ConnectorCommand::Query { query } => {
                    extract_auth_token(&query.sql)
                        .map(|token| session.authenticate_if_valid_token(token))
                }
                _ => None,
            };

            let response = match auth_outcome {
                Some(true) => ConnectorResponse::applied(
                    request.request_id,
                    ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                ),
                Some(false) => ConnectorResponse::rejected(
                    request.request_id,
                    "invalid password",
                ),
                None => ConnectorResponse::rejected(
                    request.request_id,
                    "authentication required; run `password <password>;` first",
                ),
            };

            write_response_frame(&mut stream, response).await?;
            continue;
            
        }

        session.record_request(&request);

        let response = {
            let mut app = app.lock().await;
            app.handle_connector_request(&request)
        };

        write_response_frame(&mut stream, response).await?;
    
    }

}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn extract_auth_token(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let token = parts.next()?;
    if command.eq_ignore_ascii_case("password_token") {
        return Some(token);
    }
    None
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
