use super::negotiate_connector_stream;

use tokio::net::TcpListener;

#[tokio::test]
async fn required_tls_without_acceptor_fails_and_does_not_fallback() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener addr");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept should work");
        negotiate_connector_stream(
            stream,
            &addr.to_string(),
            common::TlsMode::Required,
            None,
        )
        .await
    });

    let _client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");

    let result = server.await.expect("server task should complete");
    assert!(result.is_err(), "required TLS must fail without acceptor");
}
