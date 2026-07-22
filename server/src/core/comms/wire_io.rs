use connector::ConnectorResponse;
use peerlib::ServiceMessage;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::comms::p2p_wire::encode_service_message;

pub async fn write_response_frame(
    stream: &mut (impl AsyncWrite + Unpin + ?Sized),
    response: ConnectorResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = bincode::serialize(&response)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn write_service_message_to_stream(
    stream: &mut (impl AsyncWrite + Unpin + ?Sized),
    message: &ServiceMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = encode_service_message(message).ok_or("failed to encode service message")?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.flush().await?;
    Ok(())
}
