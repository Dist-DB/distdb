use crate::error::Result;
use crate::p2p::protocol::ServiceMessage;

pub trait Transport {

    fn send(&mut self, peer_id: &str, message: ServiceMessage) -> Result<()>;

    fn broadcast(&mut self, message: ServiceMessage) -> Result<()>;

}