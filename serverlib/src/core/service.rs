
use crate::core::config::NodeConfig;
use crate::helpers::error::Result;

pub trait NodeService {

    fn boot(&mut self, config: NodeConfig) -> Result<()>;
    fn shutdown(&mut self) -> Result<()>;

}