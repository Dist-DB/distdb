
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub node_id: String,
    pub swarm_id: String,
    pub data_dir: PathBuf,
    pub listen_addrs: Vec<String>,
}

impl NodeConfig {
    
    pub fn validate(&self) -> Result<(), &'static str> {

        if self.node_id.trim().is_empty() {
            return Err("node_id must not be empty");
        }
        
        if self.swarm_id.trim().is_empty() {
            return Err("swarm_id must not be empty");
        }
        
        if self.listen_addrs.is_empty() {
            return Err("at least one listen address is required");
        }

        Ok(())
    }

}