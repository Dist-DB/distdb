use std::path::PathBuf;

use common::DEFAULT_SERVER_PORT;
use serverlib::NodeConfig;

const DEFAULT_LOCAL_NODE_ID: &str = "server-node-01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    pub node_id: String,
    pub swarm_id: String,
    pub data_dir: PathBuf,
    pub listen_addrs: Vec<String>,
}

impl ServerRuntimeConfig {
    pub fn default_local() -> Self {
        Self::default_local_with_data_dir(PathBuf::from("./data"))
    }

    pub fn default_local_with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            node_id: DEFAULT_LOCAL_NODE_ID.to_string(),
            swarm_id: "distdb-devnet".to_string(),
            data_dir,
            listen_addrs: vec![format!("/ip4/0.0.0.0/tcp/{DEFAULT_SERVER_PORT}")],
        }
    }

    pub fn default_local_with_listen_addr(
        data_dir: PathBuf,
        listen_addr: impl Into<String>,
    ) -> Self {
        Self {
            node_id: DEFAULT_LOCAL_NODE_ID.to_string(),
            swarm_id: "distdb-devnet".to_string(),
            data_dir,
            listen_addrs: vec![listen_addr.into()],
        }
    }

    pub fn to_node_config(&self) -> NodeConfig {
        NodeConfig {
            node_id: self.node_id.clone(),
            swarm_id: self.swarm_id.clone(),
            data_dir: self.data_dir.clone(),
            listen_addrs: self.listen_addrs.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_local_listens_on_all_interfaces() {
        let config = ServerRuntimeConfig::default_local();

        assert_eq!(config.node_id, DEFAULT_LOCAL_NODE_ID);
        assert_eq!(
            config.listen_addrs,
            vec![format!("/ip4/0.0.0.0/tcp/{DEFAULT_SERVER_PORT}")]
        );
    }
}
