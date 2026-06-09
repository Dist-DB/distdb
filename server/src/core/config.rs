use std::path::PathBuf;

use serverlib::NodeConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    pub node_id: String,
    pub swarm_id: String,
    pub data_dir: PathBuf,
    pub listen_addrs: Vec<String>,
}

impl ServerRuntimeConfig {

    pub fn default_local() -> Self {
        
        Self {
            node_id: "server-node-local-01".to_string(),
            swarm_id: "distdb-devnet".to_string(),
            data_dir: PathBuf::from("./data"),
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
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