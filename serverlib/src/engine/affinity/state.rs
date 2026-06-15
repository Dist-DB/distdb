use super::AffinitySyncPhase;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AffinityProcessorState {
    Unconfigured,
    JoinRequested,
    Syncing(AffinitySyncPhase),
    Ready,
    Degraded(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AffinityProcessorError {
    MissingAffinityDocument,
    SchemaValidationPartnerRequired,
}

impl std::fmt::Display for AffinityProcessorError {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        match self {
            Self::MissingAffinityDocument => {
                write!(f, "missing affinity document for replication planning")
            }
            Self::SchemaValidationPartnerRequired => write!(
                f,
                "schema change requires at least one reachable partner in same affinity"
            ),
        }
    
    }

}

impl std::error::Error for AffinityProcessorError {}