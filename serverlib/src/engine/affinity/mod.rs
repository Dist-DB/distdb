
pub mod storage;

mod checkpoint;
mod document;
mod processor;
mod state;
mod sync;

pub use checkpoint::CheckpointMetadata;
pub use document::{
    AffinityDocument, AffinityMember, AffinityMemberStatus, DatabaseSchemaSummary,
    ReplicationSecuritySummary,
};

pub use processor::AffinityProcessor;
pub use state::{AffinityProcessorError, AffinityProcessorState};
pub use sync::{AffinitySyncPhase, AffinitySyncStep};


#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;