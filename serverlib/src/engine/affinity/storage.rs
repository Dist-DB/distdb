use crate::engine::affinity::{AffinityDocument, CheckpointMetadata};
use crate::helpers::error::{Result, ServerLibError};
use std::path::{Path, PathBuf};

/// Manages persistence of affinity documents to disk
#[derive(Debug, Clone)]
pub struct AffinityStorage {
    data_dir: PathBuf,
}

impl AffinityStorage {

    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    fn affinity_document_path(&self, affinity_id: &str) -> PathBuf {
        self.data_dir.join(format!(".affinity_{}", affinity_id))
    }

    fn checkpoint_path(&self, affinity_id: &str) -> PathBuf {
        self.data_dir.join(format!(".checkpoint_{}", affinity_id))
    }

    /// Load affinity document from disk if it exists
    pub fn load(&self, affinity_id: &str) -> Result<Option<AffinityDocument>> {

        let path = self.affinity_document_path(affinity_id);

        if !path.exists() {
            log::debug!(
                "affinity document not found on disk affinity_id={}",
                affinity_id
            );
            return Ok(None);
        }

        let contents = std::fs::read(&path).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to read affinity document from {}: {}",
                path.display(),
                err
            ))
        })?;

        let document: AffinityDocument = bincode::deserialize(&contents).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to deserialize affinity document: {}",
                err
            ))
        })?;

        log::info!(
            "loaded affinity document from disk affinity_id={} revision={}",
            document.affinity_id,
            document.affinity_revision
        );

        Ok(Some(document))

    }

    /// Save affinity document to disk
    pub fn save(&self, document: &AffinityDocument) -> Result<()> {

        let path = self.affinity_document_path(&document.affinity_id);

        let contents = bincode::serialize(document).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to serialize affinity document: {}",
                err
            ))
        })?;

        std::fs::write(&path, &contents).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to write affinity document to {}: {}",
                path.display(),
                err
            ))
        })?;

        log::debug!(
            "saved affinity document to disk affinity_id={} revision={} size_bytes={}",
            document.affinity_id,
            document.affinity_revision,
            contents.len()
        );

        Ok(())

    }

    /// Delete affinity document from disk
    pub fn delete(&self, affinity_id: &str) -> Result<()> {

        let path = self.affinity_document_path(affinity_id);

        if !path.exists() {
            return Ok(());
        }

        std::fs::remove_file(&path).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to delete affinity document at {}: {}",
                path.display(),
                err
            ))
        })?;

        log::debug!(
            "deleted affinity document from disk affinity_id={}",
            affinity_id
        );

        Ok(())

    }

    /// Load checkpoint metadata from disk if it exists
    pub fn load_checkpoint(&self, affinity_id: &str) -> Result<Option<CheckpointMetadata>> {

        let path = self.checkpoint_path(affinity_id);

        if !path.exists() {
            log::debug!(
                "checkpoint metadata not found on disk affinity_id={}",
                affinity_id
            );
            return Ok(None);
        }

        let contents = std::fs::read(&path).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to read checkpoint metadata from {}: {}",
                path.display(),
                err
            ))
        })?;

        let checkpoint: CheckpointMetadata = bincode::deserialize(&contents).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to deserialize checkpoint metadata: {}",
                err
            ))
        })?;

        log::info!(
            "loaded checkpoint metadata from disk affinity_id={} phase={:?} progress={}%",
            checkpoint.affinity_id,
            checkpoint.current_phase,
            checkpoint.progress_percentage(100) // placeholder total
        );

        Ok(Some(checkpoint))

    }

    /// Save checkpoint metadata to disk
    pub fn save_checkpoint(&self, checkpoint: &CheckpointMetadata) -> Result<()> {

        let path = self.checkpoint_path(&checkpoint.affinity_id);

        let contents = bincode::serialize(checkpoint).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to serialize checkpoint metadata: {}",
                err
            ))
        })?;

        std::fs::write(&path, &contents).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to write checkpoint metadata to {}: {}",
                path.display(),
                err
            ))
        })?;

        log::debug!(
            "saved checkpoint metadata to disk affinity_id={} phase={:?} completed_steps={}",
            checkpoint.affinity_id,
            checkpoint.current_phase,
            checkpoint.completed_step_indices.len()
        );

        Ok(())

    }

    /// Delete checkpoint metadata from disk
    pub fn delete_checkpoint(&self, affinity_id: &str) -> Result<()> {

        let path = self.checkpoint_path(affinity_id);

        if !path.exists() {
            return Ok(());
        }

        std::fs::remove_file(&path).map_err(|err| {
            ServerLibError::Storage(format!(
                "failed to delete checkpoint metadata at {}: {}",
                path.display(),
                err
            ))
        })?;

        log::debug!(
            "deleted checkpoint metadata from disk affinity_id={}",
            affinity_id
        );

        Ok(())

    }
    
}

#[cfg(test)]
#[path = "storage_test.rs"]
mod tests;



