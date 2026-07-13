use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::epoch_nanos;

pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(path)
}

pub fn read_text(path: impl AsRef<Path>) -> io::Result<String> {
    fs::read_to_string(path)
}

pub fn read_bytes(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    fs::read(path)
}

pub fn write_text(path: impl AsRef<Path>, content: &str) -> io::Result<()> {
    write_bytes(path, content.as_bytes())
}

pub fn write_bytes(path: impl AsRef<Path>, content: &[u8]) -> io::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }

    fs::write(path, content)
}

pub fn write_bytes_atomic(path: impl AsRef<Path>, content: &[u8]) -> io::Result<()> {
    
    let path = path.as_ref();

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;

    fs::create_dir_all(parent)?;

    let tmp_name = format!(
        ".{}.tmp-{}-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("distdb"),
        std::process::id(),
        epoch_nanos!(),
    );
    let tmp_path = parent.join(tmp_name);

    let mut file = File::create(&tmp_path)?;
    file.write_all(content)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp_path, path)?;

    if let Ok(dir_file) = File::open(parent) {
        let _ = dir_file.sync_all();
    }

    Ok(())

}

pub fn append_bytes(path: impl AsRef<Path>, content: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let path = path.as_ref();

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }

    let mut file = fs::OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(content)
}

pub fn list_files(path: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            files.push(entry.path());
        }
    }
    Ok(files)
}

#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
