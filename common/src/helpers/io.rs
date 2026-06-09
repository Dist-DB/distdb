use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    fs::write(path, content)
}

pub fn append_bytes(path: impl AsRef<Path>, content: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
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
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("distdb-common-io-{name}-{nanos}.tmp"))
    }

    #[test]
    fn write_and_read_text_round_trip() {
        let path = unique_temp_file("text");

        write_text(&path, "hello world").expect("write_text should succeed");
        let data = read_text(&path).expect("read_text should succeed");

        assert_eq!(data, "hello world");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_creates_parent_directories() {
        let dir = std::env::temp_dir().join(format!(
            "distdb-common-io-dir-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let file = dir.join("nested").join("payload.bin");

        write_bytes(&file, &[1, 2, 3]).expect("write_bytes should succeed");
        let bytes = read_bytes(&file).expect("read_bytes should succeed");

        assert_eq!(bytes, vec![1, 2, 3]);
        let _ = fs::remove_file(file);
        let _ = fs::remove_dir_all(dir);
    }
}
