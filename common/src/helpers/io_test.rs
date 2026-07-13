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

    #[test]
    fn write_bytes_atomic_round_trip() {

        let path = unique_temp_file("atomic");

        write_bytes_atomic(&path, &[9, 8, 7]).expect("write_bytes_atomic should succeed");
        let bytes = read_bytes(&path).expect("read_bytes should succeed");

        assert_eq!(bytes, vec![9, 8, 7]);
        let _ = fs::remove_file(path);

    }
    