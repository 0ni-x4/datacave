#[cfg(test)]
mod tests {
    use crate::engine::{LsmEngine, LsmOptions};
    use datacave_core::mvcc::Version;
    use tempfile::TempDir;

    #[tokio::test]
    async fn put_get_roundtrip() {
        let dir = TempDir::new().expect("tempdir");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let wal_path = data_dir.join("wal.log");
        let options = LsmOptions {
            data_dir: data_dir.to_string_lossy().to_string(),
            wal_path: wal_path.to_string_lossy().to_string(),
            memtable_max_bytes: 1024,
            encryption_key: None,
        };
        let engine = LsmEngine::open(options).await.expect("open");
        let key = b"user:1";
        let value = b"alice";
        engine.put(key, value, Version::new(1)).await.expect("put");
        let got = engine.get(key, Version::new(1)).await.expect("get");
        assert_eq!(got, Some(value.to_vec()));
    }

    #[tokio::test]
    async fn delete_removes_value() {
        let dir = TempDir::new().expect("tempdir");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let wal_path = data_dir.join("wal.log");
        let options = LsmOptions {
            data_dir: data_dir.to_string_lossy().to_string(),
            wal_path: wal_path.to_string_lossy().to_string(),
            memtable_max_bytes: 1024,
            encryption_key: None,
        };
        let engine = LsmEngine::open(options).await.expect("open");
        let key = b"user:2";
        engine.put(key, b"bob", Version::new(1)).await.expect("put");
        engine.delete(key, Version::new(2)).await.expect("delete");
        let got = engine.get(key, Version::new(2)).await.expect("get");
        assert_eq!(got, None);
    }
}
