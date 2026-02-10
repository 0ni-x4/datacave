#[cfg(test)]
mod tests {
    use crate::engine::{LsmEngine, LsmOptions};
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
            wal_enabled: true,
        };
        let engine = LsmEngine::open(options).await.expect("open");
        let key = b"user:1";
        let value = b"alice";
        engine.put(key, value, 1).await.expect("put");
        let got = engine.get(key, 1).await.expect("get");
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
            wal_enabled: true,
        };
        let engine = LsmEngine::open(options).await.expect("open");
        let key = b"user:2";
        engine.put(key, b"bob", 1).await.expect("put");
        engine.delete(key, 2).await.expect("delete");
        let got = engine.get(key, 2).await.expect("get");
        assert_eq!(got, None);
    }

    #[tokio::test]
    async fn wal_replay_restores_memtable() {
        let dir = TempDir::new().expect("tempdir");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let wal_path = data_dir.join("wal.log");
        let options = LsmOptions {
            data_dir: data_dir.to_string_lossy().to_string(),
            wal_path: wal_path.to_string_lossy().to_string(),
            memtable_max_bytes: 1024,
            encryption_key: None,
            wal_enabled: true,
        };
        let engine = LsmEngine::open(options.clone()).await.expect("open");
        engine.put(b"user:3", b"claire", 1).await.expect("put");
        drop(engine);
        let restored = LsmEngine::open(options).await.expect("open");
        let got = restored.get(b"user:3", 1).await.expect("get");
        assert_eq!(got, Some(b"claire".to_vec()));
    }

    #[tokio::test]
    async fn sstable_reload_works() {
        let dir = TempDir::new().expect("tempdir");
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("data dir");
        let wal_path = data_dir.join("wal.log");
        let options = LsmOptions {
            data_dir: data_dir.to_string_lossy().to_string(),
            wal_path: wal_path.to_string_lossy().to_string(),
            memtable_max_bytes: 64,
            encryption_key: None,
            wal_enabled: true,
        };
        let engine = LsmEngine::open(options.clone()).await.expect("open");
        engine.put(b"user:4", b"dana", 1).await.expect("put");
        engine.flush().await.expect("flush");
        drop(engine);
        let restored = LsmEngine::open(options).await.expect("open");
        let got = restored.get(b"user:4", 1).await.expect("get");
        assert_eq!(got, Some(b"dana".to_vec()));
    }
}
