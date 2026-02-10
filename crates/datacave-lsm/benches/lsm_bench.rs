use criterion::{criterion_group, criterion_main, Criterion};
use datacave_lsm::engine::{LsmEngine, LsmOptions};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn lsm_put_get_bench(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    c.bench_function("lsm_put_get", |b| {
        b.to_async(&rt).iter(|| async {
            let dir = TempDir::new().expect("tempdir");
            let data_dir = dir.path().join("data");
            std::fs::create_dir_all(&data_dir).expect("data dir");
            let wal_path = data_dir.join("wal.log");
            let engine = LsmEngine::open(LsmOptions {
                data_dir: data_dir.to_string_lossy().to_string(),
                wal_path: wal_path.to_string_lossy().to_string(),
                memtable_max_bytes: 1024 * 1024,
                encryption_key: None,
                wal_enabled: true,
            })
            .await
            .expect("open");
            let key = b"user:bench";
            engine.put(key, b"value", 1).await.expect("put");
            let _ = engine.get(key, 1).await.expect("get");
        });
    });
}

criterion_group!(lsm_benches, lsm_put_get_bench);
criterion_main!(lsm_benches);
