use criterion::{criterion_group, criterion_main, Criterion};
use datacave_sql::parse_sql;
use datacave_sql::executor::SqlExecutor;
use datacave_core::catalog::Catalog;
use datacave_core::mvcc::MvccManager;
use datacave_lsm::engine::{LsmEngine, LsmOptions};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn sql_parse_bench(c: &mut Criterion) {
    c.bench_function("sql_parse", |b| {
        b.iter(|| {
            let _ = parse_sql("SELECT * FROM users WHERE id = 42;");
        })
    });
}

fn sql_end_to_end_bench(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    c.bench_function("sql_end_to_end", |b| {
        b.iter(|| {
            rt.block_on(async {
            let dir = TempDir::new().expect("tempdir");
            let data_dir = dir.path().join("data");
            std::fs::create_dir_all(&data_dir).expect("data dir");
            let wal_path = data_dir.join("wal.log");
            let storage = Arc::new(
                LsmEngine::open(LsmOptions {
                    data_dir: data_dir.to_string_lossy().to_string(),
                    wal_path: wal_path.to_string_lossy().to_string(),
                    memtable_max_bytes: 1024,
                    encryption_key: None,
                    wal_enabled: true,
                })
                .await
                .expect("open"),
            );
            let catalog = Arc::new(Mutex::new(Catalog::new()));
            let mvcc = Arc::new(MvccManager::new());
            let executor = SqlExecutor::new(catalog, mvcc, storage);

            let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
            executor.execute(&stmts[0], Some("bench")).await.expect("create");
            let stmts =
                parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice');").expect("parse");
            executor.execute(&stmts[0], Some("bench")).await.expect("insert");
            let stmts = parse_sql("SELECT * FROM users;").expect("parse");
            let _ = executor.execute(&stmts[0], Some("bench")).await.expect("select");
            })
        })
    });
}

criterion_group!(sql_benches, sql_parse_bench, sql_end_to_end_bench);
criterion_main!(sql_benches);
