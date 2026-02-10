#[cfg(test)]
mod tests {
    use crate::executor::SqlExecutor;
    use crate::parser::parse_sql;
    use datacave_core::catalog::Catalog;
    use datacave_core::mvcc::MvccManager;
    use datacave_lsm::engine::{LsmEngine, LsmOptions};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    #[tokio::test]
    async fn create_insert_select_flow() {
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
            })
            .await
            .expect("open"),
        );
        let catalog = Arc::new(Mutex::new(Catalog::new()));
        let mvcc = Arc::new(MvccManager::new());
        let executor = SqlExecutor::new(catalog, mvcc, storage);

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("SELECT * FROM users;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
    }
}
