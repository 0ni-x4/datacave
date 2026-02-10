#[cfg(test)]
mod tests {
    use crate::executor::SqlExecutor;
    use crate::parser::parse_sql;
    use crate::planner::plan_statement;
    use datacave_core::catalog::Catalog;
    use datacave_core::mvcc::MvccManager;
    use datacave_core::types::DataValue;
    use datacave_lsm::engine::{LsmEngine, LsmOptions};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    async fn setup_executor() -> (SqlExecutor, TempDir) {
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
        (executor, dir)
    }

    #[tokio::test]
    async fn create_insert_select_flow() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("SELECT * FROM users;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn select_aggregates() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE t (id INT, val INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO t (id, val) VALUES (1, 10), (2, 20), (3, 30);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("SELECT COUNT(*) FROM t;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::Int64(3));

        let stmts = parse_sql("SELECT SUM(val) FROM t;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows[0].values[0], DataValue::Float64(60.0));

        let stmts = parse_sql("SELECT AVG(val) FROM t;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows[0].values[0], DataValue::Float64(20.0));

        let stmts = parse_sql("SELECT MIN(val), MAX(val) FROM t;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows[0].values[0], DataValue::Float64(10.0));
        assert_eq!(result.rows[0].values[1], DataValue::Float64(30.0));
    }

    #[tokio::test]
    async fn select_inner_join() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("SELECT orders.id, users.name FROM orders INNER JOIN users ON orders.user_id = users.id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[1], DataValue::String("alice".into()));
        assert_eq!(result.rows[1].values[1], DataValue::String("bob".into()));
    }

    #[tokio::test]
    async fn select_group_by_aggregates() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE sales (region TEXT, product TEXT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO sales (region, product, amount) VALUES \
             ('east', 'A', 10), ('east', 'A', 20), ('east', 'B', 30), \
             ('west', 'A', 40), ('west', 'B', 50), ('west', 'B', 60);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // Verify 6 rows inserted
        let stmts = parse_sql("SELECT * FROM sales;").expect("parse");
        let all_rows = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(all_rows.rows.len(), 6, "expect 6 rows in sales");

        // GROUP BY region: 2 groups (east, west), multiple aggregates
        let stmts = parse_sql(
            "SELECT region, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales GROUP BY region;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2, "expect 2 groups (east, west)");

        let east_row = result.rows.iter().find(|r| r.values[0] == DataValue::String("east".into())).expect("east");
        assert_eq!(east_row.values[1], DataValue::Int64(3), "east COUNT(*)");
        assert_eq!(east_row.values[2], DataValue::Float64(60.0), "east SUM(amount)");
        assert_eq!(east_row.values[3], DataValue::Float64(20.0), "east AVG(amount)");
        assert_eq!(east_row.values[4], DataValue::Float64(10.0), "east MIN(amount)");
        assert_eq!(east_row.values[5], DataValue::Float64(30.0), "east MAX(amount)");

        let west_row = result.rows.iter().find(|r| r.values[0] == DataValue::String("west".into())).expect("west");
        assert_eq!(west_row.values[1], DataValue::Int64(3), "west COUNT(*)");
        assert_eq!(west_row.values[2], DataValue::Float64(150.0), "west SUM(amount)");
        assert_eq!(west_row.values[3], DataValue::Float64(50.0), "west AVG(amount)");
        assert_eq!(west_row.values[4], DataValue::Float64(40.0), "west MIN(amount)");
        assert_eq!(west_row.values[5], DataValue::Float64(60.0), "west MAX(amount)");

        // GROUP BY region, product: 4 groups
        let stmts = parse_sql(
            "SELECT region, product, COUNT(*), SUM(amount) FROM sales GROUP BY region, product;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 4, "expect 4 groups (east/A, east/B, west/A, west/B)");
    }

    #[tokio::test]
    async fn select_group_by_having() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE sales (region TEXT, product TEXT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO sales (region, product, amount) VALUES \
             ('east', 'A', 10), ('east', 'A', 20), ('east', 'B', 30), \
             ('west', 'A', 40), ('west', 'B', 50), ('west', 'B', 60);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // HAVING region = 'west': column vs literal (supported)
        let stmts = parse_sql(
            "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region HAVING region = 'west';"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Int64(3));
        assert_eq!(result.rows[0].values[2], DataValue::Float64(150.0));

        // HAVING region != 'east': filters to west only
        let stmts = parse_sql(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING region != 'east';"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Float64(150.0));
    }

    #[tokio::test]
    async fn select_order_by_limit() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE t (id INT, val INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO t (id, val) VALUES (1, 30), (2, 10), (3, 20), (4, 40), (5, 5);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // ORDER BY val ASC, LIMIT 2
        let stmts = parse_sql("SELECT id, val FROM t ORDER BY val ASC LIMIT 2;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[1], DataValue::Int64(5));
        assert_eq!(result.rows[1].values[1], DataValue::Int64(10));

        // ORDER BY val DESC, LIMIT 2
        let stmts = parse_sql("SELECT id, val FROM t ORDER BY val DESC LIMIT 2;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[1], DataValue::Int64(40));
        assert_eq!(result.rows[1].values[1], DataValue::Int64(30));

        // ORDER BY 2 (position), LIMIT 1, OFFSET 2
        let stmts = parse_sql("SELECT id, val FROM t ORDER BY 2 ASC LIMIT 1 OFFSET 2;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], DataValue::Int64(20));
    }

    #[tokio::test]
    async fn select_order_by_limit_with_aggregates() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE sales (region TEXT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO sales (region, amount) VALUES ('east', 60), ('west', 150), ('north', 80);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // ORDER BY aggregate alias (total), LIMIT 2
        let stmts = parse_sql(
            "SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY total DESC LIMIT 2;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Float64(150.0));
        assert_eq!(result.rows[1].values[0], DataValue::String("north".into()));
    }

    #[tokio::test]
    async fn select_join_order_by_limit() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 3, 150);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql(
            "SELECT orders.id, users.name, orders.amount FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY amount DESC LIMIT 2;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[2], DataValue::Int64(200));
        assert_eq!(result.rows[1].values[2], DataValue::Int64(150));
    }

    #[tokio::test]
    async fn begin_commit_rollback_plan_and_execute() {
        let (executor, _) = setup_executor().await;

        for sql in ["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"] {
            let stmts = parse_sql(sql).expect("parse");
            assert!(!stmts.is_empty(), "{} should parse", sql);
            let plan = plan_statement(&stmts[0]);
            assert!(plan.is_some(), "{} should produce a plan", sql);
            let result = executor.execute(&stmts[0], Some("t1")).await;
            assert!(result.is_ok(), "{} should execute without error", sql);
        }
    }
}
