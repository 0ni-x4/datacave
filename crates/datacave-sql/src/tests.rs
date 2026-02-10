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
    async fn select_left_join() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        // Users: 1=alice, 2=bob. Orders: user_id 1 and 2 match, user_id 99 has no match
        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 99, 50);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // LEFT JOIN: all 3 orders, row 3 has NULL for users.name
        let stmts = parse_sql("SELECT orders.id, users.name, orders.amount FROM orders LEFT JOIN users ON orders.user_id = users.id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 3, "LEFT JOIN must return all left rows");

        let row1 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(1)).expect("order 1");
        assert_eq!(row1.values[1], DataValue::String("alice".into()));
        assert_eq!(row1.values[2], DataValue::Int64(100));

        let row2 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(2)).expect("order 2");
        assert_eq!(row2.values[1], DataValue::String("bob".into()));
        assert_eq!(row2.values[2], DataValue::Int64(200));

        let row3 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(3)).expect("order 3");
        assert_eq!(row3.values[1], DataValue::Null, "unmatched left row must have NULL right columns");
        assert_eq!(row3.values[2], DataValue::Int64(50));

        // INNER JOIN on same data: only 2 rows (no user_id 99)
        let stmts = parse_sql("SELECT orders.id, users.name FROM orders INNER JOIN users ON orders.user_id = users.id;").expect("parse");
        let inner_result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(inner_result.rows.len(), 2, "INNER JOIN must exclude unmatched rows");
    }

    #[tokio::test]
    async fn select_three_table_inner_join() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE items (id INT, order_id INT, product TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 1, 150);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO items (id, order_id, product) VALUES (1, 1, 'wand'), (2, 2, 'book'), (3, 3, 'hat');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql(
            "SELECT orders.id, users.name, items.product, orders.amount FROM orders \
             INNER JOIN users ON orders.user_id = users.id \
             INNER JOIN items ON orders.id = items.order_id;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");

        assert_eq!(result.rows.len(), 3, "three-table INNER JOIN must return matching rows");
        let row1 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(1)).expect("order 1");
        assert_eq!(row1.values[1], DataValue::String("alice".into()));
        assert_eq!(row1.values[2], DataValue::String("wand".into()));
        assert_eq!(row1.values[3], DataValue::Int64(100));

        let row2 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(2)).expect("order 2");
        assert_eq!(row2.values[1], DataValue::String("bob".into()));
        assert_eq!(row2.values[2], DataValue::String("book".into()));
        assert_eq!(row2.values[3], DataValue::Int64(200));

        let row3 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(3)).expect("order 3");
        assert_eq!(row3.values[1], DataValue::String("alice".into()));
        assert_eq!(row3.values[2], DataValue::String("hat".into()));
        assert_eq!(row3.values[3], DataValue::Int64(150));
    }

    #[tokio::test]
    async fn select_three_table_left_join_chain() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE items (id INT, order_id INT, product TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 99, 50);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO items (id, order_id, product) VALUES (1, 1, 'wand'), (2, 2, 'book');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql(
            "SELECT orders.id, users.name, items.product FROM orders \
             LEFT JOIN users ON orders.user_id = users.id \
             LEFT JOIN items ON orders.id = items.order_id;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");

        assert_eq!(result.rows.len(), 3, "LEFT JOIN chain must return all left rows");
        let row1 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(1)).expect("order 1");
        assert_eq!(row1.values[1], DataValue::String("alice".into()));
        assert_eq!(row1.values[2], DataValue::String("wand".into()));

        let row2 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(2)).expect("order 2");
        assert_eq!(row2.values[1], DataValue::String("bob".into()));
        assert_eq!(row2.values[2], DataValue::String("book".into()));

        let row3 = result.rows.iter().find(|r| r.values[0] == DataValue::Int64(3)).expect("order 3");
        assert_eq!(row3.values[1], DataValue::Null, "unmatched user_id 99 -> NULL");
        assert_eq!(row3.values[2], DataValue::Null, "order 3 has no items row -> NULL");
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

        // HAVING COUNT(*) > 2: aggregate expression (west and east both have 3, so both pass)
        let stmts = parse_sql(
            "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region HAVING COUNT(*) > 2;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2, "both east (3) and west (3) have COUNT(*) > 2");

        // HAVING COUNT(*) > 2: filter to groups with more than 2 rows (region,product groups)
        let stmts = parse_sql(
            "SELECT region, product, COUNT(*) FROM sales GROUP BY region, product HAVING COUNT(*) > 2;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 0, "no region+product group has > 2 rows");

        // HAVING SUM(amount) >= 100: west=150, east=60 -> only west
        let stmts = parse_sql(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) >= 100;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Float64(150.0));

        // HAVING SUM(amount) >= 100 with literal on left: 100 <= SUM(amount)
        let stmts = parse_sql(
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING 100 <= SUM(amount);"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));

        // HAVING COUNT(*) > 2 AND SUM(amount) >= 100: both east and west have count>2, only west has sum>=100
        let stmts = parse_sql(
            "SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region HAVING COUNT(*) > 2 AND SUM(amount) >= 100;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("west".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Int64(3));
        assert_eq!(result.rows[0].values[2], DataValue::Float64(150.0));
    }

    #[tokio::test]
    async fn select_join_group_by_having() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE orders (id INT, user_id INT, amount INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        let stmts = parse_sql("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 75);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // JOIN + GROUP BY + HAVING SUM(amount) >= 100: alice=300, bob=50, carol=75
        let stmts = parse_sql(
            "SELECT users.name, SUM(orders.amount) FROM orders INNER JOIN users ON orders.user_id = users.id GROUP BY users.name HAVING SUM(orders.amount) >= 100;"
        ).expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::String("alice".into()));
        assert_eq!(result.rows[0].values[1], DataValue::Float64(300.0));
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
    async fn select_where_filtering() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE t (id INT, val INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO t (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // WHERE id = 3
        let stmts = parse_sql("SELECT * FROM t WHERE id = 3;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::Int64(3));
        assert_eq!(result.rows[0].values[1], DataValue::Int64(30));

        // WHERE val > 25
        let stmts = parse_sql("SELECT * FROM t WHERE val > 25;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 3, "val 30, 40, 50");

        // WHERE val >= 30
        let stmts = parse_sql("SELECT * FROM t WHERE val >= 30;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 3);

        // WHERE val < 20
        let stmts = parse_sql("SELECT * FROM t WHERE val < 20;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], DataValue::Int64(10));

        // WHERE val != 30
        let stmts = parse_sql("SELECT * FROM t WHERE val != 30;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 4);

        // WHERE id = 2 AND val = 20
        let stmts = parse_sql("SELECT * FROM t WHERE id = 2 AND val = 20;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], DataValue::Int64(2));
        assert_eq!(result.rows[0].values[1], DataValue::Int64(20));

        // WHERE id > 1 AND val < 40
        let stmts = parse_sql("SELECT * FROM t WHERE id > 1 AND val < 40;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2, "id 2 val 20, id 3 val 30");

        // WHERE id = 2 OR id = 4
        let stmts = parse_sql("SELECT * FROM t WHERE id = 2 OR id = 4;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);
        let ids: Vec<i64> = result.rows.iter().map(|r| match r.values[0] { DataValue::Int64(x) => x, _ => 0 }).collect();
        assert!(ids.contains(&2) && ids.contains(&4));

        // WHERE (id = 1 OR id = 5) AND val > 10  -- parenthesized OR, then AND
        let stmts = parse_sql("SELECT * FROM t WHERE (id = 1 OR id = 5) AND val > 10;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 1, "only id=5 has val>10 of (1,5); id=1 has val=10");
        assert_eq!(result.rows[0].values[0], DataValue::Int64(5));

        // WHERE id > 1 AND val < 40 OR id = 1  -- AND binds tighter: (id>1 AND val<40) OR id=1
        let stmts = parse_sql("SELECT * FROM t WHERE id > 1 AND val < 40 OR id = 1;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 3, "id 2,3 from AND, plus id 1 from OR");
    }

    #[tokio::test]
    async fn update_with_where() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE t (id INT, val INT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO t (id, val) VALUES (1, 10), (2, 20), (3, 30), (4, 40);"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // UPDATE without WHERE would update all; with WHERE id = 2 only that row
        let stmts = parse_sql("UPDATE t SET val = 99 WHERE id = 2;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("update");
        assert_eq!(result.rows_affected, 1);

        let stmts = parse_sql("SELECT * FROM t ORDER BY id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.rows[0].values[1], DataValue::Int64(10));  // id 1 unchanged
        assert_eq!(result.rows[1].values[1], DataValue::Int64(99)); // id 2 updated
        assert_eq!(result.rows[2].values[1], DataValue::Int64(30));  // id 3 unchanged
        assert_eq!(result.rows[3].values[1], DataValue::Int64(40));  // id 4 unchanged

        // UPDATE with AND
        let stmts = parse_sql("UPDATE t SET val = 77 WHERE id = 3 AND val = 30;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("update");
        assert_eq!(result.rows_affected, 1);

        let stmts = parse_sql("SELECT val FROM t WHERE id = 3;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows[0].values[0], DataValue::Int64(77));
    }

    #[tokio::test]
    async fn delete_with_where() {
        let (executor, _) = setup_executor().await;

        let stmts = parse_sql("CREATE TABLE t (id INT, name TEXT);").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("create");

        let stmts = parse_sql(
            "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');"
        ).expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");

        // DELETE WHERE id = 2
        let stmts = parse_sql("DELETE FROM t WHERE id = 2;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("delete");
        assert_eq!(result.rows_affected, 1);

        let stmts = parse_sql("SELECT * FROM t ORDER BY id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].values[1], DataValue::String("a".into()));
        assert_eq!(result.rows[1].values[1], DataValue::String("c".into()));
        assert_eq!(result.rows[2].values[1], DataValue::String("d".into()));

        // DELETE WHERE name = 'c'
        let stmts = parse_sql("DELETE FROM t WHERE name = 'c';").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("delete");
        assert_eq!(result.rows_affected, 1);

        let stmts = parse_sql("SELECT * FROM t ORDER BY id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2);

        // DELETE with OR
        let stmts = parse_sql("INSERT INTO t (id, name) VALUES (5, 'e'), (6, 'f');").expect("parse");
        executor.execute(&stmts[0], Some("t1")).await.expect("insert");
        let stmts = parse_sql("DELETE FROM t WHERE id = 1 OR id = 6;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("delete");
        assert_eq!(result.rows_affected, 2);
        let stmts = parse_sql("SELECT * FROM t ORDER BY id;").expect("parse");
        let result = executor.execute(&stmts[0], Some("t1")).await.expect("select");
        assert_eq!(result.rows.len(), 2, "d,e remain after deleting id 1 and 6");
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
