# Datacave Compatibility Test Matrix

This document lists implemented SQL semantics and wire protocol behaviors with precise details for compatibility testing and regression coverage.

## SQL Semantics

### DDL

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `CREATE TABLE` | Supported | Single table, basic column types (INT, BIGINT, TEXT, BOOLEAN, FLOAT). Primary key via `PRIMARY KEY (col)` constraint. |
| `DROP TABLE` | Parser only | Parser accepts; planner/executor not wired. |
| `CREATE INDEX` | Not supported | Planned. |

### DML

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `INSERT` | Supported | Values list; single-table only. Column list optional; values aligned by schema or position. |
| `SELECT` | Supported | Single-table or INNER JOIN; projection, `*`, qualified column names. WHERE with column op literal (=, !=, >, >=, <, <=) and AND. ORDER BY, LIMIT, OFFSET supported. |
| `UPDATE` | Supported | Single-table assignments. WHERE with column op literal and AND (updates only matching rows). |
| `DELETE` | Supported | Single-table. WHERE with column op literal and AND (deletes only matching rows). |

### Joins

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `INNER JOIN` | Supported | Multi-table chains (3+). Equality `ON col1 = col2` or `USING (col)`. |
| `LEFT JOIN` | Supported | Multi-table chains; equality ON or USING. |
| `RIGHT JOIN` | Not supported | Planner returns `None` for non-inner/non-left. |
| `FULL OUTER JOIN` | Not supported | Planner returns `None`. |
| `CROSS JOIN` | Not supported | Planner returns `None`. |
| Multi-table (3+) joins | Supported | INNER/LEFT chain; ON must reference tables in scope; USING supported. When selecting multiple columns with the same unqualified name from different tables (e.g. `customers.name`, `products.name`), use qualified names in SELECT; column order may vary. |

### Aggregations

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `COUNT(*)` | Supported | Returns `Int64`. |
| `COUNT(col)` | Supported | Counts non-null values. |
| `SUM(col)` | Supported | Returns `Float64`; numeric types only. |
| `AVG(col)` | Supported | Returns `Float64`. |
| `MIN(col)` | Supported | Returns `Float64` for numeric; nulls excluded. |
| `MAX(col)` | Supported | Returns `Float64` for numeric; nulls excluded. |
| `GROUP BY` | Supported | Single-table and join+aggregate. `GROUP BY ALL` not supported. |
| `HAVING` | Supported | Only with GROUP BY. Column/alias, literal, or aggregate operands; `=`, `!=`, `<`, `<=`, `>`, `>=`. Aggregate expressions (e.g. `HAVING COUNT(*) > 2`, `HAVING SUM(amount) >= 100`) supported. AND for compound conditions. Single-table and join-grouped. |

### Aggregates on Joined Rows

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `SELECT COUNT(*) FROM t1 JOIN t2 ON ...` | Supported | Join executed first; aggregates computed on joined result. |
| `SELECT SUM(col) FROM t1 JOIN t2 ON ...` | Supported | Same as above. |
| `SELECT ... FROM t1 JOIN t2 ON ... ORDER BY col LIMIT n` | Supported | JOIN executed first; ORDER BY and LIMIT applied to joined result. |
| `SELECT ... FROM t1 JOIN t2 ON ... WHERE col = lit` | Supported | WHERE applied to joined result (col op literal). |

### Ordering and Pagination

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `ORDER BY` | Supported | Column name, qualified column, or 1-based position. ASC/DESC. |
| `LIMIT` | Supported | Numeric literal only. |
| `OFFSET` | Supported | Numeric literal only. |

### Transaction Control

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `BEGIN` / `START TRANSACTION` | Accepted | Wire-accepted; returns `BEGIN` tag; ReadyForQuery `InTransaction`. Mutating statements buffered. |
| `COMMIT` | Accepted | Executes buffered statements; ReadyForQuery `Idle`. |
| `ROLLBACK` | Accepted | Discards buffer; ReadyForQuery `Idle`. |
| Atomicity | Not implemented | No multi-statement atomicity or isolation. |

### Other SQL

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| Subqueries | Not supported | Planned (IN, EXISTS, scalar). |
| `WHERE` | Supported | SELECT, UPDATE, DELETE. Predicates: column op literal for =, !=, >, >=, <, <=. AND and OR combinations, parenthesized expressions. Column/literal order arbitrary. No functions, subqueries. |

## Wire Protocol

### Message Types

| Message | Direction | Status | Notes |
|---------|-----------|--------|-------|
| Startup | Client→Server | Supported | Cleartext params: user, database, tenant_id. |
| AuthenticationCleartextPassword | Server→Client | Supported | When auth enabled. |
| Password | Client→Server | Supported | Cleartext password. |
| AuthenticationOk | Server→Client | Supported | On success. |
| ParameterStatus | Server→Client | Supported | server_version. |
| ReadyForQuery | Server→Client | Supported | `I` (Idle), `T` (Transaction), `E` (Error). |
| Query (Q) | Client→Server | Supported | Simple query. |
| Parse (P) | Client→Server | Supported | Statement name + query. |
| Bind (B) | Client→Server | Supported | Portal + statement; param substitution for $1, $2, ?. |
| Execute (E) | Client→Server | Supported | Portal + max_rows. |
| Sync (S) | Client→Server | Supported | Flushes extended-query pipeline. |
| Flush (H) | Client→Server | Supported | Request to flush output buffer; no-op (unbuffered). |
| Describe (D) | Client→Server | Supported | Statement or Portal; returns NoData. |
| ParseComplete (1) | Server→Client | Supported | After Parse. |
| BindComplete (2) | Server→Client | Supported | After Bind. |
| RowDescription (T) | Server→Client | Supported | Column names for SELECT. |
| DataRow (D) | Server→Client | Supported | Text format values. |
| CommandComplete (C) | Server→Client | Supported | Tag: OK, OK N, SELECT N, BEGIN, COMMIT, ROLLBACK. |
| ErrorResponse (E) | Server→Client | Supported | On error. |
| NoData (n) | Server→Client | Supported | For Describe on non-SELECT. |
| Close (C) | Client→Server | Supported | Statement or Portal. |
| CloseComplete (3) | Server→Client | Supported | After Close. |
| Terminate (X) | Client→Server | Supported | Closes connection. |

### Extended Query Flow

| Step | Message | Response |
|------|---------|----------|
| 1 | Parse (P) | ParseComplete (1) |
| 2 | Bind (B) | BindComplete (2) or ErrorResponse |
| 3 | Describe (D) Portal | NoData (n) or RowDescription |
| 4 | Execute (E) | RowDescription + DataRows + CommandComplete |
| 5 | Sync (S) | ReadyForQuery |

### Ready State Semantics

| State | Code | When |
|-------|------|------|
| Idle | `I` | Not in transaction; after COMMIT/ROLLBACK. |
| Transaction | `T` | After BEGIN; before COMMIT/ROLLBACK. |
| Error | `E` | After failed command; requires ROLLBACK. |

## Test Coverage

- **Unit tests**: `datacave-sql/tests.rs` — create/insert/select, aggregates, inner join, transaction commands.
- **Integration tests**: `datacave-server/server.rs` — simple query roundtrip, CREATE/INSERT/SELECT, BEGIN/COMMIT/ROLLBACK, extended query Parse/Bind/Execute/Sync, joins+aggregates, join+order_by+limit, join+where, WHERE with OR/parentheses, HAVING aggregate expression via simple query. Three-table join exercised (expects failure until supported).
