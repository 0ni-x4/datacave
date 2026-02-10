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
| `SELECT` | Supported | Single-table projection, `*`, qualified column names. |
| `UPDATE` | Supported | Single-table assignments; no subqueries, no `WHERE`. |
| `DELETE` | Supported | Single-table; no `WHERE` (deletes all visible rows). |

### Joins

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `INNER JOIN` | Supported | Single join between two tables. Equality `ON col1 = col2` or `USING (col)`. |
| `LEFT JOIN` | Not supported | Planner returns `None` for non-inner joins. |
| `RIGHT JOIN` | Not supported | Planner returns `None`. |
| `FULL OUTER JOIN` | Not supported | Planner returns `None`. |
| `CROSS JOIN` | Not supported | Planner returns `None`. |
| Multi-table joins | Not supported | Planner supports only one join. |

### Aggregations

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `COUNT(*)` | Supported | Returns `Int64`. |
| `COUNT(col)` | Supported | Counts non-null values. |
| `SUM(col)` | Supported | Returns `Float64`; numeric types only. |
| `AVG(col)` | Supported | Returns `Float64`. |
| `MIN(col)` | Supported | Returns `Float64` for numeric; nulls excluded. |
| `MAX(col)` | Supported | Returns `Float64` for numeric; nulls excluded. |
| `GROUP BY` | Not supported | All rows treated as one group. |
| `HAVING` | Not supported | Planned. |

### Aggregates on Joined Rows

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `SELECT COUNT(*) FROM t1 JOIN t2 ON ...` | Supported | Join executed first; aggregates computed on joined result. |
| `SELECT SUM(col) FROM t1 JOIN t2 ON ...` | Supported | Same as above. |

### Transaction Control

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `BEGIN` / `START TRANSACTION` | Accepted (no-op) | Wire-accepted; returns `BEGIN` tag; ReadyForQuery `InTransaction`. |
| `COMMIT` | Accepted (no-op) | Returns `COMMIT` tag; ReadyForQuery `Idle`. |
| `ROLLBACK` | Accepted (no-op) | Returns `ROLLBACK` tag; ReadyForQuery `Idle`. |
| Atomicity | Not implemented | No multi-statement atomicity or isolation. |

### Other SQL

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| Subqueries | Not supported | Planned (IN, EXISTS, scalar). |
| `WHERE` | Not supported | Planned. |
| `ORDER BY` | Not supported | Planned. |
| `LIMIT` / `OFFSET` | Not supported | Planned. |

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
| Bind (B) | Client→Server | Supported | Portal + statement; 0 param format codes. |
| Execute (E) | Client→Server | Supported | Portal + max_rows. |
| Sync (S) | Client→Server | Supported | Flushes extended-query pipeline. |
| Describe (D) | Client→Server | Supported | Statement or Portal; returns NoData. |
| ParseComplete (1) | Server→Client | Supported | After Parse. |
| BindComplete (2) | Server→Client | Supported | After Bind. |
| RowDescription (T) | Server→Client | Supported | Column names for SELECT. |
| DataRow (D) | Server→Client | Supported | Text format values. |
| CommandComplete (C) | Server→Client | Supported | Tag: OK, OK N, SELECT N, BEGIN, COMMIT, ROLLBACK. |
| ErrorResponse (E) | Server→Client | Supported | On error. |
| NoData (n) | Server→Client | Supported | For Describe on non-SELECT. |
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
- **Integration tests**: `datacave-server/server.rs` — simple query roundtrip, CREATE/INSERT/SELECT, BEGIN/COMMIT/ROLLBACK, extended query Parse/Bind/Execute/Sync, joins+aggregates via simple query.
