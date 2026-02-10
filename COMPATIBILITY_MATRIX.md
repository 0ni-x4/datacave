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
| `SELECT` | Supported | Single-table projection, `*`, qualified column names. No WHERE filtering. ORDER BY, LIMIT, OFFSET supported. |
| `UPDATE` | Supported | Single-table assignments; no WHERE (updates all visible rows). |
| `DELETE` | Supported | Single-table; no WHERE (deletes all visible rows). |

### Joins

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `INNER JOIN` | Supported | Two-table only. Equality `ON col1 = col2` or `USING (col)`. Executor uses first join only. |
| `LEFT JOIN` | Not supported | Planner returns `None` for non-inner joins. |
| `RIGHT JOIN` | Not supported | Planner returns `None`. |
| `FULL OUTER JOIN` | Not supported | Planner returns `None`. |
| `CROSS JOIN` | Not supported | Planner returns `None`. |
| Multi-table (3+) joins | Not supported | Planner rejects; extract_equality_columns fails for chained joins. |

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
| `HAVING` | Partial | Only with GROUP BY. Column/alias or literal operands; `=`, `!=`, `<`, `<=`, `>`, `>=`. Aggregate expressions (e.g. `HAVING COUNT(*) > 2`) not supported. |

### Aggregates on Joined Rows

| Feature | Status | Implementation Notes |
|---------|--------|------------------------|
| `SELECT COUNT(*) FROM t1 JOIN t2 ON ...` | Supported | Join executed first; aggregates computed on joined result. |
| `SELECT SUM(col) FROM t1 JOIN t2 ON ...` | Supported | Same as above. |

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
| `WHERE` | Not supported | SELECT, UPDATE, DELETE ignore WHERE. |

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
- **Integration tests**: `datacave-server/server.rs` — simple query roundtrip, CREATE/INSERT/SELECT, BEGIN/COMMIT/ROLLBACK, extended query Parse/Bind/Execute/Sync, joins+aggregates via simple query.
