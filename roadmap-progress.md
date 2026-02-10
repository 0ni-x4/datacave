# Roadmap Progress

Tracks implemented vs pending parity blocks for Datacave.

## SQL Parity

| Block | Status | Notes |
|-------|--------|-------|
| DDL: CREATE TABLE | Done | Basic schemas, column types, PRIMARY KEY |
| DDL: DROP TABLE | Pending | Parser accepts; planner/executor not wired |
| DML: INSERT | Done | Values list |
| DML: SELECT | Done | Single-table or INNER JOIN; WHERE (col op literal); ORDER BY, LIMIT, OFFSET |
| DML: UPDATE | Done | Single-table; WHERE optional |
| DML: DELETE | Done | Single-table; WHERE optional |
| Joins | Done | Two-table INNER JOIN; ON col1=col2 or USING (col); JOIN+ORDER BY+LIMIT supported |
| Aggregations | Done | COUNT, SUM, AVG, MIN, MAX; GROUP BY; HAVING (column/literal/aggregate expr, AND) |
| ORDER BY / LIMIT / OFFSET | Done | Column/position; numeric literals only |
| Subqueries | Pending | IN, EXISTS, scalar subqueries |
| Transactions (BEGIN/COMMIT/ROLLBACK) | Done | Mutating stmts buffered until COMMIT; no isolation |
| Indexes | Pending | CREATE INDEX, use in plans |

## Protocol Parity

| Block | Status | Notes |
|-------|--------|-------|
| Startup / auth cleartext | Done | |
| Simple query (Q) | Done | |
| Extended query (Parse/Bind/Execute/Sync) | Done | Parse/Bind/Execute/Sync, param substitution $1/$2/? |
| Close (C) | Done | Statement/Portal |
| Terminate (X) | Done | |
| COPY protocol | Pending | |

## Storage Parity

| Block | Status | Notes |
|-------|--------|-------|
| LSM engine | Done | Put, get, delete, compaction |
| WAL | Done | Replay on open |
| SSTable flush | Done | |
| Encryption at rest | Done | Optional |
| Multi-version reads (MVCC) | Done | Versioned snapshots |

## Server / Cluster Parity

| Block | Status | Notes |
|-------|--------|-------|
| Sharding | Done | Hash-based table routing |
| Replication | Done | Leader + replicas, quorum writes |
| Failover | Done | Leader election on failure |
| TLS | Done | Optional |
| Auth (users/roles) | Done | |
| Audit logging | Done | Optional |
| Metrics / health | Done | Prometheus, /health, /ready |

## Pending Roadmap Items (from README)

- [ ] Improved SQL coverage and query planning optimizations
- [ ] Advanced compaction and caching strategies
- [ ] Cluster membership and replication tooling
- [ ] Extended observability (metrics and tracing)
