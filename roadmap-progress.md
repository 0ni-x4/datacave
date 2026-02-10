# Roadmap Progress

Tracks implemented vs pending parity blocks for Datacave.

## SQL Parity

| Block | Status | Notes |
|-------|--------|-------|
| DDL: CREATE TABLE | Done | Basic schemas, column types |
| DDL: DROP TABLE | Pending | Parser accepts; planner/executor not wired |
| DML: INSERT | Done | Values list |
| DML: SELECT | Done | Single-table, simple projection |
| DML: UPDATE | Done | Single-table assignments |
| DML: DELETE | Done | Single-table |
| Joins | Done | INNER JOIN only; ON col1=col2 or USING (col) |
| Aggregations | Done | COUNT, SUM, AVG, MIN, MAX; no GROUP BY |
| Subqueries | Pending | IN, EXISTS, scalar subqueries |
| transactions (BEGIN/COMMIT/ROLLBACK) | Wire-accepted (no-op) | Server accepts; no multi-statement atomicity |
| Indexes | Pending | CREATE INDEX, use in plans |

## Protocol Parity

| Block | Status | Notes |
|-------|--------|-------|
| Startup / auth cleartext | Done | |
| Simple query (Q) | Done | |
| Extended query (Parse/Bind/Execute/Sync) | Done | Prepared statements, portals |
| Terminate (X) | Done | |
| Prepared statements | Pending | |
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
