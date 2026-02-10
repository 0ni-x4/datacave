# Datacave Benchmarks

## Environment

- Machine: local dev environment
- Build: `cargo bench` (criterion, release profile)
- Data: temporary local filesystem
- Note: results reflect small, in-process benchmarks and are not a full database workload.

## Results

### datacave-lsm

Command:

```
cargo bench -p datacave-lsm
```

Results:

- `lsm_put_get`: 416–455 µs per iteration (criterion range)

### datacave-sql

Command:

```
cargo bench -p datacave-sql
```

Results:

- `sql_parse`: 3.30–3.72 µs per iteration
- `sql_end_to_end`: 419–447 µs per iteration

## Comparison Against Known Databases

These results are **not directly comparable** to other databases without running the same workloads, on the same hardware, with identical datasets and settings. The benchmarks above are micro‑benchmarks and include setup/teardown cost in a single process.

Recommended comparisons to run for a fair, production‑relevant baseline:

- **PostgreSQL**: run `pgbench` with identical dataset sizes and collect TPS/latency.
- **SQLite**: run SQLite shell benchmarks on comparable schema/queries.
- **RocksDB**: run `db_bench` with point‑lookup and write workloads.

If you want, I can add standardized benchmarks (e.g., `pgbench` and `db_bench`) and capture direct numbers for this machine.

## External Reference Numbers (Published)

These are published figures from each project’s own benchmarking or docs. They are **not apples‑to‑apples** with Datacave’s micro‑benchmarks.

- **PostgreSQL (pgbench example)**: documentation example shows `tps = 896.967014` with 10 clients, 1 thread, scaling factor 10, 1000 tx/client (TPC‑B‑like script).  
  Source: https://www.postgresql.org/docs/current/pgbench.html
- **SQLite (speedtest CPU cycles)**: SQLite reports CPU cycles for a ~30k‑statement workload; 3.6.1 used ~3.376 million cycles and 3.48.0 used ~0.965 million (cachegrind on Ubuntu 16.04, gcc 5.4.0).  
  Source: https://sqlite.org/cpu.html
- **RocksDB (db_bench bulkload)**: RocksDB wiki reports bulk‑load throughput for 900M keys; example row shows `ops/sec = 1,003,732` (version 7.2.2, non‑DIO).  
  Source: https://github.com/facebook/rocksdb/wiki/performance-benchmarks
