# Datacave

![Datacave logo](./datacave.png)

Datacave is a high-performance, distributed SQL database built in Rust from the ground up. It uses its own LSM-based storage engine and SQL layer; for easy connectivity it speaks the PostgreSQL wire protocol and SQL dialect, so you can use existing Postgres clients and tools while the storage and execution are entirely Datacave.

## Highlights

- LSM-backed storage for write-heavy workloads and efficient compactions
- PostgreSQL wire protocol support for easy client connectivity
- Modular Rust workspace with clean crate boundaries
- Focus on performance, observability, and correctness

## Architecture Overview

Datacave is organized as a set of focused crates that compose into a full database server:

- `datacave-core`: core types, errors, catalog, and shared traits
- `datacave-lsm`: LSM storage engine
- `datacave-sql`: SQL parsing, planning, and execution
- `datacave-protocol`: PostgreSQL wire protocol (client compatibility only; no Postgres server dependency)
- `datacave-server`: server binary and connection handling

Data flows from protocol handling to query planning and execution, then into the LSM engine for storage. This separation keeps the wire protocol, SQL layer, and storage engine independently testable and replaceable.

## Getting Started

### Prerequisites

- Rust toolchain (stable)
- A Postgres-compatible client (`psql`, DBeaver, or a language driver)

### Run the Server

```
cargo run -p datacave-server -- serve --config config.example.toml
```

### Connect with psql

```
psql "host=127.0.0.1 port=5433 user=admin dbname=default"
```

## Configuration

Datacave loads configuration from a TOML file. The `config.example.toml` file documents the available options and defaults. Typical settings include:

- Listen address and port
- Storage path and engine options
- Sharding and replication factor
- Metrics and health endpoints
- TLS and authentication settings

### Admin Commands

Generate a password hash for config:

```
cargo run -p datacave-server -- gen-password-hash --password "change-me"
```

## Observability

- Metrics: `GET /metrics` on the metrics listen address
- Health: `GET /health`
- Readiness: `GET /ready`

## SQL Compatibility Matrix

Supported today:
- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`

Not yet supported:
- Joins
- Aggregations
- Transactions

## Development

### Workspace Layout

This repository is a Rust workspace. Build, test, and run components using Cargo workspace commands.

### Build

```
cargo build --workspace
```

### Test

```
cargo test --workspace
```

### Benchmarks

```
cargo bench -p datacave-lsm
cargo bench -p datacave-sql
```

### Lint and Format

```
cargo fmt --all
cargo clippy --workspace --all-targets
```

## Security and Encryption

Datacave supports optional TLS, authentication, audit logging, and encryption at rest. Configure these in `config.example.toml`. The server uses cleartext password auth in a way that Postgres clients understand, and storage encryption applies to WAL and SSTable files.

## Roadmap

- Improved SQL coverage and query planning optimizations
- Advanced compaction and caching strategies
- Cluster membership and replication tooling
- Extended observability (metrics and tracing)

## Contributing

Issues and pull requests are welcome. Please keep changes scoped, add tests for new behavior, and follow the existing crate boundaries and conventions.

## License

See the repository license for details.
