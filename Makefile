BUILD_FLAGS :=

.PHONY: build test lint bench run

build:
	cargo build --workspace $(BUILD_FLAGS)

test:
	cargo test --workspace $(BUILD_FLAGS)

lint:
	cargo fmt --all
	cargo clippy --workspace --all-targets -- -D warnings

bench:
	cargo bench -p datacave-lsm
	cargo bench -p datacave-sql

run:
	cargo run -p datacave-server -- serve --config config.example.toml
