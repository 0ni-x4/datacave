FROM rust:1.75 as builder

WORKDIR /app
COPY . .
RUN cargo build --release -p datacave-server

FROM debian:bookworm-slim

RUN useradd -m datacave
WORKDIR /home/datacave
COPY --from=builder /app/target/release/datacave-server /usr/local/bin/datacave-server
COPY config.example.toml /home/datacave/config.toml
RUN chown -R datacave:datacave /home/datacave
USER datacave

EXPOSE 5433 9898
CMD ["datacave-server", "--config", "/home/datacave/config.toml"]
