FROM rust:1.92-bookworm AS builder
WORKDIR /app

COPY README.md Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked --package energyhistoriand

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/energyhistoriand /usr/local/bin/energyhistoriand

RUN mkdir -p /data

EXPOSE 8080

CMD ["/usr/local/bin/energyhistoriand", "--listen-addr", "0.0.0.0:8080", "--state-db", "/data/control-plane.sqlite"]
