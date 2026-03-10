FROM rust:1.92-bookworm AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY README.md Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --package energyhistoriand

EXPOSE 8080

CMD ["cargo", "run", "--package", "energyhistoriand", "--", "--listen-addr", "0.0.0.0:8080"]
