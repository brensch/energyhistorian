FROM rust:1.92-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/energyhistorian /usr/local/bin/energyhistorian

RUN mkdir -p /data

EXPOSE 8080

CMD ["energyhistorian", "--data-dir", "/data", "--listen-addr", "0.0.0.0:8080"]
