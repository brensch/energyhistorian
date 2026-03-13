FROM rust:1.92-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates
COPY apps ./apps

RUN cargo build --release --locked -p ai-api

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ai-api /usr/local/bin/ai-api

EXPOSE 8090

CMD ["ai-api"]
