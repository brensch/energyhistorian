FROM rust:1.92-bookworm AS builder
WORKDIR /app

COPY README.md Cargo.toml Cargo.lock ./
COPY apps ./apps
COPY libs ./libs

RUN cargo build --release --locked \
    --package reconcile-semantics \
    --package downloaderd \
    --package parserd \
    --package schedulerd

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/downloaderd /usr/local/bin/downloaderd
COPY --from=builder /app/target/release/parserd /usr/local/bin/parserd
COPY --from=builder /app/target/release/reconcile-semantics /usr/local/bin/reconcile-semantics
COPY --from=builder /app/target/release/schedulerd /usr/local/bin/schedulerd

RUN mkdir -p /data

EXPOSE 8080

CMD ["/usr/local/bin/schedulerd", "--listen-addr", "0.0.0.0:8080"]
