FROM rust:1.89-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY config ./config

RUN cargo build --release

FROM debian:bookworm-slim

RUN useradd --system --uid 10001 --create-home appuser

WORKDIR /app

COPY --from=builder /app/target/release/rust-proxy /usr/local/bin/rust-proxy
COPY --from=builder /app/config ./config

USER appuser

EXPOSE 8080

CMD ["rust-proxy"]