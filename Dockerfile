# Build stage
FROM rust:1.85-slim-bookworm AS builder

LABEL org.opencontainers.image.source="https://github.com/nostr-wot/nostr-wot-oracle"
LABEL org.opencontainers.image.description="Pairwise distance queries for Nostr Web of Trust"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock* ./

# Create dummy main.rs to cache dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy actual source code
COPY src ./src

# Build the application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/wot-oracle /app/wot-oracle

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV DB_PATH=/app/data/wot.db
ENV HTTP_PORT=8080
ENV RUST_LOG=info

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["/app/wot-oracle"]
