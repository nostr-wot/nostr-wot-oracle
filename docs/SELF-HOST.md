# Self-Hosting Guide

This guide covers deploying WoT Oracle using Docker.

## Requirements

- Docker 20.10+
- Docker Compose 2.0+
- 1GB RAM minimum (2GB+ recommended for large graphs)
- Persistent storage for SQLite database

## Quick Start

### Option 1: Pre-built Image (Recommended)

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1

# Run with default settings
docker run -d \
  --name wot-oracle \
  -p 8080:8080 \
  -v wot-data:/app/data \
  ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1

# Run with custom configuration
docker run -d \
  --name wot-oracle \
  -p 8080:8080 \
  -v wot-data:/app/data \
  -e RELAYS=wss://relay.mappingbitcoin.com,wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band \
  -e CACHE_SIZE=20000 \
  -e RUST_LOG=debug \
  ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1

# Verify it's running
curl http://localhost:8080/health
```

### Option 2: Docker Compose

```bash
# Clone the repository
git clone https://github.com/nostr-wot/nostr-wot-oracle.git
cd nostr-wot-oracle

# Copy example environment file
cp .env.example .env

# Start the service
docker-compose up -d

# Check logs
docker-compose logs -f

# Verify it's running
curl http://localhost:8080/health
```

## Configuration

Edit `.env` or pass environment variables to docker-compose:

```bash
# .env file
RELAYS=wss://relay.mappingbitcoin.com,wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
HTTP_PORT=8080
RATE_LIMIT_PER_MINUTE=100
CACHE_SIZE=10000
CACHE_TTL_SECS=300
RUST_LOG=info
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAYS` | damus, nos.lol, nostr.band | Comma-separated Nostr relay WebSocket URLs |
| `HTTP_PORT` | 8080 | Port to expose the HTTP API |
| `DB_PATH` | /app/data/wot.db | SQLite database path (inside container) |
| `RATE_LIMIT_PER_MINUTE` | 100 | Max requests per IP per minute |
| `CACHE_SIZE` | 10000 | Number of query results to cache |
| `CACHE_TTL_SECS` | 300 | Cache entry lifetime in seconds |
| `MAX_HOPS` | 5 | Default max hops for queries |
| `DVM_ENABLED` | false | Enable NIP-90 DVM interface |
| `DVM_PRIVATE_KEY` | - | DVM signing key (nsec or hex) |
| `RUST_LOG` | info | Log level (trace/debug/info/warn/error) |

## Docker Compose

The default `docker-compose.yml`:

```yaml
version: '3.8'

services:
  nostr-wot-oracle:
    image: ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1
    # Or build from source:
    # build: .
    container_name: nostr-wot-oracle
    restart: unless-stopped
    ports:
      - "${HTTP_PORT:-8080}:8080"
    volumes:
      - nostr-wot-data:/app/data
    environment:
      - RELAYS=${RELAYS:-wss://relay.mappingbitcoin.com,wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band}
      - HTTP_PORT=8080
      - DB_PATH=/app/data/wot.db
      - DVM_ENABLED=${DVM_ENABLED:-false}
      - DVM_PRIVATE_KEY=${DVM_PRIVATE_KEY:-}
      - RATE_LIMIT_PER_MINUTE=${RATE_LIMIT_PER_MINUTE:-100}
      - MAX_HOPS=${MAX_HOPS:-3}
      - CACHE_SIZE=${CACHE_SIZE:-10000}
      - CACHE_TTL_SECS=${CACHE_TTL_SECS:-300}
      - RUST_LOG=${RUST_LOG:-info}
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s

volumes:
  nostr-wot-data:
```

## Production Deployment

### Behind a Reverse Proxy (nginx)

```nginx
upstream wot_oracle {
    server 127.0.0.1:8080;
    keepalive 32;
}

server {
    listen 443 ssl http2;
    server_name wot.example.com;

    ssl_certificate /etc/letsencrypt/live/wot.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/wot.example.com/privkey.pem;

    location / {
        proxy_pass http://wot_oracle;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Important for rate limiting to work correctly
        proxy_set_header X-Forwarded-For $remote_addr;
    }
}
```

### With Traefik

```yaml
services:
  wot-oracle:
    # ... (same as above)
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.wot.rule=Host(`wot.example.com`)"
      - "traefik.http.routers.wot.tls.certresolver=letsencrypt"
      - "traefik.http.services.wot.loadbalancer.server.port=8080"
```

## Resource Sizing

### Memory

| Graph Size | Recommended RAM |
|------------|-----------------|
| < 100k nodes | 512MB |
| 100k - 500k nodes | 1GB |
| 500k - 1M nodes | 2GB |
| > 1M nodes | 4GB+ |

### CPU

- 1 core minimum
- 2+ cores recommended for concurrent queries
- BFS queries are CPU-bound and run on a thread pool

### Storage

- SQLite database grows ~100 bytes per node + ~16 bytes per edge
- 1M nodes with 10M edges: ~1GB database
- Enable WAL mode (default) for better write performance

## Monitoring

### Health Check

```bash
curl http://localhost:8080/health
```

### Statistics

```bash
curl http://localhost:8080/stats
```

Returns:
- `node_count` - Total pubkeys indexed
- `edge_count` - Total follow relationships
- `cache.hits/misses` - Cache performance
- `locks.read_wait_ns` - Lock contention metrics

### Logs

```bash
# View logs
docker-compose logs -f wot-oracle

# Increase verbosity
RUST_LOG=debug docker-compose up
```

Log levels:
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Sync progress, startup info (default)
- `debug` - Query details, cache hits/misses
- `trace` - Everything (very verbose)

## Backup & Restore

### Backup

```bash
# Stop the service (optional, for consistency)
docker-compose stop

# Copy the database
docker cp wot-oracle:/app/data/wot.db ./backup-$(date +%Y%m%d).db

# Restart
docker-compose start
```

### Restore

```bash
docker-compose stop
docker cp ./backup.db wot-oracle:/app/data/wot.db
docker-compose start
```

## Troubleshooting

### Service won't start

```bash
# Check logs
docker-compose logs wot-oracle

# Common issues:
# - Port already in use: change HTTP_PORT
# - Permission denied on volume: check Docker volume permissions
```

### High memory usage

- Reduce `CACHE_SIZE`
- The graph itself is memory-resident; size scales with indexed pubkeys

### Slow queries

- Check `/stats` for cache hit rate
- Increase `CACHE_SIZE` or `CACHE_TTL_SECS`
- Ensure queries use reasonable `max_hops` (lower = faster)

### Rate limit errors (429)

- Increase `RATE_LIMIT_PER_MINUTE`
- Or implement client-side rate limiting

### No data / empty graph

- Check relay connectivity in logs
- Ensure relays have kind:3 events
- Initial sync can take several minutes
