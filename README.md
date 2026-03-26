# WoT Oracle

[![Build](https://github.com/nostr-wot/nostr-wot-oracle/actions/workflows/ci.yml/badge.svg?job=build)](https://github.com/nostr-wot/nostr-wot-oracle/actions/workflows/ci.yml)
[![Tests](https://github.com/nostr-wot/nostr-wot-oracle/actions/workflows/ci.yml/badge.svg?job=test)](https://github.com/nostr-wot/nostr-wot-oracle/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/nostr-wot/nostr-wot-oracle/branch/main/graph/badge.svg)](https://codecov.io/gh/nostr-wot/nostr-wot-oracle)

A high-performance Nostr Web of Trust oracle that indexes the global follow graph and provides pairwise distance queries between pubkeys.

## What It Does

WoT Oracle continuously syncs follow lists (kind:3 events) from Nostr relays and builds an in-memory graph. You can then query the "social distance" between any two pubkeys - how many hops through the follow graph connect them. It also caches kind:0 profile metadata (name, picture, NIP-05, etc.) and can return cached profiles alongside distance queries via the `include_profiles` parameter or the dedicated `/profiles` endpoint.

**Example:** If Alice follows Bob, and Bob follows Carol, then the distance from Alice to Carol is 2 hops.

## Quick Start

### Using Pre-built Docker Image (Recommended)

```bash
# Pull and run
docker pull ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1
docker run -d -p 8080:8080 -v wot-data:/app/data ghcr.io/nostr-wot/nostr-wot-oracle:0.2.1

# Check health
curl http://localhost:8080/health
```

### Using Docker Compose

```bash
# Clone and start
git clone https://github.com/nostr-wot/nostr-wot-oracle.git
cd nostr-wot-oracle
docker-compose up -d

# Check health
curl http://localhost:8080/health
```

### Building from Source

```bash
# Requires Rust 1.75+
cargo build --release

# Run with default settings
./target/release/wot-oracle

# Or with custom relays
RELAYS=wss://relay.mappingbitcoin.com,wss://relay.damus.io,wss://nos.lol ./target/release/wot-oracle
```

## API Usage

### Get Distance Between Two Pubkeys

```bash
curl "http://localhost:8080/distance?from=PUBKEY1&to=PUBKEY2"
```

Response:
```json
{
  "from": "abc123...",
  "to": "def456...",
  "hops": 2,
  "path_count": 3,
  "mutual_follow": false,
  "bridges": ["bridge1...", "bridge2..."]
}
```

Include cached profile metadata in the response:

```bash
curl "http://localhost:8080/distance?from=PUBKEY1&to=PUBKEY2&include_profiles=true"
```

### Batch Query

```bash
curl -X POST http://localhost:8080/distance/batch \
  -H "Content-Type: application/json" \
  -d '{"from": "PUBKEY1", "targets": ["PUBKEY2", "PUBKEY3"]}'
```

### Get Follows

```bash
curl "http://localhost:8080/follows?pubkey=PUBKEY"
```

### Get Common Follows

```bash
curl "http://localhost:8080/common-follows?from=PUBKEY1&to=PUBKEY2"
```

### Get Shortest Path

```bash
curl "http://localhost:8080/path?from=PUBKEY1&to=PUBKEY2"
```

### Get Profiles

```bash
curl "http://localhost:8080/profiles?pubkeys=PUBKEY1,PUBKEY2"
```

### Graph Stats

```bash
curl http://localhost:8080/stats
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAYS` | damus, nos.lol, nostr.band | Comma-separated relay URLs |
| `HTTP_PORT` | 8080 | HTTP server port |
| `DB_PATH` | wot.db | SQLite database path |
| `RATE_LIMIT_PER_MINUTE` | 100 | Per-IP rate limit |
| `MAX_HOPS` | 3 | Default max hops for distance queries (1-5) |
| `CACHE_SIZE` | 10000 | LRU cache entries |
| `CACHE_TTL_SECS` | 300 | Cache TTL (5 min) |
| `PROFILE_CACHE_SIZE` | 50000 | Profile cache LRU entries |
| `PROFILE_CACHE_TTL_SECS` | 3600 | Profile cache TTL (1 hour) |

See [.env.example](.env.example) for all options.

## Documentation

- [API Reference](docs/API.md) - Full REST API documentation
- [DVM Interface](docs/DVM.md) - NIP-90 Nostr integration
- [Self-Hosting Guide](docs/SELF-HOST.md) - Docker deployment guide
- [Architecture](docs/ARCHITECTURE.md) - How it works internally
- [Sync & Relay Discovery](docs/SYNC.md) - Sync process and relay discovery strategy

## Performance

- **BFS Algorithm:** Bidirectional breadth-first search with O(b^(d/2)) complexity
- **Memory:** ~100 bytes per node, ~8 bytes per edge
- **Latency:** Sub-millisecond for cached queries, <50ms for uncached
- **Throughput:** 10,000+ queries/second on modern hardware

## References

[nostr-wot.com](https://nostr-wot.com)

## License

MIT
