# Nostr WoT Pairwise Distance Oracle

## Business Requirements Document (BRD)

---

## 1. Executive Summary

### Product Name
**WoT Oracle**

### Problem Statement
No service exists that answers the simple question: "How many hops separate me from another Nostr user?" Existing WoT implementations are ego-centric (showing your entire trust network) or compute abstract scores. Users need a lightweight, pairwise query to make trust decisions in real-time.

### Solution
A global follow-graph indexer that answers pairwise distance queries between any two Nostr pubkeys, returning hop count, path count, and bridging nodes.

### Primary Use Cases
1. **Merchant verification** - "Is this Bitcoin merchant in my network?"
2. **DM filtering** - "Should I see this message from a stranger?"
3. **Content discovery** - "How connected am I to this note's author?"
4. **Relay access control** - "Is this pubkey within N hops of our seed?"

---

## 2. Functional Requirements

### 2.1 Core Query API

**Endpoint**: `GET /distance`

**Input**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `from` | hex pubkey | Yes | - | The observer/viewer pubkey |
| `to` | hex pubkey | Yes | - | The target pubkey |
| `max_hops` | integer | No | 3 | Maximum depth to search (1-5) |
| `include_bridges` | boolean | No | false | Return intermediate pubkeys |
| `bypass_cache` | boolean | No | false | Skip cache, force fresh computation |

**Output**:
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "to": "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  "hops": 2,
  "path_count": 7,
  "mutual_follow": false,
  "bridges": ["fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"]
}
```

**Response Fields**:
| Field | Type | Description |
|-------|------|-------------|
| `hops` | integer \| null | Shortest path length (null if not connected within max_hops) |
| `path_count` | integer | Number of distinct shortest paths |
| `bridges` | string[] \| null | Pubkeys where forward/backward searches meet (if `include_bridges=true`) |
| `mutual_follow` | boolean | True if both follow each other |

### 2.2 Batch Query API

**Endpoint**: `POST /distance/batch`

**Input**:
```json
{
  "from": "82341f882b6eabcd...",
  "targets": ["3bf0c63fcb93463...", "fa984bd7dbb282f..."],
  "max_hops": 5,
  "include_bridges": false,
  "bypass_cache": false
}
```

**Limits**: Maximum 100 targets per batch request.

**Output**:
```json
{
  "from": "82341f882b6eabcd...",
  "results": [
    { "from": "82341f...", "to": "3bf0c6...", "hops": 2, "path_count": 4, "mutual_follow": false },
    { "from": "82341f...", "to": "fa984b...", "hops": 1, "path_count": 1, "mutual_follow": true }
  ]
}
```

### 2.3 DVM Interface (NIP-90)

**Request Event (kind 5950)**:
```json
{
  "kind": 5950,
  "tags": [
    ["i", "<from_pubkey>", "text"],
    ["param", "target", "<to_pubkey>"],
    ["param", "max_hops", "3"]
  ],
  "content": ""
}
```

**Response Event (kind 6950)**:
```json
{
  "kind": 6950,
  "tags": [
    ["e", "<request_event_id>"],
    ["p", "<requester>"],
    ["result", "hops", "2"],
    ["result", "path_count", "7"],
    ["result", "mutual_follow", "false"]
  ],
  "content": "{\"from\":\"...\",\"to\":\"...\",\"hops\":2,...}"
}
```

### 2.4 Statistics Endpoint

**Endpoint**: `GET /stats`

**Output**:
```json
{
  "node_count": 847293,
  "edge_count": 52847102,
  "nodes_with_follows": 423000,
  "cache": {
    "size": 5432,
    "hits": 123456,
    "misses": 7890
  },
  "locks": {
    "read_count": 1000000,
    "write_count": 50000,
    "read_wait_ns": 12345678,
    "write_wait_ns": 987654
  }
}
```

### 2.5 Health Endpoint

**Endpoint**: `GET /health`

**Output**:
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

---

## 3. Non-Functional Requirements

### 3.1 Performance

| Metric | Target |
|--------|--------|
| Single query latency (cached) | < 1ms |
| Single query latency (uncached, p50) | < 20ms |
| Single query latency (uncached, p99) | < 100ms |
| Batch query (100 targets) | < 500ms |
| Queries per second | > 10,000 |

### 3.2 Scalability

| Metric | Initial | Target |
|--------|---------|--------|
| Indexed pubkeys | 500k | 2M |
| Indexed edges | 50M | 200M |
| Memory usage | 1 GB | 4 GB |
| Storage (SQLite) | 1 GB | 10 GB |

### 3.3 Availability

- Target uptime: 99%
- Graceful degradation: Return cached/stale results if sync is behind
- Health endpoint for load balancer integration

### 3.4 Security

- Per-IP rate limiting: 100 requests/minute (token bucket)
- Input validation: 64-char hex pubkeys, max_hops 1-5
- No sensitive data stored (all data is public kind:3 events)

---

## 4. Technical Architecture

### 4.1 System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         WoT Oracle                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Ingestion  │───▶│   WotGraph   │◀───│   HTTP API   │      │
│  │    Daemon    │    │  (In-Memory) │    │    (Axum)    │      │
│  └──────┬───────┘    └──────────────┘    └──────────────┘      │
│         │                   │                    │              │
│         │            ┌──────┴──────┐            │              │
│         │            │    Cache    │            │              │
│         │            │   (Moka)    │            │              │
│         │            └─────────────┘            │              │
│         │                                       │              │
│  ┌──────▼───────┐                      ┌───────▼──────┐       │
│  │    SQLite    │                      │  DVM Service │       │
│  │  (Persist)   │                      │   (NIP-90)   │       │
│  └──────────────┘                      └──────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                                        │
         ▼                                        ▼
   ┌──────────┐                            ┌──────────┐
   │  Relays  │                            │  Relays  │
   └──────────┘                            └──────────┘
```

### 4.2 Component Details

#### Graph Store (`src/graph/store.rs`)

```rust
pub struct WotGraph {
    interner: PubkeyInterner,              // String deduplication
    pubkey_to_id: DashMap<Arc<str>, u32>,  // Lock-free pubkey → ID
    id_to_pubkey: RwLock<Vec<Arc<str>>>,   // ID → pubkey
    follows: RwLock<Vec<Vec<u32>>>,        // Sorted adjacency (outgoing)
    followers: RwLock<Vec<Vec<u32>>>,      // Sorted adjacency (incoming)
    node_info: RwLock<Vec<Option<NodeInfo>>>,
    lock_metrics: LockMetrics,             // Contention tracking
}
```

**Key Design Decisions**:
- **Integer Node IDs**: Pubkeys interned to `u32` for memory efficiency
- **Sorted Adjacency Lists**: `Vec<u32>` with binary search for O(log n) membership
- **String Interning**: All pubkeys use `Arc<str>` via `PubkeyInterner`
- **parking_lot::RwLock**: Faster than std, no poisoning, fair scheduling

#### BFS Algorithm (`src/graph/bfs.rs`)

**Bidirectional BFS** with O(b^(d/2)) complexity:
- Expand smaller frontier for better pruning
- Track path counts at each visited node
- Collect meeting points as bridge nodes
- Early termination when not collecting bridges

**Optimizations**:
- Thread-local `BfsState` with preallocated structures (zero allocation per query)
- Double-buffered frontiers (`Vec` swap vs `VecDeque`)
- Entry API for single HashMap lookup
- Reusable HashSet/Vec for bridge deduplication

#### Query Cache (`src/cache.rs`)

```rust
pub struct CacheKey {
    from_id: u32,      // Compact: 4 bytes vs 64-char string
    to_id: u32,
    max_hops: u8,
    include_bridges: bool,
}
```

**Features**:
- Moka LRU cache with TTL expiration
- Lock-free concurrent access
- Epoch-based invalidation on graph updates
- Configurable size and TTL

#### Ingestion Daemon (`src/sync/ingestion.rs`)

**Event Processing**:
1. Early dedup via LRU cache keyed by `[u8; 32]` pubkey bytes
2. Parse p-tags to extract follow list
3. Timestamp check (only process newer events)
4. Diff-based graph update (only changed edges)
5. Async batch persistence to SQLite

#### HTTP API (`src/api/http.rs`)

**Middleware Stack**:
- CORS (allow all origins)
- Rate limiting via `tower_governor` (per-IP token bucket)
- Request validation

**Async/Blocking Separation**:
```rust
// CPU-bound BFS runs on blocking thread pool
let result = tokio::task::spawn_blocking(move || {
    bfs::compute_distance(&graph, &query)
}).await?;
```

### 4.3 Data Model

#### SQLite Schema

```sql
CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    pubkey TEXT UNIQUE NOT NULL,
    kind3_event_id TEXT,
    kind3_created_at INTEGER,
    updated_at INTEGER
);

CREATE TABLE edges (
    follower_id INTEGER NOT NULL,
    followed_id INTEGER NOT NULL,
    PRIMARY KEY (follower_id, followed_id)
);

CREATE TABLE sync_state (
    relay_url TEXT PRIMARY KEY,
    last_event_time INTEGER,
    last_sync_at INTEGER
);
```

#### Memory Layout

```
Approximate per-node: ~100 bytes
Approximate per-edge: ~8 bytes (4 bytes each in follows + followers)

Example: 1M nodes, 10M edges ≈ 180MB
```

---

## 5. Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAYS` | damus, nos.lol, nostr.band | Comma-separated relay URLs |
| `HTTP_PORT` | 8080 | HTTP server port |
| `DB_PATH` | wot.db | SQLite database path |
| `RATE_LIMIT_PER_MINUTE` | 100 | Per-IP rate limit |
| `MAX_HOPS` | 3 | Default max hops |
| `CACHE_SIZE` | 10000 | LRU cache entries |
| `CACHE_TTL_SECS` | 300 | Cache TTL (5 min) |
| `DVM_ENABLED` | false | Enable NIP-90 DVM |
| `DVM_PRIVATE_KEY` | - | DVM signing key |
| `RUST_LOG` | info | Log level |

---

## 6. Implementation Status

### Completed Features

- [x] In-memory graph with `Arc<str>` interning and sorted adjacency lists
- [x] Bidirectional BFS with thread-local preallocated state
- [x] Moka LRU cache with TTL-based expiration
- [x] SQLite persistence with WAL mode and batch writes
- [x] Multi-relay ingestion with LRU deduplication
- [x] HTTP REST API: `/distance`, `/distance/batch`, `/stats`, `/health`, `/follows`, `/common-follows`, `/path`
- [x] Per-IP rate limiting via `tower_governor`
- [x] `spawn_blocking` for CPU-bound BFS (async-friendly)
- [x] DVM interface (kind 5950/6950)
- [x] Docker deployment (multi-stage build)
- [x] Graceful shutdown handling
- [x] Comprehensive test suite

### Performance Optimizations

- [x] `parking_lot::RwLock` (faster than std)
- [x] `Arc<str>` throughout (zero-copy responses)
- [x] Entry API (single HashMap lookup)
- [x] Sorted `Vec<u32>` with binary search
- [x] Thread-local BFS state (allocation-free hot path)
- [x] Reusable bridge HashSet/Vec
- [x] Compact cache keys (u32 IDs vs strings)

### Not Yet Implemented

- [ ] Negentropy sync (NIP-77)
- [ ] Prometheus metrics endpoint
- [ ] Bloom filters for fast "not connected"
- [ ] NIP-98 authentication
- [ ] On-demand pubkey refresh

---

## 7. Project Structure

```
wot-oracle/
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── README.md
├── docs/
│   ├── API.md
│   ├── ARCHITECTURE.md
│   ├── SELF-HOST.md
│   └── BRD.md
└── src/
    ├── main.rs              # Entry point
    ├── config.rs            # Environment config
    ├── cache.rs             # Moka LRU cache
    ├── graph/
    │   ├── mod.rs
    │   ├── store.rs         # WotGraph with DashMap + RwLock
    │   ├── bfs.rs           # Bidirectional BFS
    │   ├── interner.rs      # Arc<str> string interning
    │   └── metrics.rs       # Lock contention tracking
    ├── sync/
    │   ├── mod.rs
    │   └── ingestion.rs     # Relay sync + dedup
    ├── db/
    │   ├── mod.rs
    │   └── sqlite.rs        # Persistence layer
    └── api/
        ├── mod.rs
        ├── http.rs          # Axum REST API
        └── dvm.rs           # NIP-90 DVM
```

---

## 8. Future Enhancements

1. **Negentropy Sync**: NIP-77 for efficient catch-up sync
2. **Prometheus Metrics**: `/metrics` endpoint for monitoring
3. **Bloom Filters**: Fast "not connected" responses
4. **WebSocket API**: Real-time distance updates
5. **NIP-85 Publishing**: Publish scores as trusted assertions
6. **Client Libraries**: TypeScript and Rust SDKs

---

## 9. Open Questions

1. Should edges be weighted by interaction (zaps, replies)?
2. How to handle mute lists (kind:10000) as negative edges?
3. Should bridges be limited to top N by follower count?
4. Pricing model for high-volume API access?

---

*Document Version: 1.0*
*Last Updated: 2026-02-02*
