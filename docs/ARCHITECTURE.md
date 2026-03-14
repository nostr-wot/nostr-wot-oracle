# Architecture

This document describes the internal architecture of WoT Oracle.

## Overview

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

## Components

### WotGraph (In-Memory Graph Store)

**Location:** `src/graph/store.rs`

The core data structure holding the follow graph in memory.

```rust
pub struct WotGraph {
    interner: PubkeyInterner,              // String deduplication
    pubkey_to_id: DashMap<Arc<str>, u32>,  // Pubkey → Node ID
    id_to_pubkey: RwLock<Vec<Arc<str>>>,   // Node ID → Pubkey
    follows: RwLock<Vec<Vec<u32>>>,        // Adjacency list (outgoing)
    followers: RwLock<Vec<Vec<u32>>>,      // Adjacency list (incoming)
    node_info: RwLock<Vec<Option<NodeInfo>>>, // Metadata per node
}
```

**Key Design Decisions:**

1. **Integer Node IDs:** Pubkeys are interned to `u32` IDs for memory efficiency (~4 bytes vs 64 bytes per reference).

2. **Sorted Adjacency Lists:** Follow lists are sorted `Vec<u32>` for O(log n) membership checks via binary search.

3. **Bidirectional Edges:** Both `follows` and `followers` are maintained for efficient bidirectional BFS.

4. **String Interning:** All pubkey strings use `Arc<str>` via `PubkeyInterner` to avoid duplicate allocations.

5. **parking_lot::RwLock:** Faster than std::sync::RwLock, no poisoning, fair scheduling.

### BFS Algorithm

**Location:** `src/graph/bfs.rs`

Bidirectional breadth-first search to find shortest paths.

```
Forward Search                    Backward Search
    (from)                            (to)
      │                                │
      ▼                                ▼
   Level 0                          Level 0
      │                                │
      ▼                                ▼
   Level 1 ◀────── Meeting Point ─────▶ Level 1
      │                                │
      ▼                                ▼
   Level 2                          Level 2
```

**Algorithm:**

1. Initialize forward frontier with source, backward frontier with target
2. Expand the smaller frontier (better pruning)
3. When frontiers meet, record meeting nodes and distance
4. Continue until `fwd_dist + bwd_dist > best_distance` or `max_hops` reached
5. Count paths by multiplying path counts at meeting points

**Complexity:** O(b^(d/2)) where b = average branching factor, d = distance

**Optimizations:**

- **Thread-local state:** `BfsState` is reused across queries (no allocation per query)
- **Double-buffered frontiers:** `Vec` swap instead of `VecDeque` for cache locality
- **Early termination:** Exit immediately when not collecting bridges
- **Entry API:** Single HashMap lookup instead of contains+insert

### Query Cache

**Location:** `src/cache.rs`

LRU cache with time-based expiration using Moka.

```rust
pub struct CacheKey {
    from_id: u32,
    to_id: u32,
    max_hops: u8,
    include_bridges: bool,
}
```

**Features:**

- **Compact Keys:** Uses node IDs (10 bytes) instead of pubkey strings (128 bytes)
- **TTL Expiration:** Configurable via `CACHE_TTL_SECS`
- **TTL-based expiration:** Entries expire after configured TTL; no automatic invalidation on graph changes
- **Lock-free reads:** Moka provides concurrent access without blocking

### Ingestion Daemon

**Location:** `src/sync/ingestion.rs`

Continuously syncs kind:3 (contact list) events from relays.

```
Relay A ──┐
Relay B ──┼──▶ Event Stream ──▶ Dedup ──▶ Graph Update ──▶ Persist Queue
Relay C ──┘                       │
                                  │
                            LRU Cache
                        (pubkey → latest event)
```

**Event Processing:**

1. **Early Dedup:** LRU cache keyed by pubkey bytes rejects already-seen events
2. **Tag Parsing:** Extract p-tags to get follow list
3. **Timestamp Check:** Only process if newer than existing event for pubkey
4. **Graph Update:** Diff old/new follows, update adjacency lists
5. **Async Persist:** Send to background worker for SQLite batching

**Deduplication:**

- LRU cache (100k entries) keyed by `[u8; 32]` pubkey bytes
- Stores `(created_at, event_id)` to detect older/duplicate events
- Checked before parsing tags (CPU-intensive)

> For the full sync process including relay discovery, multi-source event verification,
> batched subscriptions, and progressive depth crawling, see **[SYNC.md](SYNC.md)**.

### SQLite Persistence

**Location:** `src/db/sqlite.rs`

Durable storage for graph recovery on restart.

**Schema:**

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

**Optimizations:**

- **WAL Mode:** Better concurrent read/write performance
- **Prepared Statements:** Cached via `prepare_cached()`
- **Batch Writes:** `update_follows_batch()` commits multiple events in one transaction
- **Background Worker:** Writes don't block the ingestion loop

### HTTP API

**Location:** `src/api/http.rs`

Axum-based REST API.

**Middleware Stack:**

```
Request
   │
   ▼
┌──────────────────┐
│   CORS Layer     │  Allow cross-origin requests
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Rate Limiter    │  tower_governor (per-IP token bucket)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│     Router       │  /health, /stats, /distance, /distance/batch, /follows, /common-follows, /path
└────────┬─────────┘
         │
         ▼
Response
```

**Async/Blocking Separation:**

BFS is CPU-bound and would block the async runtime. Solution:

```rust
// CPU-bound work runs on blocking thread pool
let result = tokio::task::spawn_blocking(move || {
    bfs::compute_distance(&graph, &query)
}).await?;
```

### DVM Service

**Location:** `src/api/dvm.rs`

NIP-90 Data Vending Machine interface for Nostr-native queries.

**Protocol:**

1. Subscribe to kind:5950 events on relays
2. Parse request: `["i", "<from>"]`, `["param", "target", "<to>"]`
3. Compute distance
4. Publish kind:6950 response signed with DVM key

### Relay Discovery

**Status:** Not yet implemented — see [SYNC.md](SYNC.md) for the proposed strategy.

**Current:** Static relays configured via `RELAYS` env var (default: `damus.io`, `nos.lol`, `nostr.band`). All relays queried uniformly for all users.

**Planned:** Dynamic relay discovery using the outbox model:

1. **NIP-65 (kind:10002)** — Fetch user's declared read/write relays from `purplepag.es` or `relay.nostr.band`
2. **Kind:3 relay hints** — Extract relay URLs from p-tag position 2 (`["p", pubkey, relay_url, petname]`)
3. **Fallback** — Seed relays for users without relay metadata

This enables targeted subscriptions (fewer relays, more relevant data) and ensures we find events from users who publish to non-default relays.

## Data Flow

### Query Path

```
HTTP Request
     │
     ▼
┌─────────────┐
│  Validate   │  Check pubkey format, max_hops range
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Cache Check │  Lock-free Moka lookup
└─────┬───────┘
      │
      ├─── Cache Hit ───▶ Return cached result
      │
      ▼
┌─────────────┐
│spawn_blocking│  Move to thread pool
└─────┬───────┘
      │
      ▼
┌─────────────┐
│  BFS Search │  Bidirectional traversal
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Cache Insert│  Store result for future queries
└─────┬───────┘
      │
      ▼
HTTP Response
```

### Ingestion Path

```
Relay Event
     │
     ▼
┌─────────────┐
│ Dedup Check │  LRU cache by pubkey
└─────┬───────┘
      │
      ├─── Already seen ───▶ Skip
      │
      ▼
┌─────────────┐
│ Parse Tags  │  Extract p-tags
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Graph Update│  Diff + update adjacency lists
└─────┬───────┘
      │
      ├─── Timestamp older ───▶ Skip
      │
      ▼
┌─────────────┐
│ Persist Queue│  Send to background worker
└─────────────┘
      │
      ▼
┌─────────────┐
│ Batch Write │  SQLite transaction (100 events)
└─────────────┘
```

## Memory Layout

```
WotGraph Memory:
┌────────────────────────────────────────┐
│ pubkey_to_id: DashMap                  │  ~80 bytes/entry
│   Arc<str> (24) + u32 (4) + overhead   │
├────────────────────────────────────────┤
│ id_to_pubkey: Vec<Arc<str>>            │  ~24 bytes/entry
│   Arc pointer                          │
├────────────────────────────────────────┤
│ follows: Vec<Vec<u32>>                 │  ~4 bytes/edge
│   Sorted node IDs                      │
├────────────────────────────────────────┤
│ followers: Vec<Vec<u32>>               │  ~4 bytes/edge
│   Sorted node IDs (reverse index)      │
├────────────────────────────────────────┤
│ interner: DashMap<Arc<str>, ()>        │  String storage
│   Actual pubkey bytes (64 chars)       │
└────────────────────────────────────────┘

Approximate total: ~100 bytes/node + ~8 bytes/edge
1M nodes, 10M edges ≈ 180MB
```

## Concurrency Model

```
┌─────────────────────────────────────────────────────────────┐
│                      Tokio Runtime                          │
├─────────────────────────────────────────────────────────────┤
│  Async Tasks:                                               │
│  - HTTP request handlers                                    │
│  - Relay WebSocket connections                              │
│  - DVM event subscription                                   │
│  - Persistence worker (batching)                            │
├─────────────────────────────────────────────────────────────┤
│  Blocking Thread Pool (spawn_blocking):                     │
│  - BFS computation                                          │
│  - Heavy CPU work                                           │
├─────────────────────────────────────────────────────────────┤
│  Synchronization:                                           │
│  - DashMap: lock-free pubkey lookups                        │
│  - parking_lot::RwLock: adjacency list access               │
│  - Moka cache: lock-free with internal sharding             │
│  - tokio::sync::RwLock: dedup LRU cache                     │
└─────────────────────────────────────────────────────────────┘
```

## Performance Characteristics

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| Cache lookup | O(1) | <1ms |
| BFS (cached) | O(1) | <1ms |
| BFS (uncached, 2 hops) | O(b²) | 1-10ms |
| BFS (uncached, 5 hops) | O(b^2.5) | 10-50ms |
| Graph update | O(k log k) | <1ms |
| Batch persist | O(n) | 10-100ms |

Where b = average branching factor (~100-1000 follows), k = follow count
