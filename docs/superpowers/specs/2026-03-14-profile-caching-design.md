# Profile Caching Design Spec

## Summary

Add kind:0 (profile metadata) caching to WoT Oracle. Profiles are ingested from relays and batch-fetched from `purplepag.es`, stored in SQLite with a Moka LRU cache for hot reads, and optionally returned alongside all API responses via `include_profiles=true`. A new `/profiles` endpoint provides direct profile lookups.

## Motivation

Clients querying WoT distance often need to display the users involved (source, target, bridges). Currently they must make separate requests to fetch profiles. By caching profiles server-side and optionally including them in responses, we eliminate extra round-trips and provide a better developer experience.

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                      Profile System                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │ Profile Ingestion│───▶│  Profile Store   │              │
│  │  (kind:0 events) │    │  (Moka + SQLite) │              │
│  └──────────────────┘    └────────┬─────────┘              │
│                                   │                         │
│  ┌──────────────────┐    ┌────────▼─────────┐              │
│  │ Batch Fetcher    │───▶│  Profile Cache   │              │
│  │ (purplepag.es)   │    │  (Moka LRU)      │              │
│  └──────────────────┘    └────────┬─────────┘              │
│                                   │                         │
│                          ┌────────▼─────────┐              │
│                          │   HTTP API        │              │
│                          │  (include_profiles│              │
│                          │   + /profiles)    │              │
│                          └──────────────────┘              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Profile Ingestion

**Source 1: Relay subscription**

Extend the existing ingestion daemon to also subscribe to `kind:0` events:

```rust
// Current: only kind:3
let filter = Filter::new().kind(Kind::ContactList);

// New: kind:3 + kind:0
let filters = vec![
    Filter::new().kind(Kind::ContactList),
    Filter::new().kind(Kind::Metadata),
];
```

Processing:
1. Early dedup via LRU cache (same pattern as kind:3, separate cache for kind:0)
2. Parse event: `content` field is JSON with profile data
3. Timestamp check: only accept if `created_at` > stored `created_at`
4. Store to SQLite via batch persistence queue
5. Insert/update Moka cache

**Source 2: Batch fetch from purplepag.es**

A dedicated background task fetches profiles for pubkeys discovered via kind:3 ingestion.

**Queue structure:** A `DashSet<[u8; 32]>` shared between the ingestion loop and the batch fetcher. When a kind:3 event introduces a new pubkey (not in Moka cache and not in the DashSet already), the pubkey bytes are inserted into the set.

**Background task lifecycle:**

```rust
// Spawned once at startup, owns its own nostr_sdk::Client
async fn profile_fetch_worker(
    queue: Arc<DashSet<[u8; 32]>>,
    discovery_relays: Vec<String>,  // e.g., ["wss://purplepag.es"]
    profile_cache: Arc<ProfileCache>,
    db: Arc<Database>,
    persist_tx: mpsc::Sender<PersistUpdate>,
    config: ProfileFetchConfig,
) {
    let client = Client::default();
    for relay in &discovery_relays {
        client.add_relay(relay).await.ok();
    }
    client.connect().await;

    loop {
        tokio::time::sleep(Duration::from_secs(config.interval_secs)).await;

        // Drain up to batch_size pubkeys from queue
        let batch: Vec<PublicKey> = queue.iter()
            .take(config.batch_size)
            .map(|pk| PublicKey::from_bytes(*pk))
            .collect();

        if batch.is_empty() { continue; }

        // Remove from queue before fetching (re-queued on failure if needed)
        for pk in &batch {
            queue.remove(&pk.to_bytes());
        }

        // One-shot REQ: subscribe, collect until EOSE, then unsubscribe
        let filter = Filter::new()
            .kind(Kind::Metadata)
            .authors(batch);

        // Subscribe and collect events
        let sub_id = client.subscribe(vec![filter], None).await?;
        // Process events from notifications until EOSE or timeout
        // On EOSE or timeout (10s): unsubscribe
        client.unsubscribe(sub_id).await;

        // Each received kind:0 event goes through the same dedup/persist pipeline
    }
}
```

**Key details:**
- Batch size: up to 500 authors per subscription (configurable via `PROFILE_FETCH_BATCH_SIZE`, bounds: `10..=1000`)
- Interval: configurable via `PROFILE_FETCH_INTERVAL_SECS` (default 60s, bounds: `10..=3600`)
- Staleness: profiles older than `PROFILE_STALE_SECS` (default 86400 / 24h, bounds: `3600..=604800`) are re-queued for refresh
- Creates its own `nostr_sdk::Client` with only discovery relays (separate from ingestion client)
- Uses one-shot REQ pattern: subscribe → collect until EOSE → unsubscribe
- Deduplication: shares the same `seen_profiles` LRU with the relay subscription path (both sources check the same cache before processing)

### Storage

#### SQLite Schema

```sql
CREATE TABLE IF NOT EXISTS profiles (
    pubkey TEXT PRIMARY KEY,
    content TEXT NOT NULL,           -- Raw kind:0 JSON content
    event_id TEXT,                   -- kind:0 event ID for provenance
    created_at INTEGER NOT NULL,     -- Event created_at timestamp
    updated_at INTEGER NOT NULL      -- When we last wrote this row
);

CREATE INDEX IF NOT EXISTS idx_profiles_updated ON profiles(updated_at);
```

The `content` column stores the raw JSON string from the kind:0 event's `content` field. This preserves all fields without needing schema changes when new NIP-defined fields appear.

#### Moka Cache

**File:** New file `src/profile_cache.rs` (separate from `src/cache.rs` since `QueryCache` and `ProfileCache` have completely different key/value types and lifecycles).

```rust
pub struct ProfileCache {
    cache: moka::sync::Cache<String, Arc<ProfileData>>,
    db: Arc<Database>,  // For SQLite fallback on cache miss
}

pub struct ProfileData {
    pub content: serde_json::Value,  // Parsed kind:0 content
    pub created_at: i64,
}
```

- Default capacity: 50,000 entries (configurable via `PROFILE_CACHE_SIZE`, bounds: `1000..=200_000`)
- TTL: `PROFILE_CACHE_TTL_SECS` (default 3600, bounds: `60..=86400`)
- Key: `String` pubkey (simpler than `Arc<str>`; profile lookups are not as hot as BFS node IDs)
- On miss: `ProfileCache` internally queries its `Arc<Database>` reference, populates the Moka cache, and returns
- `ProfileCache` is stored in `AppState` and accessible from all API handlers

**Modified AppState:**

```rust
pub struct AppState {
    pub graph: Arc<WotGraph>,
    pub config: Arc<Config>,
    pub cache: Arc<QueryCache>,
    pub profile_cache: Arc<ProfileCache>,  // NEW
}
```

#### Lookup Flow

```
Request needs profile for pubkey X
    │
    ▼
┌──────────┐
│ Moka     │──── Hit ────▶ Return Arc<ProfileData>
│ Cache    │
└────┬─────┘
     │ Miss
     ▼
┌──────────┐
│ SQLite   │──── Found ──▶ Insert Moka, Return
│ profiles │
└────┬─────┘
     │ Not found
     ▼
  Return None (profile not yet ingested)
```

### API Changes

#### New Query Parameter: `include_profiles`

Added to all existing endpoints:

| Endpoint | New Param |
|----------|-----------|
| `GET /distance` | `include_profiles=true` |
| `POST /distance/batch` | `"include_profiles": true` |
| `GET /follows` | `include_profiles=true` |
| `GET /common-follows` | `include_profiles=true` |
| `GET /path` | `include_profiles=true` |

When `include_profiles=true`, the response includes a `profiles` map with all relevant pubkeys:

```json
{
  "from": "abc...",
  "to": "def...",
  "hops": 2,
  "bridges": ["ghi..."],
  "profiles": {
    "abc...": { "name": "Alice", "picture": "https://...", "nip05": "alice@example.com" },
    "def...": { "name": "Bob", "display_name": "Bobby" },
    "ghi...": { "name": "Charlie", "about": "Bridge node" }
  }
}
```

**Which pubkeys get profiles:**

| Endpoint | Pubkeys Included |
|----------|-----------------|
| `/distance` | `from`, `to`, all `bridges` (if `include_bridges=true`) |
| `/distance/batch` | `from`, all `targets`, all `bridges` |
| `/follows` | `pubkey`, all returned follows |
| `/common-follows` | `from`, `to`, all common follows |
| `/path` | `from`, `to`, all intermediate path nodes |

If a pubkey has no cached profile, it is omitted from the `profiles` map (not an error).

#### New Endpoint: `GET /profiles`

Direct profile lookup for arbitrary pubkeys.

**Request:**
```
GET /profiles?pubkeys=abc...,def...,ghi...
```

**Response:**
```json
{
  "profiles": {
    "abc...": { "name": "Alice", "picture": "https://..." },
    "def...": { "name": "Bob" }
  }
}
```

- Max 100 pubkeys per request
- Unknown pubkeys are silently omitted
- Same validation as other endpoints (64 hex chars)

#### Response Type Changes

All handlers always return `WithProfiles<T>`. When `include_profiles` is `false` (the default), `profiles` is `None` and omitted from the JSON output via `skip_serializing_if`. This preserves backward compatibility — existing consumers see identical JSON.

```rust
#[derive(Serialize)]
pub struct WithProfiles<T: Serialize> {
    #[serde(flatten)]
    pub inner: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profiles: Option<HashMap<String, serde_json::Value>>,
}
```

Handler return types change from `Result<Json<T>, ErrorResponse>` to `Result<Json<WithProfiles<T>>, ErrorResponse>`.

**Query param struct modifications:**

```rust
// Added to DistanceQueryParams, PathQueryParams, FollowsQueryParams, CommonFollowsQueryParams:
#[serde(default)]
pub include_profiles: bool,

// Added to BatchDistanceRequest (POST body):
#[serde(default)]
pub include_profiles: bool,
```

**Profile resolution helper:**

```rust
async fn resolve_profiles(
    profile_cache: &ProfileCache,
    pubkeys: &[&str],
) -> Option<HashMap<String, serde_json::Value>> {
    let mut map = HashMap::new();
    for pk in pubkeys {
        if let Some(profile) = profile_cache.get(pk).await {
            map.insert(pk.to_string(), profile.content.clone());
        }
    }
    if map.is_empty() { None } else { Some(map) }
}
```

**Profile count limits:** For endpoints that could return many pubkeys (e.g., `/follows` with 5000 follows), profiles are capped at the first 500 pubkeys. The `from`/`to` pubkeys are always included. This is documented in the API docs and the cap is configurable via `MAX_PROFILE_RESULTS` (default 500, bounds: `10..=1000`).

### Profile Deduplication

Same pattern as kind:3:

```rust
// Separate LRU cache for kind:0 dedup
let seen_profiles: LruCache<[u8; 32], SeenEvent> = LruCache::new(100_000);

// On kind:0 event:
// 1. Check if pubkey in seen_profiles with >= created_at → skip
// 2. Parse content JSON
// 3. Check SQLite for existing created_at → skip if not newer
// 4. Store to SQLite + Moka cache
// 5. Update seen_profiles
```

### Batch Persistence

The persistence channel is changed from `mpsc::channel::<FollowUpdate>` to an enum:

```rust
enum PersistUpdate {
    Follow(FollowUpdate),
    Profile(ProfileUpdate),
}

struct ProfileUpdate {
    pubkey: String,
    content: String,        // Raw JSON
    event_id: String,
    created_at: i64,
}
```

The `persistence_worker` partitions each batch into follow updates and profile updates, then calls separate DB methods within a single transaction:

```rust
async fn flush_batch(db: &Database, batch: &mut Vec<PersistUpdate>) {
    let (follow_updates, profile_updates): (Vec<_>, Vec<_>) = batch.drain(..)
        .partition(|u| matches!(u, PersistUpdate::Follow(_)));

    let follow_batch: Vec<FollowUpdateBatch> = /* convert follow_updates */;
    let profile_batch: Vec<ProfileUpdateBatch> = /* convert profile_updates */;

    // Single transaction for both
    db.persist_batch(&follow_batch, &profile_batch).unwrap();
}
```

New DB method:

```rust
impl Database {
    /// Persist follow and profile updates in a single transaction.
    pub fn persist_batch(
        &self,
        follows: &[FollowUpdateBatch<'_>],
        profiles: &[ProfileUpdateBatch<'_>],
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        // ... upsert follows (existing logic) ...
        // ... upsert profiles (new logic) ...
        tx.commit()?;
        Ok(())
    }
}
```

If `content` JSON is malformed (not valid JSON), the profile update is logged as a warning and skipped — it is not persisted to SQLite or cached in Moka.

## Configuration

| Variable | Default | Bounds | Description |
|----------|---------|--------|-------------|
| `PROFILE_CACHE_SIZE` | `50000` | `1000..=200000` | Max profiles in Moka LRU cache |
| `PROFILE_CACHE_TTL_SECS` | `3600` | `60..=86400` | Profile cache TTL (1 hour) |
| `PROFILE_FETCH_ENABLED` | `true` | bool | Enable batch profile fetching from purplepag.es |
| `PROFILE_FETCH_INTERVAL_SECS` | `60` | `10..=3600` | How often to batch-fetch missing profiles |
| `PROFILE_FETCH_BATCH_SIZE` | `500` | `10..=1000` | Max authors per purplepag.es subscription |
| `PROFILE_STALE_SECS` | `86400` | `3600..=604800` | Re-fetch profiles older than this |
| `MAX_PROFILE_RESULTS` | `500` | `10..=1000` | Max profiles per API response |
| `DISCOVERY_RELAYS` | `wss://purplepag.es` | — | Relays used for profile + relay list discovery |

## Memory Impact

- Profile data: ~1-2 KB per profile (JSON with name, picture URL, about, etc.)
- Moka cache at 50k entries: ~50-100 MB
- SQLite handles cold storage with no memory impact
- Profile dedup LRU: ~3.2 MB (100k entries * 32 bytes)

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/profile_cache.rs` | Create | ProfileCache struct (Moka + SQLite fallback), ProfileData type |
| `src/sync/ingestion.rs` | Modify | Add kind:0 subscription, profile dedup, PersistUpdate enum, batch fetcher task |
| `src/db/sqlite.rs` | Modify | Add profiles table, `persist_batch()`, profile query methods |
| `src/api/http.rs` | Modify | Add `include_profiles` to all query params, add `/profiles` endpoint, `WithProfiles<T>` wrapper, `resolve_profiles()` helper |
| `src/config.rs` | Modify | Add profile cache config variables with bounds |
| `src/main.rs` | Modify | Initialize ProfileCache, wire into AppState, add `profile_cache` module |
| `src/lib.rs` or `src/main.rs` | Modify | Add `mod profile_cache;` |
| `docs/API.md` | Modify | Document `/profiles` endpoint and `include_profiles` parameter |
| `docs/SYNC.md` | Modify | Add profile ingestion to sync documentation |

## Non-Goals

- **Profile validation** — We store whatever kind:0 content we receive. No schema enforcement.
- **Profile search** — No full-text search on names/about. This is a cache, not a search engine.
- **Profile images** — No image proxying or caching. Just store the URL.
- **NIP-05 verification** — We store the nip05 field but don't verify it via HTTP.
