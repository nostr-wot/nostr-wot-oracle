# Sync & Relay Discovery

This document describes how WoT Oracle discovers follow relationships and relays, how it determines the source of truth for follow lists, and how it batches requests for efficient network usage.

## Table of Contents

- [Overview](#overview)
- [Current Implementation](#current-implementation)
- [Proposed Sync Architecture](#proposed-sync-architecture)
- [Relay Discovery Strategy](#relay-discovery-strategy)
- [Follow Event Resolution](#follow-event-resolution)
- [Batched Subscriptions](#batched-subscriptions)
- [Progressive Depth Crawling](#progressive-depth-crawling)
- [Data Flow Diagrams](#data-flow-diagrams)
- [Profile Ingestion (kind:0)](#profile-ingestion-kind0)
- [Configuration](#configuration)

---

## Overview

WoT Oracle builds a global follow graph by ingesting `kind:3` (contact list / NIP-02) events from the Nostr network and caches user profile metadata by ingesting `kind:0` (profile / NIP-01) events. The core challenges are:

1. **Finding follow lists** — Where do we fetch a user's kind:3 event from?
2. **Relay discovery** — How do we know which relays a user publishes to?
3. **Source of truth** — How do we ensure we have the *latest* follow list, not a stale one?
4. **Efficiency** — How do we minimize relay connections and redundant requests?

## Current Implementation

### What Works

| Aspect | Status | Details |
|--------|--------|---------|
| Kind:3 ingestion | ✅ | Subscribes to kind:3 events across configured relays |
| Kind:0 ingestion | ✅ | Subscribes to kind:0 events for profile caching alongside kind:3 |
| Deduplication | ✅ | LRU cache (100k entries) by pubkey bytes, skips older events |
| Timestamp validation | ✅ | Graph only accepts events newer than the stored `created_at` |
| Batch persistence | ✅ | 100 events per SQLite transaction, 5-second flush timeout |
| Batch API queries | ✅ | `/distance/batch` supports up to 100 targets per request |

### What's Missing

| Aspect | Status | Impact |
|--------|--------|--------|
| NIP-65 relay discovery | ❌ | Can't find user-specific relays; misses events from users on non-default relays |
| Relay hints from p-tags | ❌ | Kind:3 p-tags contain relay URLs at position 2 — currently discarded |
| Multi-source verification | ❌ | Takes first event seen; no waiting for multiple relays to confirm latest |
| Dynamic relay connections | ❌ | Only connects to statically-configured relays from `RELAYS` env var |
| Outbox model | ❌ | No NIP-65-aware routing; all relays queried uniformly |
| Indexer API usage | ❌ | No use of nostr.band API or purplepag.es for bootstrapping |

### Current Sync Flow

```
Static Relays (env var)
    │
    ▼
Subscribe kind:3 (all authors, no filter)
    │
    ▼
Event arrives ──▶ LRU dedup check ──▶ Parse p-tags ──▶ Graph update ──▶ Persist queue
                   (skip if older)    (pubkey only)   (if newer)       (batch of 100)
```

**Problem:** This is a passive, firehose approach. We connect to 3 relays and hope to see every user's latest kind:3 event flow by. Users who publish only to relays we don't monitor are invisible.

---

## Proposed Sync Architecture

### Design Principles

1. **Respect user sovereignty** — Users choose their relays via NIP-65. Honor those choices.
2. **Outbox model** — To read a user's events, connect to their *write* relays.
3. **Multiple sources** — Never trust a single relay for "latest." Wait for corroboration.
4. **Progressive discovery** — Start from seed relays, discover more relays as we crawl.
5. **Batch aggressively** — Group users by relay, subscribe in bulk, minimize connections.
6. **Degrade gracefully** — If NIP-65 is unavailable, fall back to relay hints, then seed relays.

### High-Level Flow

```
                    ┌──────────────────────────────────────┐
                    │         Sync Orchestrator            │
                    └──────────┬───────────────────────────┘
                               │
              ┌────────────────┼────────────────────┐
              ▼                ▼                    ▼
     ┌────────────────┐ ┌────────────┐  ┌───────────────────┐
     │ Relay Discovery│ │  Follow    │  │  Event Resolution │
     │    Service     │ │  Fetcher   │  │     Service       │
     └────────────────┘ └────────────┘  └───────────────────┘
              │                │                    │
              ▼                ▼                    ▼
     ┌────────────────┐ ┌────────────┐  ┌───────────────────┐
     │  Relay Routing │ │  Batched   │  │  Multi-Source     │
     │    Table       │ │  Subs Pool │  │  Conflict Resolver│
     └────────────────┘ └────────────┘  └───────────────────┘
```

---

## Relay Discovery Strategy

To find where a user publishes, we use multiple sources in order of reliability:

### Source Priority

| Priority | Source | Event Kind | How |
|----------|--------|-----------|-----|
| 1 | **NIP-65 relay list** | `kind:10002` | Author-signed declaration of read/write relays. Canonical source. |
| 2 | **Kind:3 p-tag relay hints** | `kind:3` | Relay URL at position 2 of `["p", pubkey, relay_url, petname]`. Hints from the *follower* about where they find this user. |
| 3 | **Seed/indexer relays** | — | Bootstrap relays like `purplepag.es`, `relay.nostr.band` that aggregate events from across the network. |

### NIP-65 Relay List (kind:10002)

NIP-65 events declare a user's relay preferences:

```json
{
  "kind": 10002,
  "tags": [
    ["r", "wss://relay.damus.io"],
    ["r", "wss://my-relay.com", "write"],
    ["r", "wss://inbox.nostr.com", "read"]
  ]
}
```

- **No marker** = both read and write
- **`"write"`** = outbox — user publishes here (fetch their events from here)
- **`"read"`** = inbox — user reads here (send them mentions/DMs here)

**For WoT Oracle**, we care about **write relays** — that's where we'll find a user's kind:3 event.

### Kind:3 Relay Hints

Kind:3 p-tags can carry relay hints:

```json
["p", "ab12...cd34", "wss://relay.example.com", "alice"]
```

Position 2 (`wss://relay.example.com`) is a hint from the follow-list author about where to find the followed user's events. These are less authoritative than NIP-65 (they're from a third party) but widely available.

**Current code discards these.** The ingestion should extract and store them as fallback relay hints.

### Bootstrap / Indexer Relays

These special-purpose relays aggregate data from across the network:

| Relay | Purpose | Event Kinds |
|-------|---------|-------------|
| `wss://purplepag.es` | Directory relay for relay discovery | `kind:0`, `kind:10002` only |
| `wss://relay.nostr.band` | Full indexer relay | All kinds, supports NIP-45 COUNT |

**Usage:**
- Query `purplepag.es` for `kind:10002` events to bootstrap relay discovery
- Query `relay.nostr.band` for `kind:3` events as a fallback aggregator
- These should be used as **bootstrap sources**, not as the sole data source

### Relay Routing Table

The system should maintain an in-memory routing table:

```
pubkey → RelaySet {
    write_relays: Vec<String>,     // From NIP-65 (outbox)
    read_relays: Vec<String>,      // From NIP-65 (inbox)
    hint_relays: Vec<String>,      // From kind:3 p-tag hints
    source: RelaySource,           // How we discovered these
    updated_at: u64,               // When this entry was last refreshed
}
```

**Lookup order:**
1. Check routing table for user's write relays
2. If missing → query `purplepag.es` + `relay.nostr.band` for `kind:10002`
3. If still missing → use hint relays from kind:3 p-tags
4. If still missing → use seed relays (configured defaults)

---

## Follow Event Resolution

### The Problem

Kind:3 is a **replaceable event** — only the latest version per pubkey is valid. But relays don't gossip among themselves. Relay A may have a version from January while Relay B has the real latest from March.

### Resolution Strategy

**For each user whose kind:3 we want:**

1. **Query multiple relays in parallel** with `{ kinds: [3], authors: [pubkey], limit: 1 }`
2. **Collect responses** with a timeout window (e.g., 3-5 seconds, or until N relays respond)
3. **Select the winner** using NIP-01 conflict resolution:
   - Primary: highest `created_at` timestamp
   - Tiebreaker: lowest event `id` (lexicographic)
4. **Update graph** only with the winning event

```
  Relay A ──────┐
                │     ┌──────────────┐     ┌────────────────┐
  Relay B ──────┼────▶│   Collect    │────▶│  Select latest │────▶ Graph Update
                │     │  (timeout)   │     │  (created_at)  │
  Relay C ──────┘     └──────────────┘     └────────────────┘
                       Wait for:
                       - All relays respond, OR
                       - 3-5 second timeout, OR
                       - N responses received
```

### Why Wait?

The current implementation processes events immediately as they arrive. This means:
- If a stale relay responds first, we accept a stale follow list
- When the real latest arrives later, we update again (wasted work)
- In the worst case, we might miss the latest entirely if it's on a relay we don't monitor

**Waiting for multiple sources:**
- Increases confidence we have the actual latest event
- Reduces unnecessary graph updates from stale events
- Worth the latency cost at sync time (queries can still be served from cache/existing graph)

### Continuous vs. One-Shot

After initial sync, the system should still process kind:3 events as they stream in (the current firehose approach), but with the same timestamp-based conflict resolution. The waiting strategy is primarily for:
- Initial bootstrap
- Targeted re-fetches of specific users
- Periodic freshness checks

---

## Batched Subscriptions

### The Problem

Subscribing to kind:3 for each user individually creates N subscriptions. With 500k users, that's unmanageable.

### Batching Strategy

#### Level 0 (Seed User)

```
Single subscription:
  { kinds: [3, 10002], authors: [seed_pubkey] }
  → Sent to: purplepag.es + seed relays
  → Wait: until responses from ≥2 relays or 5s timeout
```

#### Level 1 (Seed's Follows)

```
Batch subscription (grouped by relay):
  For each relay R that multiple followed users publish to:
    { kinds: [3], authors: [pubkey1, pubkey2, ..., pubkeyN] }
    → Sent to: R
    → Wait: until responses from ≥2 relays per user or 10s timeout

  If user has no known relays:
    { kinds: [3, 10002], authors: [pubkeyA, pubkeyB, ...] }
    → Sent to: purplepag.es + relay.nostr.band
```

#### Level 2+ (Follows of Follows)

Same as Level 1 but with more users. The relay routing table is now populated, so more requests go to user-specific relays rather than indexers.

### Relay Grouping

To minimize connections, group users by their write relays:

```
Step 1: Collect all pubkeys needing kind:3 fetch
Step 2: Look up each pubkey's write relays in routing table
Step 3: Group: relay_url → [pubkey1, pubkey2, ...]
Step 4: For each group, create one subscription with all pubkeys
Step 5: Subscribe, collect responses, resolve conflicts
```

**Example:**
```
relay.damus.io → [alice, bob, dave]     → 1 subscription, 3 authors
nos.lol        → [alice, carol, eve]    → 1 subscription, 3 authors
my-relay.com   → [bob]                  → 1 subscription, 1 author
```

Alice appears on two relays — we'll get her kind:3 from both, then select the latest.

### Subscription Limits

Most relays limit authors per filter to ~1000. If a batch exceeds this:
- Split into chunks of 500-1000 authors
- Send as separate subscriptions to the same relay

---

## Progressive Depth Crawling

### Why Progressive?

The first level (seed user's follows) takes the longest because:
- We don't know anyone's relays yet
- We must bootstrap via indexer relays
- We must wait for multi-source verification

Each subsequent level is faster because:
- The relay routing table is populated from previous levels
- Relay hints from kind:3 p-tags are available
- Subscriptions are better targeted (fewer indexer queries)

### Crawl Process

```
Level 0: Fetch seed user's kind:3 + kind:10002
         ├── Extract follows (e.g., 500 pubkeys)
         ├── Extract relay hints from p-tags
         └── Store in relay routing table
              │
Level 1: For seed's 500 follows:
         ├── Batch-fetch kind:10002 from purplepag.es
         ├── Update relay routing table
         ├── Batch-fetch kind:3 grouped by relay
         ├── Wait for multi-source verification
         ├── Extract their follows (~50k unique pubkeys)
         └── Extract relay hints from their p-tags
              │
Level 2: For ~50k unique pubkeys:
         ├── Many already have relay info (from Level 1 hints)
         ├── Batch-fetch kind:10002 for unknowns
         ├── Batch-fetch kind:3 grouped by relay
         └── Continue...
```

### Timing Expectations

| Level | Users | Expected Duration | Why |
|-------|-------|-------------------|-----|
| 0 | 1 | 2-5 seconds | Single user, wait for multiple relays |
| 1 | ~500 | 30-60 seconds | Relay discovery needed, batch subscriptions, multi-source wait |
| 2 | ~50k | 5-15 minutes | Most relay info known, large batches, some unknowns |
| 3+ | ~500k | 15-30 minutes | Routing table well-populated, mostly targeted fetches |

After initial sync, the continuous firehose subscription maintains freshness.

---

## Data Flow Diagrams

### Complete Sync Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Sync Orchestrator                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────┐                                                   │
│  │  1. Bootstrap    │  Connect to seed relays + purplepag.es            │
│  │     Relays       │  Subscribe kind:10002 for target pubkeys          │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  2. Build Relay  │  Parse kind:10002 r-tags (read/write)             │
│  │     Routing      │  Store write relays per pubkey                    │
│  │     Table        │  Fall back to p-tag hints if no kind:10002        │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  3. Batch Fetch  │  Group pubkeys by write relay                     │
│  │     kind:3       │  Subscribe { kinds:[3], authors:[...] }           │
│  │     Events       │  per relay                                        │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  4. Multi-Source │  Collect kind:3 from multiple relays per user     │
│  │     Resolution   │  Wait for timeout or N responses                  │
│  │                  │  Select highest created_at (lowest id tiebreak)   │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  5. Process &    │  Parse p-tags → extract follows + relay hints     │
│  │     Extract      │  Update relay routing table with hints            │
│  │                  │  Update in-memory graph (diff-based)              │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  6. Batch        │  Queue updates → batch of 100 per transaction    │
│  │     Persist      │  5-second flush timeout                           │
│  └────────┬─────────┘                                                   │
│           ▼                                                             │
│  ┌──────────────────┐                                                   │
│  │  7. Next Level   │  Enqueue newly discovered pubkeys                 │
│  │     Crawl        │  Repeat from step 2 for next depth level          │
│  └──────────────────┘                                                   │
│                                                                         │
│  ┌──────────────────┐                                                   │
│  │  8. Continuous   │  After initial crawl, maintain firehose           │
│  │     Firehose     │  subscription for real-time updates               │
│  └──────────────────┘                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Relay Discovery Decision Tree

```
Need events from user X
        │
        ▼
  ┌─────────────┐
  │ Check relay │
  │ routing     │──── Found write relays ────▶ Use them
  │ table       │
  └──────┬──────┘
         │ Not found
         ▼
  ┌─────────────┐
  │ Query       │
  │ purplepag.es│──── Found kind:10002 ──────▶ Parse r-tags
  │ + nostr.band│                               Update routing table
  └──────┬──────┘                               Use write relays
         │ Not found
         ▼
  ┌─────────────┐
  │ Check p-tag │
  │ relay hints │──── Found hints ───────────▶ Use hint relays
  └──────┬──────┘
         │ Not found
         ▼
  ┌─────────────┐
  │ Use seed    │──── Always available ──────▶ Use configured defaults
  │ relays      │
  └─────────────┘
```

---

## Configuration

### Current

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAYS` | `wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band` | Seed relays for initial connections |

### Proposed Additions

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCOVERY_RELAYS` | `wss://purplepag.es,wss://relay.nostr.band` | Relays queried for kind:10002 (relay lists) |
| `SYNC_TIMEOUT_SECS` | `5` | How long to wait for multi-source verification per batch |
| `SYNC_MIN_SOURCES` | `2` | Minimum relay responses before accepting an event |
| `SYNC_BATCH_SIZE` | `500` | Max authors per subscription filter |
| `SYNC_MAX_DEPTH` | `3` | How many levels deep to crawl from seed |
| `SEED_PUBKEY` | (none) | Starting pubkey for targeted WoT crawl |

---

## Comparison: Current vs. Proposed

| Aspect | Current | Proposed |
|--------|---------|----------|
| Relay selection | Static 3 relays | Dynamic per-user via NIP-65 + hints |
| Follow discovery | Passive firehose | Active crawl + continuous firehose |
| Event freshness | First seen wins | Multi-source verification with timeout |
| Relay hints | Discarded | Extracted and stored in routing table |
| NIP-65 | Not used | Core of relay discovery strategy |
| Indexer usage | relay.nostr.band as relay only | purplepag.es + nostr.band for bootstrapping |
| Subscription model | 1 global filter for all kind:3 | Batched per-relay, grouped by author |
| Depth control | None (all events) | Progressive level-by-level crawl |
| Network efficiency | Low (broad firehose) | High (targeted, batched, grouped) |

---

## Profile Ingestion (kind:0)

WoT Oracle ingests `kind:0` (user metadata / NIP-01) events to populate the profile cache. Profile data (display name, picture URL, NIP-05 identifier, about text, etc.) is made available through the `/profiles` endpoint and via the `include_profiles` parameter on other endpoints.

### Ingestion Sources

Profiles are ingested through two mechanisms:

1. **Relay subscription (continuous):** The same relay connections used for kind:3 events also subscribe to kind:0 events. As profiles stream in from connected relays, they are processed and cached alongside follow lists.

2. **purplepag.es batch fetch (bootstrap):** During initial sync and progressive depth crawling, `purplepag.es` is queried in batch for kind:0 events of discovered pubkeys. This is particularly effective because `purplepag.es` serves as a directory relay that aggregates kind:0 and kind:10002 events from across the network.

### Deduplication

Profile events follow the same deduplication pattern as kind:3 events:

- **LRU dedup cache:** An LRU cache keyed by pubkey bytes stores `(created_at, event_id)` tuples. Incoming kind:0 events are checked against this cache and rejected if already seen or older than the cached entry.
- **Timestamp validation:** The profile cache only accepts events with a `created_at` newer than the currently stored profile for that pubkey, ensuring stale profiles never overwrite fresher ones.
- **Replaceable event semantics:** Kind:0 is a replaceable event (per NIP-01), so only the latest version per pubkey is retained. The same conflict resolution rules apply: highest `created_at` wins, lowest event `id` breaks ties.

### Sync Flow

```
Relay subscription (kind:0)
    │
    ▼
Event arrives ──▶ LRU dedup check ──▶ Parse JSON content ──▶ Profile cache update ──▶ Persist queue
                   (skip if older)    (name, picture, etc.)   (if newer)                (batch of 100)

purplepag.es batch fetch
    │
    ▼
{ kinds: [0], authors: [pubkey1, pubkey2, ...] }
    │
    ▼
Responses ──▶ Same pipeline as above
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILE_CACHE_SIZE` | `50000` | Maximum number of profiles in the in-memory LRU cache |
| `PROFILE_CACHE_TTL_SECS` | `3600` | Time-to-live for cached profile entries (1 hour) |

---

## Implementation Notes

### Relay Hint Extraction (Quick Win)

The current `process_event` function in `src/sync/ingestion.rs` only extracts pubkeys from p-tags. To also extract relay hints:

```rust
// Current: only extracts pubkey (with validation)
let follows: Vec<String> = event.tags.iter()
    .filter_map(|tag| {
        let tag_vec = tag.as_slice();
        if tag_vec.len() >= 2 && tag_vec[0] == "p" {
            let pk = &tag_vec[1];
            if pk.len() == 64 && pk.bytes().all(|b| b.is_ascii_hexdigit()) {
                Some(pk.to_string())
            } else {
                None
            }
        } else { None }
    }).collect();

// Proposed: extract pubkey + relay hint
struct FollowEntry {
    pubkey: String,
    relay_hint: Option<String>,
}

let follows: Vec<FollowEntry> = event.tags.iter()
    .filter_map(|tag| {
        let tag_vec = tag.as_slice();
        if tag_vec.len() >= 2 && tag_vec[0] == "p" {
            let relay_hint = tag_vec.get(2)
                .filter(|r| r.starts_with("wss://") || r.starts_with("ws://"))
                .map(|r| r.to_string());
            Some(FollowEntry {
                pubkey: tag_vec[1].to_string(),
                relay_hint,
            })
        } else { None }
    }).collect();
```

### NIP-01 Conflict Resolution

```rust
fn select_latest_event(events: &[Event]) -> Option<&Event> {
    events.iter().max_by(|a, b| {
        a.created_at.cmp(&b.created_at)
            .then_with(|| b.id.cmp(&a.id)) // Lower ID wins on tie
    })
}
```

### Subscription Batching

```rust
// Group pubkeys by their known write relays
fn group_by_relay(
    pubkeys: &[String],
    routing_table: &RelayRoutingTable,
    fallback_relays: &[String],
) -> HashMap<String, Vec<String>> {
    let mut groups: HashMap<String, Vec<String>> = HashMap::new();

    for pubkey in pubkeys {
        let relays = routing_table
            .get_write_relays(pubkey)
            .unwrap_or_else(|| fallback_relays.to_vec());

        for relay in relays {
            groups.entry(relay).or_default().push(pubkey.clone());
        }
    }

    groups
}
```
