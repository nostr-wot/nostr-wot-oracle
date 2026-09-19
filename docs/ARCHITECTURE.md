# Architecture

The Rust binary runs Axum HTTP and optional NIP-90 DVM services on Tokio, plus a supervised relay-ingestion loop. SQLite is authoritative for committed follow/mute lists; the in-memory graph serves queries.

## Graph

Pubkeys are interned and represented by integer node IDs. Forward and reverse follow adjacency lists are sorted vectors. Public mute lists have their own metadata and sorted IDs. Mutations are serialized; readers follow a consistent adjacency→pubkey→metadata lock order. A monotonic revision invalidates cached results after topology or mute-evidence changes.

Bidirectional BFS expands the smaller frontier and counts shortest paths through the complete meeting layer. Thread-local scratch buffers are reused. Counts saturate at `u64::MAX`. Path reconstruction returns only intermediate nodes and respects the requested bound. Sorted-list intersections and pagination avoid unnecessary string conversion. CPU-heavy queries share four permits across HTTP and DVM; permits stay inside blocking tasks until computation finishes.

## Persistence

Each author/kind has independent replaceable-event ordering. SQLite transactions select the winning event per author, compare with persisted provenance, and insert/delete changed edges. Reload reads numeric edges and restores all metadata, including empty lists. Schema additions for public mutes preserve existing follow data.

Ingestion parses bounded batches, commits before publishing to the graph, retries failures and exits on persistent errors. Signals trigger a drain. Detailed sync limitations are in [SYNC.md](SYNC.md).

## Cache and API

Moka bounds cache size and TTL. Computations capture their starting graph revision; only results for an unchanged revision may be inserted, and reads reject stale generations. `/distance` remains follow-only. `/trust` combines independently observed distance and public mute evidence without assigning a numeric score. Private mute entries are unavailable.

The application exposes process liveness, ingestion readiness and graph/sync metrics separately. Production deployment uses a localhost container port behind nginx/HTTPS with persistent storage and restart supervision.
