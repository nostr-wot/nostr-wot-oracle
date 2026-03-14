# CLAUDE.md

## Project: WoT Oracle

Nostr Web of Trust oracle — indexes the global follow graph and provides pairwise distance queries.

## Commit Guidelines

- Do NOT add Co-Authored-By or Co-Signed-By lines to commits
- Do NOT add any AI attribution to commits
- Write commit messages in conventional commit format (feat:, fix:, docs:, etc.)
- Keep commit messages concise — focus on what changed and why

## Documentation Guidelines

- Do NOT commit plans, specs, or design documents to the repository
- Only commit documentation that describes what has been implemented
- Keep docs/ for user-facing documentation: API reference, architecture, deployment guides
- Remove any planning artifacts before committing

## Code Style

- Rust, using Tokio async runtime with Axum for HTTP
- Use parking_lot for Mutex/RwLock (not std::sync)
- Validate all inputs at API boundaries
- CPU-bound work (BFS, large computations) goes on spawn_blocking
- SQLite persistence goes on spawn_blocking
- Error messages should not leak internal details

## Key Architecture

- `src/graph/store.rs` — In-memory WotGraph with sorted adjacency lists
- `src/graph/bfs.rs` — Bidirectional BFS with thread-local state reuse
- `src/sync/ingestion.rs` — kind:3 + kind:0 event ingestion from relays
- `src/db/sqlite.rs` — SQLite persistence with batch writes
- `src/api/http.rs` — Axum REST API
- `src/api/dvm.rs` — NIP-90 Data Vending Machine
- `src/cache.rs` — Moka query cache
- `src/profile_cache.rs` — Moka profile cache + SQLite fallback (planned)

## Testing

- Run tests: `cargo test`
- Run with logging: `RUST_LOG=debug cargo run`
- All 39 tests should pass before committing
