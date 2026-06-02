# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-06-02

### Fixed
- **HTTP API not responding** - axum rate limiter required `ConnectInfo<SocketAddr>` to extract the client IP; without it the middleware silently dropped every request (#2). The webserver now starts and serves traffic in Docker.
- **Docker build broken** - Bumped builder image from `rust:1.85-slim-bookworm` to `rust:1.86-slim-bookworm`. Newer transitive deps (`icu_properties`, `idna_adapter`) require rustc 1.86.

## [0.2.1] - 2026-02-03

### Security
- **max_hops limit reduced** from 10 to 5 (default: 3) to prevent CPU exhaustion attacks
- **Bounded configuration values** - CACHE_SIZE (100-100,000), RATE_LIMIT (1-1000), CACHE_TTL (10-3600s)
- **Request body size limit** - 1MB limit to prevent memory exhaustion
- **DVM max_hops validation** - Now properly validates and clamps values (was silently accepting any value)
- **Less verbose error messages** - Pubkey validation errors no longer leak exact validation rules
- **DVM response hardening** - Removed full request echo from responses

### Changed
- Default max_hops changed from 5 to 3
- Maximum allowed max_hops changed from 10 to 5

## [0.2.0] - 2026-02-03

### Added
- `GET /follows?pubkey=xxx` - Returns array of pubkeys that the given pubkey follows
- `GET /common-follows?from=xxx&to=yyy` - Returns array of pubkeys that both from and to follow (mutual follows)
- `GET /path?from=xxx&to=yyy` - Returns array of pubkeys forming the shortest path between two pubkeys

## [0.1.0] - Initial Release - 2026-02-02

### Added
- Core Web of Trust graph indexing from Nostr relays
- `GET /health` - Health check endpoint
- `GET /stats` - Graph and cache statistics
- `GET /distance` - Query social distance between two pubkeys
- `POST /distance/batch` - Batch distance queries (up to 100 targets)
- Bidirectional BFS algorithm for efficient path finding
- LRU cache with TTL for query results
- Per-IP rate limiting
- Optional DVM (NIP-90) interface
- SQLite persistence for graph state
