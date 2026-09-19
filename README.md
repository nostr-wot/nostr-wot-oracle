# Nostr WoT Oracle

A Rust service that indexes public Nostr follow and mute lists. Query directed follow distance, shortest-path counts, mutual follows, and public mute evidence between pubkeys.

**Distance is a follow-graph measurement, not a combined trust score.** Mutes are separate signals. Only public kind-10000 `p` tags are visible; encrypted entries are not available to the oracle. Coverage is limited to events returned by the configured relays, so a missing path or mute list does not establish absence across Nostr.

## Run

```sh
docker compose up -d
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/ready
curl http://127.0.0.1:8080/stats
```

Compose uses `ghcr.io/nostr-wot/nostr-wot-oracle:0.3.0`, stores SQLite in a named volume, and binds to localhost. Use an HTTPS reverse proxy for public access. Set `BIND_ADDRESS=0.0.0.0` only when direct network exposure is intended.

From source, use Rust 1.93.0 or newer:

```sh
cargo test --locked
cargo build --locked --release
./target/release/wot-oracle
```

## API

Replace `FROM` and `TO` with 64-character lowercase hexadecimal public keys.

```sh
curl 'http://127.0.0.1:8080/distance?from=FROM&to=TO&include_bridges=true'
curl 'http://127.0.0.1:8080/path?from=FROM&to=TO'
curl 'http://127.0.0.1:8080/follows?pubkey=FROM&offset=0&limit=500'
curl 'http://127.0.0.1:8080/mutes?pubkey=FROM'
curl 'http://127.0.0.1:8080/trust?from=FROM&to=TO'
```

`/trust` returns `follow_distance` and `public_mute_evidence`, including direct mute flags, followed accounts publicly muting the target, and whether each side's public mute list is known. It does not change graph traversal or calculate a numeric score.

`POST /distance/batch` accepts `from`, up to 100 `targets`, and optional `max_hops`, `include_bridges`, `bypass_cache`. Duplicate targets retain their positions but are calculated once per request. `/common-follows` returns the intersection of two follow lists.

`/health` reports process liveness; `/ready` requires running ingestion, no current persistence failure and a recently received list event. Neither promises a complete global graph. `/stats` includes graph size, public mute counts, cache/lock metrics and ingestion status.

## Operation

- Follow and mute lists have independent event versions: newest timestamp wins, lowest event ID breaks ties.
- Batches commit to SQLite before becoming visible in memory. Failed batches retry, then terminate ingestion so the supervisor can restart and recover from persisted state.
- SIGTERM/SIGINT drains the pending batch and shuts down HTTP gracefully. Allow at least 60 seconds before force-stopping a container.
- Cache entries are tied to graph revisions. Four concurrent expensive queries are allowed across HTTP and DVM.
- Profiles, `/profiles`, NIP-65 relay discovery and comprehensive historical crawling are not implemented.

See [.env.example](.env.example), [API reference](docs/API.md), [self-hosting](docs/SELF-HOST.md), [sync behavior](docs/SYNC.md), [architecture](docs/ARCHITECTURE.md) and [changelog](CHANGELOG.md).

MIT license.
