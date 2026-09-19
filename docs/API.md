# API reference

Version 0.3.0. Pubkeys use 64-character lowercase hexadecimal strings. Distance follows directed kind-3 edges; public mutes are separately reported observations, not negative traversal edges or a numeric trust score.

| Endpoint | Parameters | Result |
|---|---|---|
| `GET /health` | None | Process status and release version |
| `GET /ready` | None | Ingestion snapshot; 200 when ready, 503 otherwise |
| `GET /stats` | None | Node/follow/mute counts, cache, locks and sync status |
| `GET /distance` | `from`, `to`, optional `max_hops` (1–5, default 3), `include_bridges` (false), `bypass_cache` (false) | `from`, `to`, `hops`, `path_count`, `mutual_follow`, optional `bridges` |
| `POST /distance/batch` | JSON `from`, `targets` (up to 100), same optional distance settings | `from` and ordered `results`; duplicates preserved |
| `GET /path` | `from`, `to`, optional `max_hops` | `from`, `to`, `path`: intermediate pubkeys only, or null |
| `GET /follows` | `pubkey`, optional `offset` (0), `limit` (500, capped at 5000) | `pubkey`, `follows`, `total` |
| `GET /common-follows` | `from`, `to` | `from`, `to`, `common_follows` |
| `GET /mutes` | Same pagination parameters as follows | `pubkey`, `mutes`, `total`, `public_list_known` |
| `GET /trust` | Same parameters as distance | `follow_distance`, `public_mute_evidence` |

## Follow distance

`hops:null` means no route was found within the requested depth in the current indexed graph. It is not proof of no connection across Nostr. `path_count` counts shortest directed paths regardless of whether bridges are included. Counts saturate at the maximum unsigned 64-bit integer rather than overflowing. `bridges` are the search meeting nodes, not the entire path or disjoint-path certificates. Self-distance is zero; direct-follow distance is one. `/path` returns an empty intermediate list for self/direct paths.

## Public mute evidence

Example `/trust` response:

```json
{
  "follow_distance": {
    "from": "<source-pubkey>",
    "to": "<target-pubkey>",
    "hops": 2,
    "path_count": 1,
    "mutual_follow": false
  },
  "public_mute_evidence": {
    "source_mutes_target": false,
    "target_mutes_source": false,
    "followed_muters": ["<pubkey-followed-by-source>"],
    "source_mute_list_known": true,
    "target_mute_list_known": false
  }
}
```

`followed_muters` contains accounts directly followed by the source whose indexed public mute lists contain the target. No weight, aggregate score, or automatic exclusion is applied. Mutes may express personal preference, so clients must define their own policy. `public_list_known:false` means no public mute event was indexed. A known list with no public entries may still contain encrypted entries. Hashtag, word and thread mute tags are not pubkey evidence.

## Freshness and readiness

Cache entries become invalid when the graph revision changes. Results are snapshots of indexed events; there is no guarantee that all relays have returned the latest event. Follow distance and mute evidence may be read at slightly different instants during ingestion.

`/ready` requires running ingestion, no current database failure and a follow/mute event received within five minutes. It may return 503 on an intentionally quiet private relay even when the service is otherwise operational. `/health` is the process liveness check. `sync.coverage` is `configured_relays_only`.

`/stats` includes `node_count`, `edge_count`, `nodes_with_follows`, `mute_edge_count`, `nodes_with_mute_lists`, `cache`, `locks`, and `sync`. Sync exposes running/ready state, receipt/persistence timestamps, accepted persisted author-update count, lagged-notification count and persistence-error count. Timestamps are Unix seconds, zero when not yet observed.

## Limits and errors

Requests are limited per IP using `RATE_LIMIT_PER_MINUTE`; bodies are capped at 1 MiB. Graph computation shares a bounded worker budget with DVM requests. Validation errors return 400 with `error` and `code`; overload can return 503; rate limits return 429. Run behind a proxy that overwrites forwarded-client-IP headers, or do not trust client-provided forwarding headers.

Kind-0 profile caching and `/profiles` are not implemented. `include_profiles` is not supported.
