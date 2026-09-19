# Sync and persistence

The service subscribes to kind 3 and kind 10000 on each configured relay. It parses public `p` tags, normalizes hex casing, deduplicates keys, and ignores private encrypted mute content. Follow and mute events are deduplicated independently by `(author, kind)`. Future-dated events more than ten minutes ahead of the service clock are ignored.

Newest `created_at` wins; equal timestamps select the lowest event ID. The graph and database enforce the same ordering. A known empty list retains metadata, including after restart, so an older event cannot restore removed follows or mutes.

Received events form bounded batches of up to 100, flushed once per second. SQLite writes coalesce each author's winning event and apply edge differences inside a transaction. The graph is updated only after persistence succeeds. A failed batch is retained for up to three attempts; persistent errors stop ingestion and cause the process to exit nonzero. Container supervision can then restart it from the database. Follow and mute batches are separate transactions; a crash between them is recovered by restoring committed state and relay replay.

There is no separate lossy persistence queue. Slow database work can still overrun the SDK notification buffer. Such lag is counted, the current batch is flushed, and the SDK client, its event-ID deduplication state and subscriptions are recreated to request relay history again. This is best-effort repair: coverage still depends on each relay's history retention and query limits.

On SIGTERM/SIGINT, ingestion stops accepting new events and flushes its received batch before exiting. HTTP shuts down gracefully. An abrupt kill or host failure can lose events not yet committed; replay depends on relay availability. The SQLite volume must be retained between releases.

## Coverage limits

The service does not implement a complete global crawl, historical pagination, persisted per-relay catch-up cursors, NIP-65 relay discovery, kind-0 profiles, or decryption of private mute lists. The broad subscription returns whatever history the selected relays supply and then follows their live streams. Nodes referenced by a list can exist before their own list has been received.

`/health` indicates liveness. `/ready` indicates recent ingestion without a current persistence failure, not global completeness. `/stats` exposes receipt/persistence timestamps, lag/errors and `configured_relays_only` coverage. Consumers must distinguish unknown evidence from negative evidence.
