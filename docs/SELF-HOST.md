# Self-hosting WoT Oracle

Use the release image `ghcr.io/nostr-wot/nostr-wot-oracle:0.3.0` on Linux amd64 or arm64. Building from source requires Rust 1.93.0 or newer and the committed Cargo.lock.

## Compose

```sh
git clone https://github.com/nostr-wot/nostr-wot-oracle.git
cd nostr-wot-oracle
git checkout v0.3.0
cp .env.example .env
docker compose up -d
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/ready
```

Keep the named data volume. `docker compose down -v` destroys the indexed state. The schema upgrade adds mute tables while retaining existing follow data. Back up the database before upgrading; use SQLite's backup command for a live database, or stop the container before copying database/WAL files together.

Compose binds localhost by default. `HTTP_PORT` selects the host port; the container always listens on 8080. Use `BIND_ADDRESS=0.0.0.0` only for intentionally public direct access. The process itself binds all interfaces inside the container.

The source Dockerfile builds with Rust 1.93.0 and locked dependencies. `Dockerfile.release` accepts a prebuilt Linux binary; a macOS or Windows executable will not work in that image.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RELAYS` | damus.io, nos.lol, relay.primal.net, relay.mostr.pub | Comma-separated websocket relay URLs |
| `HTTP_PORT` | 8080 | HTTP listen port (host mapping with Compose) |
| `DB_PATH` | wot.db; /app/data/wot.db in Docker | SQLite path |
| `DVM_ENABLED` | false | Enable NIP-90 requests |
| `DVM_PRIVATE_KEY` | unset | Required for DVM; hex or nsec |
| `RATE_LIMIT_PER_MINUTE` | 100 | Per-IP token refill, bounded 1–1000 |
| `MAX_HOPS` | 3 | DVM default; HTTP default is 3, explicit limit 1–5 |
| `CACHE_SIZE` | 10000 | Bounded 100–100000 entries |
| `CACHE_TTL_SECS` | 300 | Maximum age, bounded 10–3600 seconds; graph revisions invalidate sooner |
| `RUST_LOG` | info | Logging verbosity |

Allow 60 seconds for graceful stopping. SIGTERM/SIGINT drains received batches before exit. Persistent database failure exits nonzero; `restart: unless-stopped` recovers the service. Size memory limits according to measured graph size and ingestion. No universal production throughput or memory guarantee is made.

## HTTPS reverse proxy

Point the intended DNS hostname at the server (or its configured Cloudflare origin), add an nginx virtual host and issue a certificate covering that hostname. A certificate for the bare domain alone does not cover subdomains. Route the site to the localhost container port, such as 8091:

```nginx
location / {
    proxy_pass http://127.0.0.1:8091;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $remote_addr;
    proxy_set_header Forwarded "";
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_read_timeout 30s;
}
```

The API's IP extractor trusts forwarding headers. The public proxy must overwrite them; do not append an untrusted client-provided chain. When using Cloudflare, configure nginx's real-IP module to trust only Cloudflare's published source networks. Keep the container port inaccessible externally. Exempt health/readiness from response caching.

## Verification and rollback

Verify `/health` reports the expected version, `/ready` becomes 200 after events arrive, and `/stats` shows persisted events and increasing indexed follow/mute counts. Exercise a real `/distance` and `/trust` query. Restart once and verify previously indexed data survives. Review logs for database errors and lag recovery.

Pin a release image, preferably by digest. To roll back, stop the new container, restore the pre-upgrade database backup if necessary, select the prior image, and restart. Preserve nginx configuration and certificate backups before routing changes. Initial graph population can take time and is not a complete global backfill; see [SYNC.md](SYNC.md).
