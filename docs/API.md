# API Reference

WoT Oracle exposes a REST API for querying social distance in the Nostr follow graph.

**Base URL:** `http://localhost:8080` (configurable via `HTTP_PORT`)

## Endpoints

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "version": "0.2.1"
}
```

---

### GET /stats

Returns graph statistics and cache metrics.

**Response:**
```json
{
  "node_count": 150000,
  "edge_count": 2500000,
  "nodes_with_follows": 120000,
  "cache": {
    "size": 5432,
    "capacity": 10000,
    "ttl_secs": 300
  },
  "locks": {
    "write_lock_count": 5000,
    "write_lock_avg_us": 12,
    "write_lock_max_us": 450,
    "read_lock_count": 100000,
    "read_lock_avg_us": 2,
    "read_lock_max_us": 85
  }
}
```

---

### GET /distance

Query the social distance between two pubkeys.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `from` | string | Yes | - | Source pubkey (64 hex chars) |
| `to` | string | Yes | - | Target pubkey (64 hex chars) |
| `max_hops` | integer | No | 3 | Maximum hops to search (1-5) |
| `include_bridges` | boolean | No | false | Include bridge node pubkeys |
| `bypass_cache` | boolean | No | false | Skip cache, force fresh computation |
| `include_profiles` | boolean | No | false | Include cached kind:0 profile metadata for all pubkeys in the response |

**Example:**
```bash
curl "http://localhost:8080/distance?from=82341f...&to=3bf0c6...&include_bridges=true"
```

**Response:**
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "to": "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  "hops": 2,
  "path_count": 3,
  "mutual_follow": false,
  "bridges": [
    "fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"
  ]
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `from` | string | Source pubkey |
| `to` | string | Target pubkey |
| `hops` | integer or null | Number of hops (null if not reachable) |
| `path_count` | integer | Number of shortest paths found |
| `mutual_follow` | boolean | Whether from and to follow each other |
| `bridges` | array or null | Pubkeys where paths meet (if `include_bridges=true`) |

**Error Response:**
```json
{
  "error": "Invalid pubkey format",
  "code": "INVALID_PUBKEY"
}
```

**Error Codes:**
- `INVALID_PUBKEY` - Invalid pubkey format
- `INVALID_MAX_HOPS` - max_hops must be 1-5
- `INTERNAL_ERROR` - Server error

---

### POST /distance/batch

Query distances from one pubkey to multiple targets in a single request.

**Request Body:**
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "targets": [
    "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
    "fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"
  ],
  "max_hops": 5,
  "include_bridges": false,
  "bypass_cache": false
}
```

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `from` | string | Yes | - | Source pubkey (64 hex chars) |
| `targets` | array | Yes | - | Target pubkeys (max 100) |
| `max_hops` | integer | No | 3 | Maximum hops to search (1-5) |
| `include_bridges` | boolean | No | false | Include bridge node pubkeys |
| `bypass_cache` | boolean | No | false | Skip cache, force fresh computation |
| `include_profiles` | boolean | No | false | Include cached kind:0 profile metadata for all pubkeys in the response |

**Example:**
```bash
curl -X POST http://localhost:8080/distance/batch \
  -H "Content-Type: application/json" \
  -d '{
    "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
    "targets": ["3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"]
  }'
```

**Response:**
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "results": [
    {
      "from": "82341f...",
      "to": "3bf0c6...",
      "hops": 2,
      "path_count": 1,
      "mutual_follow": false
    }
  ]
}
```

**Error Codes:**
- `TOO_MANY_TARGETS` - Maximum 100 targets per batch

---

### GET /follows

Returns the list of pubkeys that a given pubkey follows.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pubkey` | string | Yes | The pubkey to get follows for (64 hex chars) |
| `include_profiles` | boolean | No | Include cached kind:0 profile metadata for all pubkeys in the response |

**Example:**
```bash
curl "http://localhost:8080/follows?pubkey=82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2"
```

**Response:**
```json
{
  "pubkey": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "follows": [
    "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
    "fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"
  ]
}
```

---

### GET /common-follows

Returns the list of pubkeys that both `from` and `to` follow (mutual follows).

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `from` | string | Yes | First pubkey (64 hex chars) |
| `to` | string | Yes | Second pubkey (64 hex chars) |
| `include_profiles` | boolean | No | Include cached kind:0 profile metadata for all pubkeys in the response |

**Example:**
```bash
curl "http://localhost:8080/common-follows?from=82341f...&to=3bf0c6..."
```

**Response:**
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "to": "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  "common_follows": [
    "fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"
  ]
}
```

---

### GET /path

Returns the shortest path between two pubkeys as an array of intermediate pubkeys.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `from` | string | Yes | - | Source pubkey (64 hex chars) |
| `to` | string | Yes | - | Target pubkey (64 hex chars) |
| `max_hops` | integer | No | 3 | Maximum hops to search (1-5) |
| `include_profiles` | boolean | No | false | Include cached kind:0 profile metadata for all pubkeys in the response |

**Example:**
```bash
curl "http://localhost:8080/path?from=82341f...&to=3bf0c6..."
```

**Response:**
```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "to": "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  "path": [
    "fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"
  ]
}
```

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `from` | string | Source pubkey |
| `to` | string | Target pubkey |
| `path` | array or null | Array of intermediate pubkeys (empty if direct follow, null if not reachable) |

**Note:** The path array contains only the intermediate nodes. For example:
- If `from` directly follows `to`, path is `[]` (empty array)
- If the path is `from -> A -> to`, path is `["A"]`
- If the path is `from -> A -> B -> to`, path is `["A", "B"]`
- If no path exists within `max_hops`, path is `null`

---

### GET /profiles

Returns cached kind:0 profile metadata for the requested pubkeys.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `pubkeys` | string | Yes | Comma-separated list of pubkeys (64 hex chars each, max 100) |

**Example:**
```bash
curl "http://localhost:8080/profiles?pubkeys=82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2,3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"
```

**Response:**
```json
{
  "profiles": {
    "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2": {
      "name": "Alice",
      "picture": "https://example.com/alice.jpg",
      "nip05": "alice@example.com"
    },
    "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d": {
      "name": "Bob",
      "picture": "https://example.com/bob.jpg",
      "nip05": "bob@example.com"
    }
  }
}
```

**Notes:**
- Only profiles that exist in the cache are returned; missing pubkeys are silently omitted.
- Maximum 100 pubkeys per request.

---

### Profile Responses

When `include_profiles=true` is set on any endpoint, the response includes a `profiles` map containing cached kind:0 metadata for all pubkeys referenced in the response (source, target, bridges, follows, path nodes, etc.):

```json
{
  "profiles": {
    "abc...": { "name": "Alice", "picture": "https://...", "nip05": "alice@example.com" }
  }
}
```

**Note:** A maximum of 500 profiles are returned per response (`MAX_PROFILE_RESULTS`). If the response references more pubkeys than this limit, profiles are returned for the most relevant pubkeys (source, target, and bridge/path nodes take priority).

---

## Rate Limiting

Requests are rate-limited per IP address using a token bucket algorithm.

- **Default:** 100 requests per minute
- **Burst:** ~16 requests (10 second burst)
- **Response:** HTTP 429 when rate limit exceeded

Configure via `RATE_LIMIT_PER_MINUTE` environment variable.

---

## DVM Interface (NIP-90)

WoT Oracle can also respond to Nostr DVM (Data Vending Machine) requests.

**Request Event (kind 5950):**
```json
{
  "kind": 5950,
  "tags": [
    ["i", "<from_pubkey>", "text"],
    ["param", "target", "<to_pubkey>"],
    ["param", "max_hops", "5"]
  ]
}
```

**Response Event (kind 6950):**
```json
{
  "kind": 6950,
  "tags": [
    ["e", "<request_id>"],
    ["p", "<requester_pubkey>"],
    ["result", "hops", "2"],
    ["result", "path_count", "3"],
    ["result", "mutual_follow", "false"]
  ],
  "content": "{\"from\":\"...\",\"to\":\"...\",\"hops\":2,...}"
}
```

Enable DVM with `DVM_ENABLED=true` and `DVM_PRIVATE_KEY=<nsec or hex>`.

---

## Caching

Query results are cached in an LRU cache with configurable size and TTL.

- **Cache Key:** (from_id, to_id, max_hops, include_bridges)
- **Invalidation:** Cache entries expire after CACHE_TTL_SECS (default 300s). No automatic invalidation on graph changes.

Use `bypass_cache=true` to force fresh computation.
