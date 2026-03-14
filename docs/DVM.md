# DVM Interface (NIP-90)

WoT Oracle implements a [NIP-90](https://github.com/nostr-protocol/nips/blob/master/90.md) Data Vending Machine (DVM) interface, allowing Nostr clients to query social distance without HTTP.

## Overview

| Property | Value |
|----------|-------|
| Request Kind | 5950 |
| Response Kind | 6950 |
| Protocol | NIP-90 Data Vending Machine |

## Configuration

Enable DVM by setting these environment variables:

```bash
DVM_ENABLED=true
DVM_PRIVATE_KEY=nsec1...  # or hex format
```

The DVM will:
1. Connect to the same relays as the ingestion daemon (`RELAYS`)
2. Subscribe to kind:5950 events
3. Publish kind:6950 responses signed with `DVM_PRIVATE_KEY`

On startup, the DVM pubkey is logged:
```
INFO DVM service pubkey: 82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2
```

---

## Request Format (kind 5950)

### Option 1: Two Input Tags (NIP-90 Standard)

Use two `i` tags, one for each pubkey:

```json
{
  "kind": 5950,
  "pubkey": "<requester_pubkey>",
  "created_at": 1706889600,
  "tags": [
    ["i", "<from_pubkey>", "text"],
    ["i", "<to_pubkey>", "text"],
    ["param", "max_hops", "5"]
  ],
  "content": "",
  "id": "...",
  "sig": "..."
}
```

### Option 2: Combined Input Tag (Legacy)

Use a single `i` tag with colon-separated pubkeys:

```json
{
  "kind": 5950,
  "pubkey": "<requester_pubkey>",
  "created_at": 1706889600,
  "tags": [
    ["i", "<from_pubkey>:<to_pubkey>", "text"],
    ["param", "max_hops", "5"]
  ],
  "content": "",
  "id": "...",
  "sig": "..."
}
```

### Option 3: Separate Param Tags

Use individual `param` tags for each value:

```json
{
  "kind": 5950,
  "pubkey": "<requester_pubkey>",
  "created_at": 1706889600,
  "tags": [
    ["param", "from", "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2"],
    ["param", "to", "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d"],
    ["param", "max_hops", "5"]
  ],
  "content": "",
  "id": "...",
  "sig": "..."
}
```

### Parameters

| Tag | Format | Required | Default | Description |
|-----|--------|----------|---------|-------------|
| `i` (×2) | `["i", "<pubkey>", "text"]` | Yes* | - | Two tags: from and to pubkeys |
| `i` (combined) | `["i", "from:to", "text"]` | Yes* | - | Single tag with colon-separated pubkeys |
| `param` from | `["param", "from", "<pubkey>"]` | Yes* | - | Source pubkey |
| `param` to | `["param", "to", "<pubkey>"]` | Yes* | - | Target pubkey |
| `param` max_hops | `["param", "max_hops", "3"]` | No | 3 | Max search depth (1-5) |

*Use one of: two `i` tags, combined `i` tag, or both `from`/`to` params.

---

## Response Format (kind 6950)

### Success Response

```json
{
  "kind": 6950,
  "pubkey": "<dvm_service_pubkey>",
  "created_at": 1706889601,
  "tags": [
    ["e", "<request_event_id>"],
    ["p", "<requester_pubkey>"],
    ["result", "2", "hops"]
  ],
  "content": "{\"from\":\"82341f...\",\"to\":\"3bf0c6...\",\"hops\":2,\"path_count\":7,\"mutual_follow\":false,\"bridges\":[\"fa984b...\"]}",
  "id": "...",
  "sig": "..."
}
```

### Response Tags

| Tag | Description |
|-----|-------------|
| `e` | References the request event ID |
| `p` | References the requester's pubkey |
| `result` | Hop count (if path found) |

### Content JSON

The `content` field contains the full result as JSON:

```json
{
  "from": "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  "to": "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  "hops": 2,
  "path_count": 7,
  "mutual_follow": false,
  "bridges": ["fa984bd7dbb282f07e16e7ae87b26a2a7b9b90b7246a44771f0cf5ae58018f52"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `from` | string | Source pubkey |
| `to` | string | Target pubkey |
| `hops` | integer \| null | Shortest path length (null if not reachable) |
| `path_count` | integer | Number of shortest paths |
| `mutual_follow` | boolean | Whether both pubkeys follow each other |
| `bridges` | array | Pubkeys where forward/backward searches meet |

### Error Response

```json
{
  "kind": 6950,
  "pubkey": "<dvm_service_pubkey>",
  "created_at": 1706889601,
  "tags": [
    ["e", "<request_event_id>"],
    ["p", "<requester_pubkey>"],
    ["status", "error", "Invalid pubkey format"]
  ],
  "content": "{\"error\":\"Invalid pubkey format\"}",
  "id": "...",
  "sig": "..."
}
```

**Error Messages:**
- `Expected two 'i' tags with pubkeys or 'from'/'to' params`
- `Invalid pubkey format`

---

## Client Example

### Sending a Request (JavaScript)

```javascript
import { SimplePool, getEventHash, signEvent } from 'nostr-tools';

const pool = new SimplePool();

// Create request event (NIP-90 standard with two i tags)
const requestEvent = {
  kind: 5950,
  pubkey: myPubkey,
  created_at: Math.floor(Date.now() / 1000),
  tags: [
    ['i', fromPubkey, 'text'],
    ['i', toPubkey, 'text'],
    ['param', 'max_hops', '5']
  ],
  content: ''
};

requestEvent.id = getEventHash(requestEvent);
requestEvent.sig = signEvent(requestEvent, myPrivkey);

// Publish request
await pool.publish(relays, requestEvent);

// Subscribe for response
const sub = pool.sub(relays, [{
  kinds: [6950],
  '#e': [requestEvent.id]
}]);

sub.on('event', (event) => {
  const result = JSON.parse(event.content);
  console.log(`Distance: ${result.hops} hops`);
  sub.unsub();
});
```

### Using nak CLI

```bash
# Create and publish request (NIP-90 standard with two i tags)
nak event --kind 5950 \
  -t i="82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2" \
  -t i="3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d" \
  -t param=max_hops=5 \
  wss://relay.damus.io

# Subscribe for response (replace EVENT_ID)
nak req --kind 6950 -e EVENT_ID wss://relay.damus.io
```

---

## Relay Setup

The DVM connects to the relays specified in the `RELAYS` environment variable. For best results:

1. **Use the same relays** as your clients
2. **Include popular relays** where DVM requests are likely published
3. **Consider running a local relay** for lower latency

Example configuration:
```bash
RELAYS=wss://relay.mappingbitcoin.com,wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
```

---

## Monitoring

DVM activity is logged at various levels:

```
# Startup
INFO Starting DVM service...
INFO DVM service pubkey: 82341f...
INFO DVM added relay: wss://relay.damus.io
INFO DVM listening for requests (kind 5950)

# Successful request
DEBUG Received DVM request: abc123...
INFO Sent DVM response for 82341f... -> 3bf0c6...: Some(2) hops

# Error
WARN Sent DVM error response: Invalid 'from' pubkey format
ERROR Failed to process DVM request: ...
```

Set `RUST_LOG=debug` to see all DVM activity.

---

## Security Considerations

1. **Public Data**: DVM only exposes data already public on Nostr (follow graphs)
2. **Rate Limiting**: DVM has no built-in rate limiting (relies on relay limits)
3. **Key Security**: Protect `DVM_PRIVATE_KEY` - it signs all responses
4. **Validation**: All inputs are validated (64-char hex pubkeys)

---

## Comparison: HTTP vs DVM

| Feature | HTTP API | DVM (NIP-90) |
|---------|----------|--------------|
| Protocol | REST over HTTPS | Nostr events |
| Authentication | Per-IP rate limit | Nostr signatures |
| Caching | Server-side LRU | Client responsibility |
| Latency | Lower (~10-50ms) | Higher (~100-500ms) |
| Discovery | Known URL | Nostr relay subscription |
| Offline | Requires server | Works with any relay |

**Use HTTP when:**
- Building a web app with backend
- Need lowest latency
- Want server-side caching

**Use DVM when:**
- Building a pure Nostr client
- Want decentralized architecture
- No backend server available
