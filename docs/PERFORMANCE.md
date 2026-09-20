# Graph and storage performance

Run `cargo run --release --example graph_efficiency` from the repository. The example uses the production graph and SQLite modules, creates a fresh temporary database per trial, checks the restored edge count, and deletes its databases on exit. No service or relay connection is required.

The workload contains 5,000 64-character keys and 200,000 directed edges: author `i` follows `(i + j * 97) % 5000` for `j = 1..40`. Persistence uses batches of 100 authors. It also measures newer events with unchanged adjacency, single-edge replacements, and 50,000 live authors following one hub in ascending and descending node-ID order. Timings exclude input generation and database opening. Database bytes are measured after closing SQLite and checkpointing its WAL.

## Observed changes

On an Apple M1 Pro (arm64), Rust 1.93.0, three fresh-database trials on 2026-09-20 gave these medians. Baseline and optimized measurements used the same standalone harness and Cargo's default release profile; the repository example has the same workload but uses this repository's LTO release settings, so absolute timings can differ.

| Operation | v0.3.0 baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| SQLite restore | 52.26 ms | 28.26 ms | 46% lower |
| Cold SQLite writes | 1,112.72 ms | 1,008.35 ms | 9% lower |
| New database file | 12,009,472 bytes | 9,093,120 bytes | 24% smaller |

These are synthetic local measurements, not production latency or throughput guarantees. Hash-map iteration changes insertion order and page packing slightly across trials.

Restore now streams compact numeric adjacency, avoiding both the full `(i64, i64)` edge buffer and per-edge pubkey string allocation/lookup. It maps node IDs once, then rebuilds reverse adjacency in source-ID order. It validates all edges and mute metadata before changing the graph, preserves sparse database IDs through mapping, and respects newer live events when merging.

Persistence caches target IDs within each transaction. Redundant `nodes(pubkey)` and `edges(follower_id)` indexes are removed because the UNIQUE and composite PRIMARY KEY indexes already cover those lookups. Tests inspect SQLite query plans and confirm that the reverse index remains. Existing databases gain reusable free pages; their files are not automatically compacted. The smaller-file measurement applies to newly built databases.

Ingestion selects the winning event per author/kind before persistence and publication. Superseded targets no longer allocate phantom live nodes. HTTP batch distance requests reuse cache hits and compute their distinct remaining targets in one blocking task under one permit, reducing scheduling overhead while preserving result order and options.

## Limits

Live individual updates still use sorted reverse vectors. In this workload the 50,000-author hub takes roughly 17 ms in ascending source-ID order and 139 ms in descending order: shifting high-degree vectors remains expensive. Bulk restore avoids those insertions, but this change does not fix the general live-hub case.

Batch HTTP queries still run pairwise bidirectional BFS for each distinct uncached target, and a batch retains its permit until all those queries finish. The extension's immutable CSR snapshot and cached single-root traversal suit its local workload; the continuously changing global oracle retains mutable sorted adjacency. Follow pagination already resolves only the requested IDs. No CSR migration, live deployment benchmark, peak-RSS claim, or public API change is included here.
