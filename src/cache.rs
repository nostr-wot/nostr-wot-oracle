use moka::sync::Cache;
use std::time::Duration;

use crate::graph::bfs::DistanceResult;
use crate::graph::WotGraph;

// Default values for cache configuration (used by with_defaults())
#[allow(dead_code)] // Used by with_defaults() for standalone/testing scenarios
const DEFAULT_CACHE_SIZE: usize = 10000;
#[allow(dead_code)] // Used by with_defaults() for standalone/testing scenarios
const DEFAULT_TTL_SECS: u64 = 300; // 5 minutes

/// Compact cache key using node IDs instead of string pubkeys.
/// 10 bytes vs 178 bytes per key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub from_id: u32,
    pub to_id: u32,
    pub max_hops: u8,
    pub include_bridges: bool,
}

impl CacheKey {
    pub fn new(from_id: u32, to_id: u32, max_hops: u8, include_bridges: bool) -> Self {
        Self {
            from_id,
            to_id,
            max_hops,
            include_bridges,
        }
    }
}

/// Compact cached distance using node IDs for bridges.
/// Resolved to strings only at API boundary.
#[derive(Debug, Clone)]
struct CachedDistance {
    revision: u64,
    hops: Option<u32>,
    path_count: u64,
    mutual_follow: bool,
    bridge_ids: Option<Vec<u32>>, // 4 bytes each vs 88 bytes for strings
}

impl CachedDistance {
    fn from_result(result: &DistanceResult, graph: &WotGraph, revision: u64) -> Self {
        let bridge_ids = result.bridges.as_ref().map(|bridges| {
            bridges
                .iter()
                .filter_map(|pubkey| graph.get_node_id(pubkey))
                .collect()
        });

        Self {
            revision,
            hops: result.hops,
            path_count: result.path_count,
            mutual_follow: result.mutual_follow,
            bridge_ids,
        }
    }

    fn to_result(&self, graph: &WotGraph, from_id: u32, to_id: u32) -> Option<DistanceResult> {
        let from = graph.get_pubkey_arc(from_id)?;
        let to = graph.get_pubkey_arc(to_id)?;

        let bridges = self
            .bridge_ids
            .as_ref()
            .map(|ids| graph.resolve_pubkeys_arc(ids));

        Some(DistanceResult {
            from,
            to,
            hops: self.hops,
            path_count: self.path_count,
            mutual_follow: self.mutual_follow,
            bridges,
        })
    }
}

/// Lock-free concurrent cache with automatic TTL eviction.
/// Uses moka for high-performance concurrent access.
pub struct QueryCache {
    entries: Cache<CacheKey, CachedDistance>,
    ttl_secs: u64,
}

impl QueryCache {
    pub fn new(max_capacity: usize, ttl_secs: u64) -> Self {
        let entries = Cache::builder()
            .max_capacity(max_capacity as u64)
            .time_to_live(Duration::from_secs(ttl_secs))
            .build();

        Self { entries, ttl_secs }
    }

    #[allow(dead_code)] // Public API for standalone usage without config
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_CACHE_SIZE, DEFAULT_TTL_SECS)
    }

    /// Get cached result, resolving node IDs to pubkey strings.
    /// Lock-free read - no contention with other readers or writers.
    pub fn get(&self, key: &CacheKey, graph: &WotGraph) -> Option<DistanceResult> {
        let revision = graph.revision();
        let cached = self.entries.get(key)?;
        if cached.revision != revision {
            return None;
        }
        let result = cached.to_result(graph, key.from_id, key.to_id)?;
        (graph.revision() == revision).then_some(result)
    }

    /// Insert result, converting pubkey strings to node IDs for compact storage.
    /// Lock-free insert - no contention with readers.
    pub fn insert(&self, key: CacheKey, result: &DistanceResult, graph: &WotGraph, revision: u64) {
        // Never relabel an old computation with a newer graph revision.
        if graph.revision() != revision {
            return;
        }
        let cached = CachedDistance::from_result(result, graph, revision);
        self.entries.insert(key, cached);
    }

    /// Invalidate all entries. Useful when graph is updated.
    #[allow(dead_code)] // Public API for cache management
    pub fn invalidate_all(&self) {
        self.entries.invalidate_all();
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.entries.entry_count() as usize,
            capacity: self.entries.policy().max_capacity().unwrap_or(0) as usize,
            ttl_secs: self.ttl_secs,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub ttl_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn create_test_graph() -> WotGraph {
        let graph = WotGraph::new();
        graph.get_or_create_node("from_pubkey");
        graph.get_or_create_node("to_pubkey");
        graph.get_or_create_node("bridge1");
        graph.get_or_create_node("bridge2");
        graph
    }

    fn make_result(from: &str, to: &str, hops: Option<u32>) -> DistanceResult {
        DistanceResult {
            from: Arc::from(from),
            to: Arc::from(to),
            hops,
            path_count: 1,
            mutual_follow: false,
            bridges: None,
        }
    }

    #[test]
    fn test_cache_insert_and_get() {
        let graph = create_test_graph();
        let cache = QueryCache::with_defaults();

        let from_id = graph.get_node_id("from_pubkey").unwrap();
        let to_id = graph.get_node_id("to_pubkey").unwrap();
        let key = CacheKey::new(from_id, to_id, 5, false);
        let result = make_result("from_pubkey", "to_pubkey", Some(2));

        cache.insert(key, &result, &graph, graph.revision());

        let cached = cache.get(&key, &graph);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().hops, Some(2));
    }

    #[test]
    fn removal_invalidates_cached_distance_and_rejects_late_insert() {
        let graph = create_test_graph();
        graph.update_follows("from_pubkey", &["to_pubkey".into()], None, None);
        let cache = QueryCache::with_defaults();
        let key = CacheKey::new(
            graph.get_node_id("from_pubkey").unwrap(),
            graph.get_node_id("to_pubkey").unwrap(),
            5,
            false,
        );
        let revision = graph.revision();
        let result = make_result("from_pubkey", "to_pubkey", Some(1));
        cache.insert(key, &result, &graph, revision);
        assert!(cache.get(&key, &graph).is_some());
        graph.update_follows("from_pubkey", &[], None, None);
        assert!(cache.get(&key, &graph).is_none());
        cache.insert(key, &result, &graph, revision);
        assert!(cache.get(&key, &graph).is_none());
    }

    #[test]
    fn test_cache_miss() {
        let graph = create_test_graph();
        let cache = QueryCache::with_defaults();

        let from_id = graph.get_node_id("from_pubkey").unwrap();
        let to_id = graph.get_node_id("to_pubkey").unwrap();
        let key = CacheKey::new(from_id, to_id, 5, false);

        let cached = cache.get(&key, &graph);
        assert!(cached.is_none());
    }

    #[test]
    fn test_cache_different_params() {
        let graph = create_test_graph();
        let cache = QueryCache::with_defaults();

        let from_id = graph.get_node_id("from_pubkey").unwrap();
        let to_id = graph.get_node_id("to_pubkey").unwrap();

        let key1 = CacheKey::new(from_id, to_id, 5, false);
        let key2 = CacheKey::new(from_id, to_id, 3, false);
        let key3 = CacheKey::new(from_id, to_id, 5, true);

        let result = make_result("from_pubkey", "to_pubkey", Some(2));
        cache.insert(key1, &result, &graph, graph.revision());

        assert!(cache.get(&key1, &graph).is_some());
        assert!(cache.get(&key2, &graph).is_none()); // Different max_hops
        assert!(cache.get(&key3, &graph).is_none()); // Different include_bridges
    }

    #[test]
    fn test_cache_expiry() {
        let graph = create_test_graph();
        let cache = QueryCache::new(100, 0); // 0 second TTL = immediate expiry

        let from_id = graph.get_node_id("from_pubkey").unwrap();
        let to_id = graph.get_node_id("to_pubkey").unwrap();
        let key = CacheKey::new(from_id, to_id, 5, false);
        let result = make_result("from_pubkey", "to_pubkey", Some(2));

        cache.insert(key, &result, &graph, graph.revision());

        // Wait for expiry + sync
        std::thread::sleep(std::time::Duration::from_millis(50));
        cache.entries.run_pending_tasks(); // Force moka to process expiry

        let cached = cache.get(&key, &graph);
        assert!(cached.is_none());
    }

    #[test]
    fn test_cache_invalidate_all() {
        let graph = WotGraph::new();
        let cache = QueryCache::with_defaults();

        let to_id = graph.get_or_create_node("to_pubkey");
        let from_ids: Vec<u32> = (0..10)
            .map(|i| graph.get_or_create_node(&format!("from{}", i)))
            .collect();

        for (i, &from_id) in from_ids.iter().enumerate() {
            let key = CacheKey::new(from_id, to_id, 5, false);
            let result = make_result(&format!("from{}", i), "to_pubkey", Some(2));
            cache.insert(key, &result, &graph, graph.revision());
        }

        // Sync to ensure entries are counted
        cache.entries.run_pending_tasks();
        assert!(cache.stats().size > 0);

        cache.invalidate_all();
        cache.entries.run_pending_tasks();

        assert_eq!(cache.stats().size, 0);
    }

    #[test]
    fn test_cache_max_capacity() {
        let graph = WotGraph::new();
        let node_ids: Vec<u32> = (0..10)
            .map(|i| graph.get_or_create_node(&format!("node{}", i)))
            .collect();

        let cache = QueryCache::new(5, 300); // Max 5 entries

        let to_id = node_ids[9];

        // Insert 10 entries
        for (i, &from_id) in node_ids.iter().enumerate() {
            let key = CacheKey::new(from_id, to_id, 5, false);
            let result = make_result(&format!("node{}", i), "node9", Some(i as u32));
            cache.insert(key, &result, &graph, graph.revision());
        }

        // Force moka to process evictions
        cache.entries.run_pending_tasks();

        // Cache should respect max capacity (moka uses TinyLFU, not strict LRU)
        // Allow some slack since eviction is probabilistic
        assert!(cache.stats().size <= 6, "Cache should be near max capacity");

        // At least some entries should be present
        let mut found = 0;
        for &from_id in &node_ids {
            let key = CacheKey::new(from_id, to_id, 5, false);
            if cache.get(&key, &graph).is_some() {
                found += 1;
            }
        }
        assert!(found >= 3, "Should have at least 3 entries remaining");
    }

    #[test]
    fn test_cache_with_bridges() {
        let graph = create_test_graph();
        let cache = QueryCache::with_defaults();

        let from_id = graph.get_node_id("from_pubkey").unwrap();
        let to_id = graph.get_node_id("to_pubkey").unwrap();
        let key = CacheKey::new(from_id, to_id, 5, true);

        let result = DistanceResult {
            from: Arc::from("from_pubkey"),
            to: Arc::from("to_pubkey"),
            hops: Some(2),
            path_count: 2,
            mutual_follow: false,
            bridges: Some(vec![Arc::from("bridge1"), Arc::from("bridge2")]),
        };

        cache.insert(key, &result, &graph, graph.revision());

        let cached = cache.get(&key, &graph).unwrap();
        assert_eq!(cached.hops, Some(2));
        assert_eq!(cached.path_count, 2);

        let bridges = cached.bridges.unwrap();
        assert_eq!(bridges.len(), 2);
        assert!(bridges.iter().any(|b| &**b == "bridge1"));
        assert!(bridges.iter().any(|b| &**b == "bridge2"));
    }
}
