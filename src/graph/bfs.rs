use super::WotGraph;
use rustc_hash::{FxHashMap, FxHashSet};
use std::cell::RefCell;
use std::sync::Arc;

// Initial capacities for preallocated structures
const VISITED_CAPACITY: usize = 8192;
const FRONTIER_CAPACITY: usize = 1024;
const MEETING_NODES_CAPACITY: usize = 64;
const BRIDGE_CAPACITY: usize = 64;

/// Reusable BFS state to avoid allocations per query
/// Uses double-buffered Vec (current/next) instead of VecDeque for better cache locality
struct BfsState {
    fwd_visited: FxHashMap<u32, (u32, u64)>,
    fwd_current: Vec<u32>,
    fwd_next: Vec<u32>,
    bwd_visited: FxHashMap<u32, (u32, u64)>,
    bwd_current: Vec<u32>,
    bwd_next: Vec<u32>,
    meeting_nodes: Vec<(u32, u64, u64)>,
    // Reusable structures for bridge deduplication (avoids per-query allocation)
    bridge_set: FxHashSet<u32>,
    bridge_ids: Vec<u32>,
}

impl BfsState {
    fn new() -> Self {
        Self {
            fwd_visited: FxHashMap::with_capacity_and_hasher(VISITED_CAPACITY, Default::default()),
            fwd_current: Vec::with_capacity(FRONTIER_CAPACITY),
            fwd_next: Vec::with_capacity(FRONTIER_CAPACITY),
            bwd_visited: FxHashMap::with_capacity_and_hasher(VISITED_CAPACITY, Default::default()),
            bwd_current: Vec::with_capacity(FRONTIER_CAPACITY),
            bwd_next: Vec::with_capacity(FRONTIER_CAPACITY),
            meeting_nodes: Vec::with_capacity(MEETING_NODES_CAPACITY),
            bridge_set: FxHashSet::with_capacity_and_hasher(BRIDGE_CAPACITY, Default::default()),
            bridge_ids: Vec::with_capacity(BRIDGE_CAPACITY),
        }
    }

    /// Clear all structures while retaining allocated capacity
    fn clear(&mut self) {
        self.fwd_visited.clear();
        self.fwd_current.clear();
        self.fwd_next.clear();
        self.bwd_visited.clear();
        self.bwd_current.clear();
        self.bwd_next.clear();
        self.meeting_nodes.clear();
        self.bridge_set.clear();
        self.bridge_ids.clear();
    }
}

thread_local! {
    static BFS_STATE: RefCell<BfsState> = RefCell::new(BfsState::new());
}

#[derive(Debug, Clone)]
pub struct DistanceQuery {
    pub from: Arc<str>,
    pub to: Arc<str>,
    pub max_hops: u8,
    pub include_bridges: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DistanceResult {
    pub from: Arc<str>,
    pub to: Arc<str>,
    pub hops: Option<u32>,
    pub path_count: u64,
    pub mutual_follow: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bridges: Option<Vec<Arc<str>>>,
}

impl DistanceResult {
    pub fn not_found(from: Arc<str>, to: Arc<str>) -> Self {
        Self {
            from,
            to,
            hops: None,
            path_count: 0,
            mutual_follow: false,
            bridges: None,
        }
    }

    pub fn same_node(pubkey: Arc<str>) -> Self {
        Self {
            from: Arc::clone(&pubkey),
            to: pubkey,
            hops: Some(0),
            path_count: 1,
            mutual_follow: false,
            bridges: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PathQuery {
    pub from: Arc<str>,
    pub to: Arc<str>,
    pub max_hops: u8,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PathResult {
    pub from: Arc<str>,
    pub to: Arc<str>,
    pub path: Option<Vec<Arc<str>>>,
}

pub fn compute_distance(graph: &WotGraph, query: &DistanceQuery) -> DistanceResult {
    // Handle same node case
    if query.from == query.to {
        // Get Arc<str> reference from graph (or use query's Arc directly - just ref count bump)
        let pubkey_arc = graph.get_pubkey_arc_by_str(&query.from)
            .unwrap_or_else(|| Arc::clone(&query.from));
        return DistanceResult::same_node(pubkey_arc);
    }

    // Get node IDs and Arc<str> references (uses DashMap, separate from adjacency lock)
    let (from_id, from_arc) = match graph.get_node_id_and_arc(&query.from) {
        Some(pair) => pair,
        None => return DistanceResult::not_found(
            Arc::clone(&query.from),
            Arc::clone(&query.to),
        ),
    };

    let (to_id, to_arc) = match graph.get_node_id_and_arc(&query.to) {
        Some(pair) => pair,
        None => return DistanceResult::not_found(
            Arc::clone(&from_arc),
            Arc::clone(&query.to),
        ),
    };

    // Single read lock for entire BFS traversal
    graph.with_adjacency(|follows, followers| {
        // Direct follow check via binary search on sorted list
        let is_direct = |from: u32, to: u32| -> bool {
            follows
                .get(from as usize)
                .map(|list| list.binary_search(&to).is_ok())
                .unwrap_or(false)
        };

        // Check for mutual follow
        let mutual_follow = is_direct(from_id, to_id) && is_direct(to_id, from_id);

        // Check for direct follow (hops = 1)
        if is_direct(from_id, to_id) {
            return DistanceResult {
                from: Arc::clone(&from_arc),
                to: Arc::clone(&to_arc),
                hops: Some(1),
                path_count: 1,
                mutual_follow,
                bridges: if query.include_bridges { Some(vec![]) } else { None },
            };
        }

        // Bidirectional BFS using thread-local state (zero allocation)
        BFS_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.clear();
            bidirectional_bfs(
                &mut state,
                follows,
                followers,
                from_id,
                to_id,
                query.max_hops,
                query.include_bridges,
                mutual_follow,
                Arc::clone(&from_arc),
                Arc::clone(&to_arc),
                graph, // For resolve_pubkeys_arc at end
            )
        })
    })
}

#[allow(clippy::too_many_arguments)] // BFS state is intentionally flat for performance
fn bidirectional_bfs(
    state: &mut BfsState,
    follows: &[Vec<u32>],
    followers: &[Vec<u32>],
    from_id: u32,
    to_id: u32,
    max_hops: u8,
    include_bridges: bool,
    mutual_follow: bool,
    from_arc: Arc<str>,
    to_arc: Arc<str>,
    graph: &WotGraph, // Only for resolve_pubkeys_arc at end
) -> DistanceResult {
    state.fwd_visited.insert(from_id, (0, 1));
    state.fwd_current.push(from_id);

    state.bwd_visited.insert(to_id, (0, 1));
    state.bwd_current.push(to_id);

    let mut fwd_dist = 0u32;
    let mut bwd_dist = 0u32;
    let mut best_distance: Option<u32> = None;

    'outer: while !state.fwd_current.is_empty() || !state.bwd_current.is_empty() {
        // Check if we should stop
        let current_min_possible = fwd_dist + bwd_dist;
        if let Some(best) = best_distance {
            if current_min_possible >= best {
                break;
            }
        }

        if current_min_possible > u32::from(max_hops) {
            break;
        }

        // Expand smaller frontier
        let expand_forward = if state.fwd_current.is_empty() {
            false
        } else if state.bwd_current.is_empty() {
            true
        } else {
            state.fwd_current.len() <= state.bwd_current.len()
        };

        if expand_forward {
            fwd_dist += 1;

            // Process all nodes in current level (contiguous memory iteration)
            for i in 0..state.fwd_current.len() {
                let node = state.fwd_current[i];
                let (_, node_paths) = state.fwd_visited[&node];

                // Direct access to neighbors - no lock, no clone
                for &neighbor in &follows[node as usize] {
                    // Check if we've met the backward search
                    if let Some(&(bwd_d, bwd_paths)) = state.bwd_visited.get(&neighbor) {
                        let total_dist = fwd_dist + bwd_d;

                        if best_distance.is_none_or(|best| total_dist < best) {
                            best_distance = Some(total_dist);
                            state.meeting_nodes.clear();
                        }

                        if best_distance == Some(total_dist) {
                            state.meeting_nodes.push((neighbor, node_paths, bwd_paths));
                        }

                        // Early exit: if we don't need bridges, one path is enough
                        if !include_bridges {
                            break 'outer;
                        }
                    }

                    // Add to next frontier if not visited (single lookup via entry API)
                    match state.fwd_visited.entry(neighbor) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert((fwd_dist, node_paths));
                            state.fwd_next.push(neighbor);
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            // Update path count if same distance
                            let (existing_dist, existing_paths) = e.get_mut();
                            if *existing_dist == fwd_dist {
                                *existing_paths += node_paths;
                            }
                        }
                    }
                }
            }

            // Swap buffers: next becomes current
            state.fwd_current.clear();
            std::mem::swap(&mut state.fwd_current, &mut state.fwd_next);
        } else {
            bwd_dist += 1;

            // Process all nodes in current level (contiguous memory iteration)
            for i in 0..state.bwd_current.len() {
                let node = state.bwd_current[i];
                let (_, node_paths) = state.bwd_visited[&node];

                // Direct access to neighbors - no lock, no clone
                for &neighbor in &followers[node as usize] {
                    // Check if we've met the forward search
                    if let Some(&(fwd_d, fwd_paths)) = state.fwd_visited.get(&neighbor) {
                        let total_dist = fwd_d + bwd_dist;

                        if best_distance.is_none_or(|best| total_dist < best) {
                            best_distance = Some(total_dist);
                            state.meeting_nodes.clear();
                        }

                        if best_distance == Some(total_dist) {
                            state.meeting_nodes.push((neighbor, fwd_paths, node_paths));
                        }

                        // Early exit: if we don't need bridges, one path is enough
                        if !include_bridges {
                            break 'outer;
                        }
                    }

                    // Add to next frontier if not visited (single lookup via entry API)
                    match state.bwd_visited.entry(neighbor) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert((bwd_dist, node_paths));
                            state.bwd_next.push(neighbor);
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            // Update path count if same distance
                            let (existing_dist, existing_paths) = e.get_mut();
                            if *existing_dist == bwd_dist {
                                *existing_paths += node_paths;
                            }
                        }
                    }
                }
            }

            // Swap buffers: next becomes current
            state.bwd_current.clear();
            std::mem::swap(&mut state.bwd_current, &mut state.bwd_next);
        }
    }

    match best_distance {
        Some(hops) if hops as u8 <= max_hops => {
            // Calculate total path count
            let path_count: u64 = state.meeting_nodes
                .iter()
                .map(|(_, fwd_paths, bwd_paths)| fwd_paths * bwd_paths)
                .sum();

            // Collect unique bridge nodes using reusable structures (no allocation)
            let bridges = if include_bridges {
                // Deduplicate meeting node IDs
                for (id, _, _) in &state.meeting_nodes {
                    if state.bridge_set.insert(*id) {
                        state.bridge_ids.push(*id);
                    }
                }
                Some(graph.resolve_pubkeys_arc(&state.bridge_ids))
            } else {
                None
            };

            DistanceResult {
                from: from_arc,
                to: to_arc,
                hops: Some(hops),
                path_count,
                mutual_follow,
                bridges,
            }
        }
        Some(_) | None => DistanceResult::not_found(from_arc, to_arc),
    }
}

/// Compute the shortest path between two nodes, returning the actual path
pub fn compute_path(graph: &WotGraph, query: &PathQuery) -> PathResult {
    // Handle same node case
    if query.from == query.to {
        let pubkey_arc = graph.get_pubkey_arc_by_str(&query.from)
            .unwrap_or_else(|| Arc::clone(&query.from));
        return PathResult {
            from: Arc::clone(&pubkey_arc),
            to: pubkey_arc,
            path: Some(vec![]),
        };
    }

    // Get node IDs and Arc<str> references
    let (from_id, from_arc) = match graph.get_node_id_and_arc(&query.from) {
        Some(pair) => pair,
        None => return PathResult {
            from: Arc::clone(&query.from),
            to: Arc::clone(&query.to),
            path: None,
        },
    };

    let (to_id, to_arc) = match graph.get_node_id_and_arc(&query.to) {
        Some(pair) => pair,
        None => return PathResult {
            from: Arc::clone(&from_arc),
            to: Arc::clone(&query.to),
            path: None,
        },
    };

    // Single read lock for entire BFS traversal
    graph.with_adjacency(|follows, followers| {
        // Direct follow check via binary search on sorted list
        let is_direct = |from: u32, to: u32| -> bool {
            follows
                .get(from as usize)
                .map(|list| list.binary_search(&to).is_ok())
                .unwrap_or(false)
        };

        // Check for direct follow (hops = 1)
        if is_direct(from_id, to_id) {
            return PathResult {
                from: Arc::clone(&from_arc),
                to: Arc::clone(&to_arc),
                path: Some(vec![]),
            };
        }

        // BFS with parent tracking for path reconstruction
        let mut fwd_parent: FxHashMap<u32, u32> = FxHashMap::default();
        let mut bwd_parent: FxHashMap<u32, u32> = FxHashMap::default();
        let mut fwd_visited: FxHashSet<u32> = FxHashSet::default();
        let mut bwd_visited: FxHashSet<u32> = FxHashSet::default();
        let mut fwd_current: Vec<u32> = vec![from_id];
        let mut bwd_current: Vec<u32> = vec![to_id];
        let mut fwd_next: Vec<u32> = Vec::new();
        let mut bwd_next: Vec<u32> = Vec::new();

        fwd_visited.insert(from_id);
        bwd_visited.insert(to_id);

        let mut meeting_node: Option<u32> = None;
        let mut fwd_dist = 0u32;
        let mut bwd_dist = 0u32;

        'outer: while !fwd_current.is_empty() || !bwd_current.is_empty() {
            let current_min_possible = fwd_dist + bwd_dist;
            if current_min_possible as u8 > query.max_hops {
                break;
            }

            // Expand smaller frontier
            let expand_forward = if fwd_current.is_empty() {
                false
            } else if bwd_current.is_empty() {
                true
            } else {
                fwd_current.len() <= bwd_current.len()
            };

            if expand_forward {
                fwd_dist += 1;
                for &node in &fwd_current {
                    for &neighbor in &follows[node as usize] {
                        if bwd_visited.contains(&neighbor) {
                            fwd_parent.insert(neighbor, node);
                            meeting_node = Some(neighbor);
                            break 'outer;
                        }
                        if !fwd_visited.contains(&neighbor) {
                            fwd_visited.insert(neighbor);
                            fwd_parent.insert(neighbor, node);
                            fwd_next.push(neighbor);
                        }
                    }
                }
                fwd_current.clear();
                std::mem::swap(&mut fwd_current, &mut fwd_next);
            } else {
                bwd_dist += 1;
                for &node in &bwd_current {
                    for &neighbor in &followers[node as usize] {
                        if fwd_visited.contains(&neighbor) {
                            bwd_parent.insert(neighbor, node);
                            meeting_node = Some(neighbor);
                            break 'outer;
                        }
                        if !bwd_visited.contains(&neighbor) {
                            bwd_visited.insert(neighbor);
                            bwd_parent.insert(neighbor, node);
                            bwd_next.push(neighbor);
                        }
                    }
                }
                bwd_current.clear();
                std::mem::swap(&mut bwd_current, &mut bwd_next);
            }
        }

        match meeting_node {
            Some(meet) => {
                // Reconstruct path from from_id to meeting point
                let mut path_ids: Vec<u32> = Vec::new();
                let mut current = meet;
                while current != from_id {
                    if let Some(&parent) = fwd_parent.get(&current) {
                        path_ids.push(current);
                        current = parent;
                    } else {
                        break;
                    }
                }
                path_ids.reverse();

                // Reconstruct path from meeting point to to_id
                current = meet;
                while current != to_id {
                    if let Some(&child) = bwd_parent.get(&current) {
                        if child != to_id {
                            path_ids.push(child);
                        }
                        current = child;
                    } else {
                        break;
                    }
                }

                // Convert IDs to pubkeys
                let path = graph.resolve_pubkeys_arc(&path_ids);

                PathResult {
                    from: from_arc,
                    to: to_arc,
                    path: Some(path),
                }
            }
            None => PathResult {
                from: from_arc,
                to: to_arc,
                path: None,
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_graph() -> WotGraph {
        // Create a simple test graph:
        // alice -> bob -> carol -> dave
        //       -> eve -> carol
        let graph = WotGraph::new();

        graph.update_follows("alice", &["bob".to_string(), "eve".to_string()], None, None);
        graph.update_follows("bob", &["carol".to_string()], None, None);
        graph.update_follows("eve", &["carol".to_string()], None, None);
        graph.update_follows("carol", &["dave".to_string()], None, None);

        graph
    }

    #[test]
    fn test_same_node() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("alice"),
            max_hops: 5,
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, Some(0));
        assert_eq!(result.path_count, 1);
    }

    #[test]
    fn test_direct_follow() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("bob"),
            max_hops: 5,
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, Some(1));
        assert_eq!(result.path_count, 1);
    }

    #[test]
    fn test_two_hops() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("carol"),
            max_hops: 5,
            include_bridges: true,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, Some(2));
        assert_eq!(result.path_count, 2); // Two paths: alice->bob->carol and alice->eve->carol

        let bridges = result.bridges.unwrap();
        assert_eq!(bridges.len(), 2);
        assert!(bridges.iter().any(|b| &**b == "bob") || bridges.iter().any(|b| &**b == "eve"));
    }

    #[test]
    fn test_three_hops() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("dave"),
            max_hops: 5,
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, Some(3));
    }

    #[test]
    fn test_not_found() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("unknown"),
            max_hops: 5,
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, None);
        assert_eq!(result.path_count, 0);
    }

    #[test]
    fn test_max_hops_exceeded() {
        let graph = create_test_graph();
        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("dave"),
            max_hops: 2, // dave is 3 hops away
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, None);
    }

    #[test]
    fn test_mutual_follow() {
        let graph = WotGraph::new();
        graph.update_follows("alice", &["bob".to_string()], None, None);
        graph.update_follows("bob", &["alice".to_string()], None, None);

        let query = DistanceQuery {
            from: Arc::from("alice"),
            to: Arc::from("bob"),
            max_hops: 5,
            include_bridges: false,
        };

        let result = compute_distance(&graph, &query);
        assert_eq!(result.hops, Some(1));
        assert!(result.mutual_follow);
    }

    #[test]
    fn test_multiple_queries_reuse_state() {
        // Verify that multiple queries work correctly with state reuse
        let graph = create_test_graph();

        for _ in 0..10 {
            let query1 = DistanceQuery {
                from: Arc::from("alice"),
                to: Arc::from("carol"),
                max_hops: 5,
                include_bridges: false,
            };
            let result1 = compute_distance(&graph, &query1);
            assert_eq!(result1.hops, Some(2));

            let query2 = DistanceQuery {
                from: Arc::from("alice"),
                to: Arc::from("dave"),
                max_hops: 5,
                include_bridges: false,
            };
            let result2 = compute_distance(&graph, &query2);
            assert_eq!(result2.hops, Some(3));
        }
    }
}
