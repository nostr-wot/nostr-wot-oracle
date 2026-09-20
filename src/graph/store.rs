use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use super::interner::PubkeyInterner;
use super::metrics::{LockMetrics, LockMetricsSnapshot, LockTimer};

/// Node metadata (pubkey is stored separately via interner)
#[derive(Debug, Clone)]
pub struct NodeInfo {
    #[allow(dead_code)] // Stored for event provenance/debugging
    pub kind3_event_id: Option<String>,
    pub kind3_created_at: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub nodes_with_follows: usize,
    pub mute_edge_count: usize,
    pub nodes_with_mute_lists: usize,
}

/// Public kind:10000 evidence only; encrypted entries are not represented.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MuteEvidence {
    pub source_mutes_target: bool,
    pub target_mutes_source: bool,
    pub followed_muters: Vec<Arc<str>>,
    pub source_mute_list_known: bool,
    pub target_mute_list_known: bool,
}

struct MuteList {
    ids: Vec<u32>,
    event_id: Option<String>,
    created_at: Option<i64>,
}

fn accepts_event(
    old_ts: Option<i64>,
    old_id: &Option<String>,
    new_ts: Option<i64>,
    new_id: &Option<String>,
) -> bool {
    match (old_ts, new_ts) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(old), Some(new)) => {
            new > old
                || (new == old
                    && new_id
                        .as_ref()
                        .is_some_and(|new| old_id.as_ref().is_none_or(|old| new < old)))
        }
    }
}

/// Validated database snapshot. Edge IDs index this snapshot's node vector.
pub(crate) struct SnapshotNode {
    pub pubkey: String,
    pub info: NodeInfo,
    pub follows: Vec<u32>,
    pub mutes: Option<SnapshotMuteList>,
}

pub(crate) struct SnapshotMuteList {
    pub ids: Vec<u32>,
    pub event_id: Option<String>,
    pub created_at: Option<i64>,
}

pub struct WotGraph {
    // Writers serialize event ordering, adjacency diffs, and metadata publication.
    mutation: Mutex<()>,
    revision: AtomicU64,
    interner: PubkeyInterner,
    pubkey_to_id: DashMap<Arc<str>, u32>,
    id_to_pubkey: RwLock<Vec<Arc<str>>>,
    // Sorted Vec<u32> for cache-friendly iteration and O(log n) membership checks
    follows: RwLock<Vec<Vec<u32>>>,
    followers: RwLock<Vec<Vec<u32>>>,
    node_info: RwLock<Vec<Option<NodeInfo>>>,
    mutes: RwLock<FxHashMap<u32, MuteList>>,
    lock_metrics: LockMetrics,
}

impl WotGraph {
    pub fn new() -> Self {
        Self {
            mutation: Mutex::new(()),
            revision: AtomicU64::new(0),
            interner: PubkeyInterner::new(),
            pubkey_to_id: DashMap::new(),
            id_to_pubkey: RwLock::new(Vec::new()),
            follows: RwLock::new(Vec::new()),
            followers: RwLock::new(Vec::new()),
            node_info: RwLock::new(Vec::new()),
            mutes: RwLock::new(FxHashMap::default()),
            lock_metrics: LockMetrics::new(),
        }
    }

    /// Merge a validated numeric snapshot while serializing live writers. Build
    /// reverse adjacency in source-ID order once, avoiding sorted Vec insertions.
    pub(crate) fn restore_nodes(&self, nodes: Vec<SnapshotNode>) {
        let _mutation = self.mutation.lock();
        let ids: Vec<u32> = nodes
            .iter()
            .map(|node| self.get_or_create_node_locked(&node.pubkey))
            .collect();
        let mut follows = self.follows.write();
        let mut followers = self.followers.write();
        let mut metadata = self.node_info.write();
        let mut mutes = self.mutes.write();
        let mut topology_changed = false;
        let mut mutes_changed = false;
        for (index, node) in nodes.into_iter().enumerate() {
            let id = ids[index] as usize;
            if metadata[id].as_ref().is_none_or(|old| {
                accepts_event(
                    old.kind3_created_at,
                    &old.kind3_event_id,
                    node.info.kind3_created_at,
                    &node.info.kind3_event_id,
                )
            }) {
                let mut targets: Vec<u32> = node
                    .follows
                    .into_iter()
                    .map(|target| ids[target as usize])
                    .collect();
                targets.sort_unstable();
                targets.dedup();
                topology_changed |= follows[id] != targets;
                follows[id] = targets;
                metadata[id] = Some(node.info);
            }
            if let Some(list) = node.mutes {
                if mutes.get(&(id as u32)).is_none_or(|old| {
                    accepts_event(
                        old.created_at,
                        &old.event_id,
                        list.created_at,
                        &list.event_id,
                    )
                }) {
                    let mut targets: Vec<u32> = list
                        .ids
                        .into_iter()
                        .map(|target| ids[target as usize])
                        .collect();
                    targets.sort_unstable();
                    targets.dedup();
                    mutes.insert(
                        id as u32,
                        MuteList {
                            ids: targets,
                            event_id: list.event_id,
                            created_at: list.created_at,
                        },
                    );
                    mutes_changed = true;
                }
            }
        }
        if topology_changed {
            for list in followers.iter_mut() {
                list.clear();
            }
            for (source, targets) in follows.iter().enumerate() {
                for &target in targets {
                    followers[target as usize].push(source as u32);
                }
            }
        }
        if topology_changed || mutes_changed {
            self.revision.fetch_add(1, Ordering::Release);
        }
    }

    #[allow(dead_code)] // Public API for explicit node creation and benchmarks
    pub fn get_or_create_node(&self, pubkey: &str) -> u32 {
        // Fast path: check if already exists
        if let Some(id) = self.pubkey_to_id.get(pubkey) {
            return *id;
        }

        let _mutation = self.mutation.lock();
        self.get_or_create_node_locked(pubkey)
    }

    // Lock order throughout the graph: follows, followers, pubkeys, metadata, mutes.
    // The caller holds mutation; readers never acquire mutation.
    fn get_or_create_node_locked(&self, pubkey: &str) -> u32 {
        if let Some(id) = self.pubkey_to_id.get(pubkey) {
            return *id;
        }
        let mut follows = self.follows.write();
        let mut followers = self.followers.write();
        let mut id_to_pubkey = self.id_to_pubkey.write();
        let mut node_info = self.node_info.write();

        // Double-check after acquiring write lock
        if let Some(id) = self.pubkey_to_id.get(pubkey) {
            return *id;
        }

        // Intern the pubkey string - single allocation shared everywhere
        let interned = self.interner.intern(pubkey);

        let id = id_to_pubkey.len() as u32;
        id_to_pubkey.push(interned.clone());
        follows.push(Vec::new());
        followers.push(Vec::new());
        node_info.push(None);
        self.pubkey_to_id.insert(interned, id);
        self.revision.fetch_add(1, Ordering::Release);

        id
    }

    pub fn get_node_id(&self, pubkey: &str) -> Option<u32> {
        self.pubkey_to_id.get(pubkey).map(|r| *r)
    }

    /// Get node ID and Arc<str> reference together (single lookup)
    pub fn get_node_id_and_arc(&self, pubkey: &str) -> Option<(u32, Arc<str>)> {
        self.pubkey_to_id.get(pubkey).map(|r| {
            let id = *r;
            let arc = r.key().clone();
            (id, arc)
        })
    }

    /// Get Arc<str> reference by pubkey string (no allocation if found)
    pub fn get_pubkey_arc_by_str(&self, pubkey: &str) -> Option<Arc<str>> {
        self.pubkey_to_id.get(pubkey).map(|r| r.key().clone())
    }

    /// Get pubkey as Arc<str> for internal use (no allocation)
    pub fn get_pubkey_arc(&self, id: u32) -> Option<Arc<str>> {
        let id_to_pubkey = self.id_to_pubkey.read();
        id_to_pubkey.get(id as usize).cloned()
    }

    pub fn update_follows(
        &self,
        pubkey: &str,
        follow_pubkeys: &[String],
        event_id: Option<String>,
        created_at: Option<i64>,
    ) -> bool {
        let _mutation = self.mutation.lock();
        let node_id = self.get_or_create_node_locked(pubkey);

        // Check if we should update (only if newer event)
        {
            let node_info = self.node_info.read();
            if let Some(Some(info)) = node_info.get(node_id as usize) {
                if !accepts_event(
                    info.kind3_created_at,
                    &info.kind3_event_id,
                    created_at,
                    &event_id,
                ) {
                    return false;
                }
            }
        }

        // Get or create IDs for all follows and sort them
        let mut new_follow_ids: Vec<u32> = follow_pubkeys
            .iter()
            .map(|pk| self.get_or_create_node_locked(pk))
            .collect();
        new_follow_ids.sort_unstable();
        new_follow_ids.dedup();

        // Read old follows under read lock (quick clone)
        let old_follow_ids: Vec<u32> = {
            let follows = self.follows.read();
            follows.get(node_id as usize).cloned().unwrap_or_default()
        };

        // Compute diff OUTSIDE any lock - no contention during this work
        let mut to_remove = Vec::new();
        let mut to_add = Vec::new();
        let (mut old, mut new) = (0, 0);
        while old < old_follow_ids.len() && new < new_follow_ids.len() {
            match old_follow_ids[old].cmp(&new_follow_ids[new]) {
                std::cmp::Ordering::Less => {
                    to_remove.push(old_follow_ids[old]);
                    old += 1;
                }
                std::cmp::Ordering::Greater => {
                    to_add.push(new_follow_ids[new]);
                    new += 1;
                }
                std::cmp::Ordering::Equal => {
                    old += 1;
                    new += 1;
                }
            }
        }
        to_remove.extend_from_slice(&old_follow_ids[old..]);
        to_add.extend_from_slice(&new_follow_ids[new..]);
        let edges_changed = !to_remove.is_empty() || !to_add.is_empty();

        // Minimal write lock - only actual mutations
        {
            let _timer = LockTimer::write(&self.lock_metrics);
            let mut follows = self.follows.write();
            let mut followers = self.followers.write();

            // Remove old follower references (only changed ones)
            for &old_followed_id in &to_remove {
                if let Some(follower_list) = followers.get_mut(old_followed_id as usize) {
                    if let Ok(pos) = follower_list.binary_search(&node_id) {
                        follower_list.remove(pos);
                    }
                }
            }

            // Update follows list
            if let Some(follow_list) = follows.get_mut(node_id as usize) {
                *follow_list = new_follow_ids;
            }

            // Add new follower references (only changed ones)
            for &followed_id in &to_add {
                if let Some(follower_list) = followers.get_mut(followed_id as usize) {
                    match follower_list.binary_search(&node_id) {
                        Ok(_) => {}
                        Err(pos) => follower_list.insert(pos, node_id),
                    }
                }
            }
            if edges_changed {
                self.revision.fetch_add(1, Ordering::Release);
            }
        }

        // Update node info (pubkey stored via interner, not duplicated here)
        {
            let mut node_info = self.node_info.write();
            if let Some(info_slot) = node_info.get_mut(node_id as usize) {
                *info_slot = Some(NodeInfo {
                    kind3_event_id: event_id,
                    kind3_created_at: created_at,
                });
            }
        }

        true
    }

    /// Replace the public mute list independently of follow-list timestamps.
    pub fn update_mutes(
        &self,
        pubkey: &str,
        mute_pubkeys: &[String],
        event_id: Option<String>,
        created_at: Option<i64>,
    ) -> bool {
        let _mutation = self.mutation.lock();
        let node_id = self.get_or_create_node_locked(pubkey);
        {
            let mutes = self.mutes.read();
            if let Some(old) = mutes.get(&node_id) {
                if !accepts_event(old.created_at, &old.event_id, created_at, &event_id) {
                    return false;
                }
            }
        }
        let mut ids: Vec<u32> = mute_pubkeys
            .iter()
            .map(|pk| self.get_or_create_node_locked(pk))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let mut mutes = self.mutes.write();
        mutes.insert(
            node_id,
            MuteList {
                ids,
                event_id,
                created_at,
            },
        );
        // Known empty lists and refreshed provenance are meaningful mute evidence too.
        self.revision.fetch_add(1, Ordering::Release);
        true
    }

    /// None means no public mute list has been indexed, even for a known node.
    pub fn get_mutes_page(
        &self,
        pubkey: &str,
        offset: usize,
        limit: usize,
    ) -> Option<(Vec<String>, usize)> {
        let node_id = self.get_node_id(pubkey)?;
        let pubkeys = self.id_to_pubkey.read();
        let mutes = self.mutes.read();
        let list = mutes.get(&node_id)?;
        let page = list
            .ids
            .iter()
            .skip(offset)
            .take(limit)
            .map(|&id| pubkeys[id as usize].to_string())
            .collect();
        Some((page, list.ids.len()))
    }

    pub fn mute_evidence(&self, from: &str, to: &str) -> MuteEvidence {
        let from = self.get_node_id(from);
        let to = self.get_node_id(to);
        let follows = self.follows.read();
        let pubkeys = self.id_to_pubkey.read();
        let mutes = self.mutes.read();
        let has_edge = |source: Option<u32>, target: Option<u32>| match (
            source.and_then(|id| mutes.get(&id)),
            target,
        ) {
            (Some(list), Some(target)) => list.ids.binary_search(&target).is_ok(),
            _ => false,
        };
        let followed_muters = from
            .map(|from| {
                follows[from as usize]
                    .iter()
                    .filter(|&&id| has_edge(Some(id), to))
                    .map(|&id| pubkeys[id as usize].clone())
                    .collect()
            })
            .unwrap_or_default();
        MuteEvidence {
            source_mutes_target: has_edge(from, to),
            target_mutes_source: has_edge(to, from),
            followed_muters,
            source_mute_list_known: from.is_some_and(|id| mutes.contains_key(&id)),
            target_mute_list_known: to.is_some_and(|id| mutes.contains_key(&id)),
        }
    }

    /// Monotonic topology generation. Read before and after a computation before caching it.
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    /// Resolve only the requested slice, preserving the stable internal-ID order.
    pub fn get_follows_page(
        &self,
        pubkey: &str,
        offset: usize,
        limit: usize,
    ) -> Option<(Vec<String>, usize)> {
        let node_id = self.get_node_id(pubkey)?;
        let follows = self.follows.read();
        let pubkeys = self.id_to_pubkey.read();
        let ids = follows.get(node_id as usize)?;
        let page = ids
            .iter()
            .skip(offset)
            .take(limit)
            .map(|&id| pubkeys[id as usize].to_string())
            .collect();
        Some((page, ids.len()))
    }

    /// Intersect sorted adjacency lists in linear time, resolving only matches.
    pub fn common_follows(&self, from: &str, to: &str) -> Vec<String> {
        let (Some(from), Some(to)) = (self.get_node_id(from), self.get_node_id(to)) else {
            return Vec::new();
        };
        let follows = self.follows.read();
        let pubkeys = self.id_to_pubkey.read();
        let (a, b) = (&follows[from as usize], &follows[to as usize]);
        let (mut i, mut j) = (0, 0);
        let mut common = Vec::new();
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    common.push(pubkeys[a[i] as usize].to_string());
                    i += 1;
                    j += 1;
                }
            }
        }
        common
    }

    #[allow(dead_code)] // Public API for graph inspection
    pub fn get_follows(&self, pubkey: &str) -> Option<Vec<String>> {
        let node_id = self.get_node_id(pubkey)?;
        let follows = self.follows.read();
        let id_to_pubkey = self.id_to_pubkey.read();

        follows.get(node_id as usize).map(|follow_list| {
            follow_list
                .iter()
                .filter_map(|&id| id_to_pubkey.get(id as usize).map(|arc| arc.to_string()))
                .collect()
        })
    }

    #[allow(dead_code)] // Public API for graph inspection
    pub fn get_followers(&self, pubkey: &str) -> Option<Vec<String>> {
        let node_id = self.get_node_id(pubkey)?;
        let followers = self.followers.read();
        let id_to_pubkey = self.id_to_pubkey.read();

        followers.get(node_id as usize).map(|follower_list| {
            follower_list
                .iter()
                .filter_map(|&id| id_to_pubkey.get(id as usize).map(|arc| arc.to_string()))
                .collect()
        })
    }

    /// Execute a closure with read access to both adjacency lists.
    /// Holds a single read lock for the entire operation - use for BFS traversals.
    pub fn with_adjacency<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[Vec<u32>], &[Vec<u32>]) -> R,
    {
        let _timer = LockTimer::read(&self.lock_metrics);
        let follows = self.follows.read();
        let followers = self.followers.read();
        f(&follows, &followers)
    }

    /// Batch resolve node IDs to pubkeys as Arc<str> (no allocation)
    pub fn resolve_pubkeys_arc(&self, ids: &[u32]) -> Vec<Arc<str>> {
        let id_to_pubkey = self.id_to_pubkey.read();
        ids.iter()
            .filter_map(|&id| id_to_pubkey.get(id as usize).cloned())
            .collect()
    }

    #[allow(dead_code)] // Public API for node metadata inspection
    pub fn get_node_info(&self, pubkey: &str) -> Option<NodeInfo> {
        let node_id = self.get_node_id(pubkey)?;
        let node_info = self.node_info.read();
        node_info
            .get(node_id as usize)
            .and_then(|info| info.clone())
    }

    pub fn stats(&self) -> GraphStats {
        let follows = self.follows.read();
        let id_to_pubkey = self.id_to_pubkey.read();

        let node_count = id_to_pubkey.len();
        let edge_count: usize = follows.iter().map(|list| list.len()).sum();
        let nodes_with_follows = follows.iter().filter(|list| !list.is_empty()).count();

        let mutes = self.mutes.read();
        GraphStats {
            mute_edge_count: mutes.values().map(|list| list.ids.len()).sum(),
            nodes_with_mute_lists: mutes.len(),
            node_count,
            edge_count,
            nodes_with_follows,
        }
    }

    /// Get lock contention metrics
    pub fn lock_metrics(&self) -> LockMetricsSnapshot {
        self.lock_metrics.snapshot()
    }

    /// Reset lock metrics (useful after warmup period)
    #[allow(dead_code)] // Public API for metrics management
    pub fn reset_lock_metrics(&self) {
        self.lock_metrics.reset();
    }
}

impl Default for WotGraph {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mute_evidence_is_independent_and_versioned() {
        let graph = WotGraph::new();
        graph.update_follows(
            "a",
            &["b".into(), "c".into()],
            Some("follow".into()),
            Some(500),
        );
        let revision = graph.revision();
        assert!(graph.update_mutes("a", &["target".into()], Some("ff".into()), Some(10)));
        assert!(graph.revision() > revision);
        assert!(graph.update_mutes(
            "b",
            &["target".into(), "target".into()],
            Some("aa".into()),
            Some(10)
        ));
        assert!(graph.update_mutes("target", &[], Some("aa".into()), Some(10)));
        let evidence = graph.mute_evidence("a", "target");
        assert!(evidence.source_mutes_target);
        assert!(!evidence.target_mutes_source);
        assert!(evidence.source_mute_list_known && evidence.target_mute_list_known);
        assert_eq!(evidence.followed_muters, vec![Arc::<str>::from("b")]);
        assert_eq!(
            graph.get_mutes_page("b", 0, 10),
            Some((vec!["target".into()], 1))
        );
        assert_eq!(graph.get_mutes_page("c", 0, 10), None);
        assert_eq!(graph.get_mutes_page("target", 0, 10), Some((vec![], 0)));
        assert!(!graph.update_mutes("a", &[], Some("ff".into()), Some(10)));
        assert!(!graph.update_mutes("a", &[], Some("00".into()), Some(9)));
        assert!(graph.update_mutes("a", &[], Some("00".into()), Some(10)));
        assert!(!graph.mute_evidence("a", "target").source_mutes_target);
        assert_eq!(graph.get_follows("a"), Some(vec!["b".into(), "c".into()]));
        assert_eq!(graph.stats().mute_edge_count, 1);
        assert_eq!(graph.stats().nodes_with_mute_lists, 3);
        assert!(
            !graph
                .mute_evidence("missing", "absent")
                .source_mute_list_known
        );
    }

    #[test]
    fn pages_intersections_and_revisions() {
        let graph = WotGraph::new();
        let initial = graph.revision();
        graph.get_or_create_node("a");
        assert!(graph.revision() > initial);
        let before = graph.revision();
        graph.get_or_create_node("a");
        assert_eq!(graph.revision(), before);
        graph.update_follows(
            "a",
            &["b".into(), "c".into(), "d".into()],
            Some("1".into()),
            Some(1),
        );
        let topology_revision = graph.revision();
        assert!(topology_revision > before);
        graph.update_follows(
            "a",
            &["b".into(), "c".into(), "d".into()],
            Some("2".into()),
            Some(2),
        );
        assert_eq!(graph.revision(), topology_revision);
        assert_eq!(
            graph.get_follows_page("a", 1, 1),
            Some((vec!["c".into()], 3))
        );
        assert_eq!(
            graph.get_follows_page("a", usize::MAX, 10),
            Some((vec![], 3))
        );
        assert_eq!(graph.get_follows_page("missing", 0, 10), None);
        graph.update_follows("b", &["c".into(), "d".into()], None, None);
        assert_eq!(graph.common_follows("a", "b"), vec!["c", "d"]);
        assert!(graph.common_follows("a", "missing").is_empty());
        graph.update_follows("a", &[], Some("3".into()), Some(3));
        assert!(graph.revision() > topology_revision);
    }

    #[test]
    fn concurrent_readers_and_writers_preserve_topology() {
        use std::{sync::mpsc, thread, time::Duration};
        let graph = Arc::new(WotGraph::new());
        graph.get_or_create_node("a");
        let (done, results) = mpsc::channel();
        let mut handles = Vec::new();
        for worker in 0..6 {
            let graph = graph.clone();
            let done = done.clone();
            handles.push(thread::spawn(move || {
                for iteration in 0..100 {
                    if worker < 3 {
                        let ts = iteration * 3 + worker;
                        graph.update_follows(
                            "a",
                            &[format!("n{ts}")],
                            Some(format!("{ts:064x}")),
                            Some(ts),
                        );
                        graph.update_mutes(
                            "a",
                            &[format!("m{ts}")],
                            Some(format!("{ts:064x}")),
                            Some(ts),
                        );
                    } else {
                        graph.stats();
                        graph.get_follows("a");
                        graph.get_followers("a");
                        graph.get_mutes_page("a", 0, 10);
                        graph.mute_evidence("a", "n299");
                        graph.with_adjacency(|follows, followers| {
                            let ids: Vec<_> = (0..follows.len() as u32).collect();
                            assert_eq!(graph.resolve_pubkeys_arc(&ids).len(), follows.len());
                            for (a, outgoing) in follows.iter().enumerate() {
                                for &b in outgoing {
                                    assert!(followers[b as usize]
                                        .binary_search(&(a as u32))
                                        .is_ok());
                                }
                            }
                            for (b, incoming) in followers.iter().enumerate() {
                                for &a in incoming {
                                    assert!(follows[a as usize].binary_search(&(b as u32)).is_ok());
                                }
                            }
                        });
                    }
                }
                done.send(()).unwrap();
            }));
        }
        for _ in 0..6 {
            results
                .recv_timeout(Duration::from_secs(10))
                .expect("graph operation stalled");
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(graph.get_follows("a"), Some(vec!["n299".into()]));
        assert_eq!(
            graph.get_mutes_page("a", 0, 10),
            Some((vec!["m299".into()], 1))
        );
        assert_eq!(
            graph.get_node_info("a").unwrap().kind3_created_at,
            Some(299)
        );
    }

    #[test]
    fn equal_timestamp_can_restore_missing_legacy_event_id() {
        let graph = WotGraph::new();
        graph.update_follows("a", &[], None, Some(100));
        assert!(graph.update_follows("a", &["b".into()], Some("ff".into()), Some(100)));
        assert!(!graph.update_follows("a", &[], None, Some(100)));
        graph.update_mutes("a", &[], None, Some(100));
        assert!(graph.update_mutes("a", &["b".into()], Some("ff".into()), Some(100)));
        assert!(!graph.update_mutes("a", &[], None, Some(100)));
    }

    #[test]
    fn equal_timestamp_uses_lowest_event_id() {
        let graph = WotGraph::new();
        assert!(graph.update_follows("a", &["b".into()], Some("ff".repeat(32)), Some(100)));
        assert!(graph.update_follows("a", &["c".into()], Some("00".repeat(32)), Some(100)));
        assert!(!graph.update_follows("a", &["d".into()], Some("ff".repeat(32)), Some(100)));
        assert_eq!(graph.get_follows("a"), Some(vec!["c".into()]));
    }

    #[test]
    fn test_create_nodes() {
        let graph = WotGraph::new();
        let id1 = graph.get_or_create_node("pubkey1");
        let id2 = graph.get_or_create_node("pubkey2");
        let id1_again = graph.get_or_create_node("pubkey1");

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
    }

    #[test]
    fn test_update_follows() {
        let graph = WotGraph::new();

        graph.update_follows(
            "alice",
            &["bob".to_string(), "carol".to_string()],
            Some("event1".to_string()),
            Some(1000),
        );

        let follows = graph.get_follows("alice").unwrap();
        assert_eq!(follows.len(), 2);
        assert!(follows.contains(&"bob".to_string()));
        assert!(follows.contains(&"carol".to_string()));

        let bob_followers = graph.get_followers("bob").unwrap();
        assert!(bob_followers.contains(&"alice".to_string()));
    }

    #[test]
    fn test_replace_follows() {
        let graph = WotGraph::new();

        graph.update_follows("alice", &["bob".to_string()], None, Some(1000));
        graph.update_follows("alice", &["carol".to_string()], None, Some(2000));

        let follows = graph.get_follows("alice").unwrap();
        assert_eq!(follows.len(), 1);
        assert!(follows.contains(&"carol".to_string()));

        // Bob should no longer have alice as follower
        let bob_followers = graph.get_followers("bob").unwrap();
        assert!(!bob_followers.contains(&"alice".to_string()));
    }

    #[test]
    fn test_skip_old_event() {
        let graph = WotGraph::new();

        graph.update_follows("alice", &["bob".to_string()], None, Some(2000));
        let result = graph.update_follows("alice", &["carol".to_string()], None, Some(1000));

        assert!(!result); // Should skip old event

        let follows = graph.get_follows("alice").unwrap();
        assert!(follows.contains(&"bob".to_string()));
        assert!(!follows.contains(&"carol".to_string()));
    }

    #[test]
    fn test_stats() {
        let graph = WotGraph::new();

        graph.update_follows(
            "alice",
            &["bob".to_string(), "carol".to_string()],
            None,
            None,
        );
        graph.update_follows("bob", &["carol".to_string()], None, None);

        let stats = graph.stats();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 3);
        assert_eq!(stats.nodes_with_follows, 2);
    }

    #[test]
    fn test_sorted_follows() {
        let graph = WotGraph::new();

        // Insert in random order
        graph.update_follows(
            "alice",
            &[
                "zebra".to_string(),
                "apple".to_string(),
                "mango".to_string(),
            ],
            None,
            None,
        );

        // Internal IDs should be sorted
        let alice_id = graph.get_node_id("alice").unwrap();

        // Verify sorted order using with_adjacency
        graph.with_adjacency(|follows, _| {
            let follows_ids = &follows[alice_id as usize];
            for i in 1..follows_ids.len() {
                assert!(
                    follows_ids[i - 1] < follows_ids[i],
                    "follows should be sorted"
                );
            }
        });
    }

    #[test]
    fn test_binary_search_is_direct_follow() {
        let graph = WotGraph::new();

        graph.update_follows(
            "alice",
            &["bob".to_string(), "carol".to_string(), "dave".to_string()],
            None,
            None,
        );

        let alice_id = graph.get_node_id("alice").unwrap();
        let bob_id = graph.get_node_id("bob").unwrap();
        let carol_id = graph.get_node_id("carol").unwrap();
        let eve_id = graph.get_or_create_node("eve");

        // Test using with_adjacency and binary search
        graph.with_adjacency(|follows, _| {
            let alice_follows = &follows[alice_id as usize];
            assert!(alice_follows.binary_search(&bob_id).is_ok());
            assert!(alice_follows.binary_search(&carol_id).is_ok());
            assert!(alice_follows.binary_search(&eve_id).is_err());

            let bob_follows = &follows[bob_id as usize];
            assert!(bob_follows.binary_search(&alice_id).is_err());
        });
    }
}
