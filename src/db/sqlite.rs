use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::{debug, info};

use crate::graph::WotGraph;

pub struct Database {
    conn: Mutex<Connection>,
}

/// Batch update item for efficient multi-event persistence
pub struct FollowUpdateBatch<'a> {
    pub pubkey: &'a str,
    pub follows: &'a [String],
    pub event_id: Option<&'a str>,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // Public API for sync state inspection
pub struct SyncState {
    pub relay_url: String,
    pub last_event_time: Option<i64>,
    pub last_sync_at: Option<i64>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL mode for better concurrent access
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-64000; PRAGMA temp_store=MEMORY; PRAGMA mmap_size=268435456;")?;

        let db = Self {
            conn: Mutex::new(conn),
        };

        db.init_schema()?;

        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock();

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY,
                pubkey TEXT NOT NULL UNIQUE,
                kind3_event_id TEXT,
                kind3_created_at INTEGER,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_pubkey ON nodes(pubkey);

            CREATE TABLE IF NOT EXISTS edges (
                follower_id INTEGER NOT NULL,
                followed_id INTEGER NOT NULL,
                PRIMARY KEY (follower_id, followed_id),
                FOREIGN KEY (follower_id) REFERENCES nodes(id),
                FOREIGN KEY (followed_id) REFERENCES nodes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_edges_follower ON edges(follower_id);
            CREATE INDEX IF NOT EXISTS idx_edges_followed ON edges(followed_id);

            CREATE TABLE IF NOT EXISTS mute_lists (
                follower_id INTEGER PRIMARY KEY REFERENCES nodes(id),
                event_id TEXT,
                created_at INTEGER,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mute_edges (
                follower_id INTEGER NOT NULL REFERENCES nodes(id),
                followed_id INTEGER NOT NULL REFERENCES nodes(id),
                PRIMARY KEY (follower_id, followed_id)
            );
            CREATE INDEX IF NOT EXISTS idx_mute_edges_followed ON mute_edges(followed_id);

            CREATE TABLE IF NOT EXISTS sync_state (
                relay_url TEXT PRIMARY KEY,
                last_event_time INTEGER,
                last_sync_at INTEGER
            );
        "#,
        )?;

        info!("Database schema initialized");
        Ok(())
    }

    pub fn load_graph(&self, graph: &WotGraph) -> Result<()> {
        let conn = self.conn.lock();
        let mut node_stmt = conn.prepare(
            "SELECT id, pubkey, kind3_event_id, kind3_created_at FROM nodes ORDER BY id",
        )?;
        let nodes: Vec<(i64, String, Option<String>, Option<i64>)> = node_stmt
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        info!("Loading {} nodes from database", nodes.len());
        let node_indices: HashMap<i64, usize> = nodes
            .iter()
            .enumerate()
            .map(|(index, (id, _, _, _))| (*id, index))
            .collect();
        // Read numeric edges directly: no GROUP_CONCAT buffers or pubkey SQL joins.
        let mut edge_stmt = conn.prepare(
            "SELECT follower_id, followed_id FROM edges ORDER BY follower_id, followed_id",
        )?;
        let edges: Vec<(i64, i64)> = edge_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut mute_stmt = conn.prepare(
            "SELECT follower_id, event_id, created_at FROM mute_lists ORDER BY follower_id",
        )?;
        let mute_lists: Vec<(i64, Option<String>, Option<i64>)> = mute_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut mute_edge_stmt = conn.prepare(
            "SELECT follower_id, followed_id FROM mute_edges ORDER BY follower_id, followed_id",
        )?;
        let mute_edges: Vec<(i64, i64)> = mute_edge_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mute_authors: HashSet<i64> = mute_lists.iter().map(|list| list.0).collect();
        for (from, to) in &mute_edges {
            anyhow::ensure!(
                mute_authors.contains(from) && node_indices.contains_key(to),
                "Database contains invalid mute edges"
            );
        }
        for (author, _, _) in &mute_lists {
            anyhow::ensure!(
                node_indices.contains_key(author),
                "Database contains invalid mute metadata"
            );
        }
        // Validate the complete snapshot before changing the graph.
        for (from, to) in &edges {
            anyhow::ensure!(
                node_indices.contains_key(from) && node_indices.contains_key(to),
                "Database contains an edge referencing a missing node"
            );
        }
        for (_, pubkey, _, _) in &nodes {
            graph.get_or_create_node(pubkey);
        }
        let mut edge_index = 0;
        for (id, pubkey, event_id, created_at) in &nodes {
            let mut follows = Vec::new();
            while edge_index < edges.len() && edges[edge_index].0 == *id {
                let followed_index = node_indices[&edges[edge_index].1];
                follows.push(nodes[followed_index].1.clone());
                edge_index += 1;
            }
            // Empty follow lists still carry replaceable-event provenance.
            graph.update_follows(pubkey, &follows, event_id.clone(), *created_at);
        }
        let mut mute_index = 0;
        for (id, event_id, created_at) in mute_lists {
            let mut muted = Vec::new();
            while mute_index < mute_edges.len() && mute_edges[mute_index].0 == id {
                muted.push(nodes[node_indices[&mute_edges[mute_index].1]].1.clone());
                mute_index += 1;
            }
            graph.update_mutes(&nodes[node_indices[&id]].1, &muted, event_id, created_at);
        }
        info!("Loaded {} edges from database", edges.len());
        Ok(())
    }

    #[allow(dead_code)] // Public API for direct node manipulation
    pub fn upsert_node(
        &self,
        pubkey: &str,
        kind3_event_id: Option<&str>,
        kind3_created_at: Option<i64>,
    ) -> Result<i64> {
        let conn = self.conn.lock();
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            r#"
            INSERT INTO nodes (pubkey, kind3_event_id, kind3_created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(pubkey) DO UPDATE SET
                kind3_event_id = COALESCE(?2, kind3_event_id),
                kind3_created_at = COALESCE(?3, kind3_created_at),
                updated_at = ?4
            "#,
            params![pubkey, kind3_event_id, kind3_created_at, now],
        )?;

        let id = conn.query_row(
            "SELECT id FROM nodes WHERE pubkey = ?1",
            params![pubkey],
            |row| row.get(0),
        )?;

        Ok(id)
    }

    #[allow(dead_code)] // Public API for direct follow list updates
    pub fn update_follows(
        &self,
        follower_pubkey: &str,
        follows: &[String],
        event_id: Option<&str>,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.update_follows_batch(&[FollowUpdateBatch {
            pubkey: follower_pubkey,
            follows,
            event_id,
            created_at,
        }])?;
        Ok(())
    }

    /// Persist each author's winning event atomically, changing only affected edges.
    /// Returns the number of authors whose updates were accepted.
    pub fn update_follows_batch(&self, updates: &[FollowUpdateBatch<'_>]) -> Result<usize> {
        self.update_lists_batch(updates, false)
    }

    /// Persist public kind-10000 mute lists independently of follow metadata.
    pub fn update_mutes_batch(&self, updates: &[FollowUpdateBatch<'_>]) -> Result<usize> {
        self.update_lists_batch(updates, true)
    }

    fn update_lists_batch(&self, updates: &[FollowUpdateBatch<'_>], mutes: bool) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        let mut winners: HashMap<&str, &FollowUpdateBatch<'_>> = HashMap::new();
        for update in updates {
            match winners.get(update.pubkey) {
                Some(previous)
                    if !event_wins(
                        update.created_at,
                        update.event_id,
                        previous.created_at,
                        previous.event_id,
                    ) => {}
                _ => {
                    winners.insert(update.pubkey, update);
                }
            }
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp();
        let mut accepted = 0;
        {
            let mut get_node = tx.prepare_cached(if mutes {
                "SELECT n.id, m.event_id, m.created_at FROM nodes n LEFT JOIN mute_lists m ON n.id = m.follower_id WHERE n.pubkey = ?1"
            } else {
                "SELECT id, kind3_event_id, kind3_created_at FROM nodes WHERE pubkey = ?1"
            })?;
            let mut upsert_node = tx.prepare_cached(if mutes {
                "INSERT INTO mute_lists (follower_id, event_id, created_at, updated_at)
                 VALUES ((SELECT id FROM nodes WHERE pubkey = ?1), ?2, ?3, ?4)
                 ON CONFLICT(follower_id) DO UPDATE SET event_id = ?2, created_at = ?3, updated_at = ?4"
            } else {
                "INSERT INTO nodes (pubkey, kind3_event_id, kind3_created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT(pubkey) DO UPDATE SET
                 kind3_event_id = ?2, kind3_created_at = ?3, updated_at = ?4"
            })?;
            // These identifiers are internal constants, never user-provided SQL.
            let edge_table = if mutes { "mute_edges" } else { "edges" };
            let mut old_edges = tx.prepare_cached(&format!(
                "SELECT n.pubkey, e.followed_id FROM {edge_table} e JOIN nodes n ON n.id = e.followed_id WHERE e.follower_id = ?1"
            ))?;
            let mut delete_edge = tx.prepare_cached(&format!(
                "DELETE FROM {edge_table} WHERE follower_id = ?1 AND followed_id = ?2"
            ))?;
            let mut insert_node = tx.prepare_cached(
                "INSERT INTO nodes (pubkey, updated_at) VALUES (?1, ?2) ON CONFLICT(pubkey) DO NOTHING"
            )?;
            let mut insert_edge = tx.prepare_cached(&format!(
                "INSERT INTO {edge_table} (follower_id, followed_id) VALUES (?1, ?2)"
            ))?;
            for update in winners.values() {
                let existing: Option<(i64, Option<String>, Option<i64>)> = get_node
                    .query_row(params![update.pubkey], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()?;
                if let Some((_, ref id, ts)) = existing {
                    if !event_wins(update.created_at, update.event_id, ts, id.as_deref()) {
                        continue;
                    }
                }
                if mutes {
                    insert_node.execute(params![update.pubkey, now])?;
                }
                upsert_node.execute(params![
                    update.pubkey,
                    update.event_id,
                    update.created_at,
                    now
                ])?;
                let follower_id: i64 =
                    get_node.query_row(params![update.pubkey], |row| row.get(0))?;
                let previous: HashMap<String, i64> = old_edges
                    .query_map(params![follower_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()?;
                let desired: HashSet<&str> = update.follows.iter().map(String::as_str).collect();
                for (pubkey, id) in &previous {
                    if !desired.contains(pubkey.as_str()) {
                        delete_edge.execute(params![follower_id, id])?;
                    }
                }
                for pubkey in desired {
                    if !previous.contains_key(pubkey) {
                        insert_node.execute(params![pubkey, now])?;
                        let followed_id: i64 =
                            get_node.query_row(params![pubkey], |row| row.get(0))?;
                        insert_edge.execute(params![follower_id, followed_id])?;
                    }
                }
                accepted += 1;
            }
        }
        tx.commit()?;
        debug!(
            "Batch persisted {} list updates (mutes={})",
            accepted, mutes
        );
        Ok(accepted)
    }

    #[allow(dead_code)] // Public API for sync state inspection
    pub fn get_sync_state(&self, relay_url: &str) -> Result<Option<SyncState>> {
        let conn = self.conn.lock();

        let result = conn.query_row(
            "SELECT relay_url, last_event_time, last_sync_at FROM sync_state WHERE relay_url = ?1",
            params![relay_url],
            |row| {
                Ok(SyncState {
                    relay_url: row.get(0)?,
                    last_event_time: row.get(1)?,
                    last_sync_at: row.get(2)?,
                })
            },
        );

        match result {
            Ok(state) => Ok(Some(state)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(dead_code)] // Public API for sync state management
    pub fn set_sync_state(&self, relay_url: &str, last_event_time: Option<i64>) -> Result<()> {
        let conn = self.conn.lock();
        let now = chrono::Utc::now().timestamp();

        conn.execute(
            r#"
            INSERT INTO sync_state (relay_url, last_event_time, last_sync_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(relay_url) DO UPDATE SET
                last_event_time = ?2,
                last_sync_at = ?3
            "#,
            params![relay_url, last_event_time, now],
        )?;

        Ok(())
    }

    #[allow(dead_code)] // Public API for database statistics
    pub fn get_stats(&self) -> Result<(usize, usize)> {
        let conn = self.conn.lock();

        let node_count: usize =
            conn.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;

        let edge_count: usize =
            conn.query_row("SELECT COUNT(*) FROM edges", [], |row| row.get(0))?;

        Ok((node_count, edge_count))
    }
}

/// NIP-01 replaceable ordering: newest timestamp, then lowest event ID.
fn event_wins(
    new_ts: Option<i64>,
    new_id: Option<&str>,
    old_ts: Option<i64>,
    old_id: Option<&str>,
) -> bool {
    match (new_ts, old_ts) {
        (Some(new), Some(old)) if new != old => new > old,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => true, // Preserve direct, unversioned graph manipulation.
        _ => match (new_id, old_id) {
            (Some(new), Some(old)) => new < old,
            (Some(_), None) => true,
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_database_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        let (nodes, edges) = db.get_stats().unwrap();
        assert_eq!(nodes, 0);
        assert_eq!(edges, 0);
    }

    #[test]
    fn test_upsert_node() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        let id1 = db
            .upsert_node("pubkey1", Some("event1"), Some(1000))
            .unwrap();
        let id2 = db
            .upsert_node("pubkey1", Some("event2"), Some(2000))
            .unwrap();

        assert_eq!(id1, id2); // Same pubkey should return same ID
    }

    #[test]
    fn test_update_follows() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        db.update_follows(
            "alice",
            &["bob".to_string(), "carol".to_string()],
            Some("event1"),
            Some(1000),
        )
        .unwrap();

        let (nodes, edges) = db.get_stats().unwrap();
        assert_eq!(nodes, 3); // alice, bob, carol
        assert_eq!(edges, 2); // alice->bob, alice->carol
    }

    #[test]
    fn test_load_graph() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        db.update_follows("alice", &["bob".to_string()], None, None)
            .unwrap();
        db.update_follows("bob", &["carol".to_string()], None, None)
            .unwrap();

        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();

        let stats = graph.stats();
        assert_eq!(stats.node_count, 3);
        assert_eq!(stats.edge_count, 2);
    }

    #[test]
    fn test_sync_state() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        let state = db.get_sync_state("wss://relay.test").unwrap();
        assert!(state.is_none());

        db.set_sync_state("wss://relay.test", Some(1000)).unwrap();

        let state = db.get_sync_state("wss://relay.test").unwrap().unwrap();
        assert_eq!(state.relay_url, "wss://relay.test");
        assert_eq!(state.last_event_time, Some(1000));
    }

    #[test]
    fn test_update_follows_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        let follows_alice = vec!["bob".to_string(), "carol".to_string()];
        let follows_dave = vec!["eve".to_string()];

        let updates = vec![
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &follows_alice,
                event_id: Some("event1"),
                created_at: Some(1000),
            },
            FollowUpdateBatch {
                pubkey: "dave",
                follows: &follows_dave,
                event_id: Some("event2"),
                created_at: Some(2000),
            },
        ];

        let count = db.update_follows_batch(&updates).unwrap();
        assert_eq!(count, 2);

        let (nodes, edges) = db.get_stats().unwrap();
        assert_eq!(nodes, 5); // alice, bob, carol, dave, eve
        assert_eq!(edges, 3); // alice->bob, alice->carol, dave->eve
    }
    #[test]
    fn empty_follow_event_survives_restart_and_rejects_older_events() {
        let file = NamedTempFile::new().unwrap();
        {
            let db = Database::open(file.path()).unwrap();
            db.update_follows("alice", &["bob".into()], Some("old"), Some(10))
                .unwrap();
            db.update_follows("alice", &[], Some("new"), Some(20))
                .unwrap();
        }
        let db = Database::open(file.path()).unwrap();
        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();
        assert_eq!(
            graph.get_node_info("alice").unwrap().kind3_created_at,
            Some(20)
        );
        assert!(!graph.update_follows("alice", &["bob".into()], Some("old".into()), Some(10)));
        assert_eq!(graph.get_follows("alice").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn persistence_rejects_stale_duplicate_and_losing_tie_events() {
        let db = Database::open(":memory:").unwrap();
        db.update_follows("alice", &["bob".into()], Some("b"), Some(20))
            .unwrap();
        for (id, timestamp) in [
            (Some("a"), Some(19)),
            (Some("c"), Some(20)),
            (Some("b"), Some(20)),
            (None, None),
        ] {
            db.update_follows("alice", &[], id, timestamp).unwrap();
            assert_eq!(db.get_stats().unwrap().1, 1);
        }
        db.update_follows("alice", &[], Some("a"), Some(20))
            .unwrap();
        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();
        assert_eq!(
            graph
                .get_node_info("alice")
                .unwrap()
                .kind3_event_id
                .as_deref(),
            Some("a")
        );
        assert_eq!(db.get_stats().unwrap().1, 0);
    }

    #[test]
    fn batch_coalesces_authors_before_creating_edges_or_nodes() {
        let db = Database::open(":memory:").unwrap();
        let loser = vec!["loser_only".into()];
        let winner = vec!["winner_only".into()];
        let updates = [
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &loser,
                event_id: Some("b"),
                created_at: Some(20),
            },
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &winner,
                event_id: Some("a"),
                created_at: Some(20),
            },
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &loser,
                event_id: Some("0"),
                created_at: Some(19),
            },
        ];
        assert_eq!(db.update_follows_batch(&updates).unwrap(), 1);
        assert_eq!(db.get_stats().unwrap(), (2, 1));
        assert_eq!(db.update_follows_batch(&updates).unwrap(), 0);
        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();
        assert_eq!(graph.get_follows("alice").unwrap(), winner);
    }

    #[test]
    fn metadata_updates_do_not_rewrite_edges_and_deltas_touch_only_changes() {
        let db = Database::open(":memory:").unwrap();
        db.update_follows("alice", &["bob".into(), "carol".into()], Some("a"), Some(1))
            .unwrap();
        db.conn.lock().execute_batch(
            "CREATE TABLE edge_changes (operation TEXT);
             CREATE TRIGGER record_insert AFTER INSERT ON edges BEGIN INSERT INTO edge_changes VALUES ('insert'); END;
             CREATE TRIGGER record_delete AFTER DELETE ON edges BEGIN INSERT INTO edge_changes VALUES ('delete'); END;"
        ).unwrap();
        db.update_follows(
            "alice",
            &["carol".into(), "bob".into(), "bob".into()],
            Some("b"),
            Some(2),
        )
        .unwrap();
        let count = || {
            db.conn
                .lock()
                .query_row("SELECT COUNT(*) FROM edge_changes", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap()
        };
        assert_eq!(count(), 0);
        db.update_follows("alice", &["bob".into(), "dave".into()], Some("c"), Some(3))
            .unwrap();
        assert_eq!(count(), 2);
    }

    #[test]
    fn failed_edge_write_rolls_back_metadata_and_deletions() {
        let db = Database::open(":memory:").unwrap();
        db.update_follows("alice", &["bob".into()], Some("a"), Some(1))
            .unwrap();
        db.conn.lock().execute_batch(
            "CREATE TRIGGER fail_insert BEFORE INSERT ON edges BEGIN SELECT RAISE(ABORT, 'test failure'); END;"
        ).unwrap();
        assert!(db
            .update_follows("alice", &["carol".into()], Some("b"), Some(2))
            .is_err());
        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();
        assert_eq!(graph.get_follows("alice").unwrap(), vec!["bob"]);
        assert_eq!(
            graph.get_node_info("alice").unwrap().kind3_created_at,
            Some(1)
        );
        assert_eq!(db.get_stats().unwrap(), (2, 1));
    }

    #[test]
    fn graph_load_reports_corrupt_edges_before_mutating_graph() {
        let db = Database::open(":memory:").unwrap();
        db.upsert_node("alice", None, None).unwrap();
        db.conn
            .lock()
            .execute("INSERT INTO edges VALUES (1, 999)", [])
            .unwrap();
        let graph = WotGraph::new();
        assert!(db.load_graph(&graph).is_err());
        assert_eq!(graph.stats().node_count, 0);
    }
    #[test]
    fn empty_mutes_survive_restart_independently_of_follows() {
        let file = NamedTempFile::new().unwrap();
        {
            let db = Database::open(file.path()).unwrap();
            db.update_follows("alice", &["bob".into()], Some("follow"), Some(100))
                .unwrap();
            let muted = vec!["carol".into()];
            db.update_mutes_batch(&[FollowUpdateBatch {
                pubkey: "alice",
                follows: &muted,
                event_id: Some("mute"),
                created_at: Some(10),
            }])
            .unwrap();
            db.update_mutes_batch(&[FollowUpdateBatch {
                pubkey: "alice",
                follows: &[],
                event_id: Some("unmute"),
                created_at: Some(20),
            }])
            .unwrap();
        }
        let db = Database::open(file.path()).unwrap();
        let graph = WotGraph::new();
        db.load_graph(&graph).unwrap();
        assert_eq!(graph.get_follows("alice").unwrap(), vec!["bob"]);
        assert_eq!(graph.get_mutes_page("alice", 0, 10), Some((vec![], 0)));
        assert_eq!(graph.get_mutes_page("bob", 0, 10), None);
        assert!(!graph.update_mutes("alice", &["carol".into()], Some("mute".into()), Some(10)));
        assert!(graph.update_mutes(
            "alice",
            &["carol".into()],
            Some("new-mute".into()),
            Some(30)
        ));
        assert_eq!(
            graph.get_node_info("alice").unwrap().kind3_created_at,
            Some(100)
        );
    }

    #[test]
    fn mute_batch_coalesces_and_rejects_stale_events() {
        let db = Database::open(":memory:").unwrap();
        let muted = vec!["bob".into()];
        let updates = [
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &muted,
                event_id: Some("b"),
                created_at: Some(20),
            },
            FollowUpdateBatch {
                pubkey: "alice",
                follows: &[],
                event_id: Some("a"),
                created_at: Some(20),
            },
        ];
        assert_eq!(db.update_mutes_batch(&updates).unwrap(), 1);
        assert_eq!(db.update_mutes_batch(&updates).unwrap(), 0);
        assert_eq!(db.get_stats().unwrap(), (1, 0));
        let conn = db.conn.lock();
        let metadata: (String, i64) = conn
            .query_row("SELECT event_id, created_at FROM mute_lists", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(metadata, ("a".into(), 20));
        drop(conn);
        db.update_follows("alice", &["bob".into()], Some("follow"), Some(1))
            .unwrap();
        assert_eq!(db.get_stats().unwrap(), (2, 1));
    }
}
