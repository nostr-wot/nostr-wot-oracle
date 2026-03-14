use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::path::Path;
use parking_lot::Mutex;
use tracing::{info, debug, warn};

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

        conn.execute_batch(r#"
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

            CREATE TABLE IF NOT EXISTS sync_state (
                relay_url TEXT PRIMARY KEY,
                last_event_time INTEGER,
                last_sync_at INTEGER
            );
        "#)?;

        info!("Database schema initialized");
        Ok(())
    }

    pub fn load_graph(&self, graph: &WotGraph) -> Result<()> {
        let conn = self.conn.lock();

        // Load all nodes
        let mut node_stmt = conn.prepare(
            "SELECT id, pubkey, kind3_event_id, kind3_created_at FROM nodes ORDER BY id"
        )?;

        let nodes: Vec<(i64, String, Option<String>, Option<i64>)> = node_stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                ))
            })?
            .filter_map(|r| match r {
                Ok(val) => Some(val),
                Err(e) => { warn!("Skipping malformed row: {}", e); None }
            })
            .collect();

        info!("Loading {} nodes from database", nodes.len());

        // Create nodes in graph (they will get sequential IDs)
        for (_, pubkey, _, _) in &nodes {
            graph.get_or_create_node(pubkey);
        }

        // Load edges grouped by follower
        let mut edge_stmt = conn.prepare(
            "SELECT e.follower_id, n.pubkey, GROUP_CONCAT(n2.pubkey) as follows
             FROM edges e
             JOIN nodes n ON e.follower_id = n.id
             JOIN nodes n2 ON e.followed_id = n2.id
             GROUP BY e.follower_id"
        )?;

        let mut edge_count = 0;
        let edges: Vec<(String, String)> = edge_stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|r| match r {
                Ok(val) => Some(val),
                Err(e) => { warn!("Skipping malformed row: {}", e); None }
            })
            .collect();

        // Build a HashMap for O(1) lookup instead of O(N) linear scan per edge
        let node_info_map: HashMap<&str, (&Option<String>, &Option<i64>)> = nodes.iter()
            .map(|(_, pk, eid, cat)| (pk.as_str(), (eid, cat)))
            .collect();

        for (follower_pubkey, follows_csv) in edges {
            let follows: Vec<String> = follows_csv.split(',').map(|s| s.to_string()).collect();
            edge_count += follows.len();

            // Look up the node's event info in O(1)
            let (event_id, created_at) = node_info_map
                .get(follower_pubkey.as_str())
                .map(|(eid, cat)| ((*eid).clone(), **cat))
                .unwrap_or((None, None));

            graph.update_follows(&follower_pubkey, &follows, event_id, created_at);
        }

        info!("Loaded {} edges from database", edge_count);
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
    pub fn update_follows(&self, follower_pubkey: &str, follows: &[String], event_id: Option<&str>, created_at: Option<i64>) -> Result<()> {
        if follows.is_empty() {
            // Just update the node, clear edges
            let mut conn = self.conn.lock();
            let tx = conn.transaction()?;
            let now = chrono::Utc::now().timestamp();

            tx.execute(
                r#"
                INSERT INTO nodes (pubkey, kind3_event_id, kind3_created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(pubkey) DO UPDATE SET
                    kind3_event_id = COALESCE(?2, kind3_event_id),
                    kind3_created_at = COALESCE(?3, kind3_created_at),
                    updated_at = ?4
                "#,
                params![follower_pubkey, event_id, created_at, now],
            )?;

            let follower_id: i64 = tx.query_row(
                "SELECT id FROM nodes WHERE pubkey = ?1",
                params![follower_pubkey],
                |row| row.get(0),
            )?;

            tx.execute("DELETE FROM edges WHERE follower_id = ?1", params![follower_id])?;
            tx.commit()?;
            return Ok(());
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp();

        // Upsert follower node
        tx.execute(
            r#"
            INSERT INTO nodes (pubkey, kind3_event_id, kind3_created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(pubkey) DO UPDATE SET
                kind3_event_id = COALESCE(?2, kind3_event_id),
                kind3_created_at = COALESCE(?3, kind3_created_at),
                updated_at = ?4
            "#,
            params![follower_pubkey, event_id, created_at, now],
        )?;

        let follower_id: i64 = tx.query_row(
            "SELECT id FROM nodes WHERE pubkey = ?1",
            params![follower_pubkey],
            |row| row.get(0),
        )?;

        // Delete existing edges (single statement)
        tx.execute("DELETE FROM edges WHERE follower_id = ?1", params![follower_id])?;

        // Batch insert followed nodes using prepared statement
        {
            let mut insert_node_stmt = tx.prepare_cached(
                "INSERT INTO nodes (pubkey, updated_at) VALUES (?1, ?2) ON CONFLICT(pubkey) DO NOTHING"
            )?;

            for follow_pubkey in follows {
                insert_node_stmt.execute(params![follow_pubkey, now])?;
            }
        }

        // Batch fetch all followed node IDs - chunk to avoid SQLite parameter limit (~999)
        const CHUNK_SIZE: usize = 500;
        let mut followed_ids: Vec<i64> = Vec::with_capacity(follows.len());

        for chunk in follows.chunks(CHUNK_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let in_clause = placeholders.join(",");
            let select_sql = format!("SELECT id FROM nodes WHERE pubkey IN ({})", in_clause);

            let mut select_stmt = tx.prepare(&select_sql)?;
            let params: Vec<&dyn rusqlite::ToSql> = chunk
                .iter()
                .map(|s| s as &dyn rusqlite::ToSql)
                .collect();

            let rows = select_stmt.query_map(params.as_slice(), |row| row.get::<_, i64>(0))?;
            followed_ids.extend(rows.filter_map(|r| r.ok()));
        }

        // Batch insert edges using prepared statement
        {
            let mut insert_edge_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO edges (follower_id, followed_id) VALUES (?1, ?2)"
            )?;

            for followed_id in &followed_ids {
                insert_edge_stmt.execute(params![follower_id, followed_id])?;
            }
        }

        tx.commit()?;
        debug!("Updated follows for {} with {} follows", follower_pubkey, follows.len());

        Ok(())
    }

    /// Batch update multiple follow lists in a single transaction.
    /// Much faster than calling update_follows() in a loop (1 commit vs N commits).
    pub fn update_follows_batch(&self, updates: &[FollowUpdateBatch<'_>]) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.lock();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().timestamp();

        // Scope for prepared statements - must be dropped before tx.commit()
        let success_count = {
            // Prepare statements once, reuse for all updates
            let mut upsert_node_stmt = tx.prepare_cached(
                r#"
                INSERT INTO nodes (pubkey, kind3_event_id, kind3_created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(pubkey) DO UPDATE SET
                    kind3_event_id = COALESCE(?2, kind3_event_id),
                    kind3_created_at = COALESCE(?3, kind3_created_at),
                    updated_at = ?4
                "#,
            )?;

            let mut get_id_stmt = tx.prepare_cached(
                "SELECT id FROM nodes WHERE pubkey = ?1"
            )?;

            let mut delete_edges_stmt = tx.prepare_cached(
                "DELETE FROM edges WHERE follower_id = ?1"
            )?;

            let mut insert_follow_node_stmt = tx.prepare_cached(
                "INSERT INTO nodes (pubkey, updated_at) VALUES (?1, ?2) ON CONFLICT(pubkey) DO NOTHING"
            )?;

            let mut insert_edge_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO edges (follower_id, followed_id) VALUES (?1, ?2)"
            )?;

            let mut success_count = 0;

            for update in updates {
                // Upsert follower node
                upsert_node_stmt.execute(params![
                    update.pubkey,
                    update.event_id,
                    update.created_at,
                    now
                ])?;

                let follower_id: i64 = get_id_stmt.query_row(
                    params![update.pubkey],
                    |row| row.get(0),
                )?;

                // Delete existing edges
                delete_edges_stmt.execute(params![follower_id])?;

                if update.follows.is_empty() {
                    success_count += 1;
                    continue;
                }

                // Insert followed nodes
                for follow_pubkey in update.follows {
                    insert_follow_node_stmt.execute(params![follow_pubkey, now])?;
                }

                // Batch fetch followed node IDs - chunk to avoid SQLite parameter limit
                const CHUNK_SIZE: usize = 500;
                let mut followed_ids: Vec<i64> = Vec::with_capacity(update.follows.len());

                for chunk in update.follows.chunks(CHUNK_SIZE) {
                    let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
                    let in_clause = placeholders.join(",");
                    let select_sql = format!("SELECT id FROM nodes WHERE pubkey IN ({})", in_clause);

                    let mut select_stmt = tx.prepare(&select_sql)?;
                    let params_vec: Vec<&dyn rusqlite::ToSql> = chunk
                        .iter()
                        .map(|s| s as &dyn rusqlite::ToSql)
                        .collect();

                    let rows = select_stmt.query_map(params_vec.as_slice(), |row| row.get::<_, i64>(0))?;
                    followed_ids.extend(rows.filter_map(|r| r.ok()));
                }

                // Insert edges
                for followed_id in &followed_ids {
                    insert_edge_stmt.execute(params![follower_id, followed_id])?;
                }

                success_count += 1;
            }

            success_count
        }; // Prepared statements dropped here

        tx.commit()?;
        debug!("Batch persisted {} follow updates", success_count);

        Ok(success_count)
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

        let node_count: usize = conn.query_row(
            "SELECT COUNT(*) FROM nodes",
            [],
            |row| row.get(0),
        )?;

        let edge_count: usize = conn.query_row(
            "SELECT COUNT(*) FROM edges",
            [],
            |row| row.get(0),
        )?;

        Ok((node_count, edge_count))
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

        let id1 = db.upsert_node("pubkey1", Some("event1"), Some(1000)).unwrap();
        let id2 = db.upsert_node("pubkey1", Some("event2"), Some(2000)).unwrap();

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
        ).unwrap();

        let (nodes, edges) = db.get_stats().unwrap();
        assert_eq!(nodes, 3); // alice, bob, carol
        assert_eq!(edges, 2); // alice->bob, alice->carol
    }

    #[test]
    fn test_load_graph() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::open(temp_file.path()).unwrap();

        db.update_follows("alice", &["bob".to_string()], None, None).unwrap();
        db.update_follows("bob", &["carol".to_string()], None, None).unwrap();

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
}
