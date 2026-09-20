use anyhow::{anyhow, Context, Result};
use lru::LruCache;
use nostr_sdk::prelude::*;
use serde::Serialize;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use crate::db::{Database, FollowUpdateBatch};
use crate::graph::WotGraph;

const SEEN_CACHE_CAPACITY: usize = 100_000;
const BATCH_SIZE: usize = 100;
const MUTE_KIND: u16 = 10000;

#[derive(Default)]
pub struct SyncStatus {
    running: AtomicBool,
    failed: AtomicBool,
    last_event_received_at: AtomicI64,
    last_persisted_at: AtomicI64,
    persisted_events: AtomicU64,
    lagged_notifications: AtomicU64,
    persistence_errors: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct SyncSnapshot {
    pub running: bool,
    pub ready: bool,
    pub last_event_received_at: i64,
    pub last_persisted_at: i64,
    pub persisted_events: u64,
    pub lagged_notifications: u64,
    pub persistence_errors: u64,
    pub coverage: &'static str,
}

impl SyncStatus {
    pub fn ready(&self) -> bool {
        let last = self.last_event_received_at.load(Ordering::Relaxed);
        self.running.load(Ordering::Relaxed)
            && !self.failed.load(Ordering::Relaxed)
            && last > 0
            && chrono::Utc::now().timestamp().saturating_sub(last) <= 300
    }

    pub fn snapshot(&self) -> SyncSnapshot {
        SyncSnapshot {
            running: self.running.load(Ordering::Relaxed),
            ready: self.ready(),
            last_event_received_at: self.last_event_received_at.load(Ordering::Relaxed),
            last_persisted_at: self.last_persisted_at.load(Ordering::Relaxed),
            persisted_events: self.persisted_events.load(Ordering::Relaxed),
            lagged_notifications: self.lagged_notifications.load(Ordering::Relaxed),
            persistence_errors: self.persistence_errors.load(Ordering::Relaxed),
            coverage: "configured_relays_only",
        }
    }
}

#[derive(Debug, Clone)]
struct SeenEvent {
    created_at: u64,
    event_id: EventId,
}

impl SeenEvent {
    fn dominates(&self, event: &Event) -> bool {
        self.created_at > event.created_at.as_u64()
            || (self.created_at == event.created_at.as_u64() && self.event_id <= event.id)
    }
}

pub struct Ingestion {
    graph: Arc<WotGraph>,
    db: Arc<Database>,
    relays: Vec<String>,
    pub status: Arc<SyncStatus>,
}

#[derive(Debug, Clone)]
struct ListUpdate {
    kind: u16,
    pubkey: String,
    follows: Vec<String>,
    event_id: String,
    created_at: i64,
}

impl Ingestion {
    pub fn new(graph: Arc<WotGraph>, db: Arc<Database>, relays: Vec<String>) -> Self {
        Self {
            graph,
            db,
            relays,
            status: Arc::new(SyncStatus::default()),
        }
    }

    pub async fn start(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let result = self.run(&mut shutdown).await;
        self.status.running.store(false, Ordering::Relaxed);
        if result.is_err() {
            self.status.failed.store(true, Ordering::Relaxed);
        }
        result
    }

    async fn run(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        if self.relays.is_empty() {
            return Err(anyhow!("at least one relay is required"));
        }
        let (mut client, mut notifications) = connect_list_client(&self.relays).await?;
        self.status.running.store(true, Ordering::Relaxed);
        info!(
            relays = self.relays.len(),
            "Ingesting public follow and mute lists"
        );

        let mut seen = LruCache::new(NonZeroUsize::new(SEEN_CACHE_CAPACITY).unwrap());
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut flush_timer = tokio::time::interval(Duration::from_secs(1));
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = flush_timer.tick() => {
                    self.flush(&mut batch).await?;
                }
                notification = notifications.recv() => {
                    match notification {
                        Ok(RelayPoolNotification::Event { event, .. }) => {
                            let kind = event.kind.as_u16();
                            if kind != 3 && kind != MUTE_KIND { continue; }
                            let now = chrono::Utc::now().timestamp();
                            // Avoid an author's far-future event suppressing their subsequent updates.
                            if event.created_at.as_u64() > now as u64 + 600 { continue; }
                            self.status.last_event_received_at.store(now, Ordering::Relaxed);
                            let key = (event.pubkey.to_bytes(), kind);
                            if seen.get(&key).is_some_and(|entry: &SeenEvent| entry.dominates(&event)) {
                                continue;
                            }
                            if let Some(update) = process_event(&event) {
                                seen.put(key, SeenEvent { created_at: event.created_at.as_u64(), event_id: event.id });
                                batch.push(update);
                                if batch.len() >= BATCH_SIZE {
                                    self.flush(&mut batch).await?;
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(count)) => {
                            self.status.lagged_notifications.fetch_add(count, Ordering::Relaxed);
                            // The SDK marks IDs seen before broadcasting notifications. Reusing its
                            // client would suppress replay of events dropped from this receiver.
                            self.flush(&mut batch).await?;
                            seen.clear();
                            warn!(count, "Relay notifications lagged; recreating client for catch-up");
                            (client, notifications) = restart_list_client(&client, &self.relays).await?;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            self.flush(&mut batch).await?;
                            return Err(anyhow!("relay notification channel closed"));
                        }
                    }
                }
            }
        }
        // Container SIGTERM is handled by main, which waits for this durable drain.
        self.flush(&mut batch).await?;
        client.disconnect().await?;
        Ok(())
    }

    async fn flush(&self, batch: &mut Vec<ListUpdate>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let updates = Arc::new(std::mem::take(batch));
        for attempt in 0..3 {
            let db = self.db.clone();
            let graph = self.graph.clone();
            let pending = updates.clone();
            let result =
                tokio::task::spawn_blocking(move || persist_and_apply(&db, &graph, &pending))
                    .await
                    .context("persistence task failed")?;
            match result {
                Ok(count) => {
                    self.status
                        .persisted_events
                        .fetch_add(count as u64, Ordering::Relaxed);
                    self.status
                        .last_persisted_at
                        .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
                    self.status.failed.store(false, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) => {
                    self.status
                        .persistence_errors
                        .fetch_add(1, Ordering::Relaxed);
                    self.status.failed.store(true, Ordering::Relaxed);
                    if attempt == 2 {
                        return Err(error.context("persisting received list updates"));
                    }
                    warn!(attempt, %error, "Persistence failed; retaining batch for retry");
                    tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await;
                }
            }
        }
        unreachable!()
    }
}

async fn connect_list_client(
    relays: &[String],
) -> Result<(Client, broadcast::Receiver<RelayPoolNotification>)> {
    let client = Client::default();
    for relay in relays {
        client
            .add_relay(relay)
            .await
            .context("adding ingestion relay")?;
    }
    // Retain the first history events, and keep this receiver paired with this client's IDs.
    let notifications = client.notifications();
    client.connect().await;
    client.subscribe(vec![list_filter()], None).await?;
    Ok((client, notifications))
}

async fn restart_list_client(
    previous: &Client,
    relays: &[String],
) -> Result<(Client, broadcast::Receiver<RelayPoolNotification>)> {
    previous.disconnect().await?;
    // A fresh default client owns a fresh in-memory SDK dedup database. The durable
    // application database and graph are retained and reject stale replayed lists.
    connect_list_client(relays).await
}

fn list_filter() -> Filter {
    Filter::new().kinds([Kind::ContactList, Kind::Custom(MUTE_KIND)])
}

fn process_event(event: &Event) -> Option<ListUpdate> {
    let kind = event.kind.as_u16();
    if kind != 3 && kind != MUTE_KIND {
        return None;
    }
    let mut follows: Vec<String> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let values = tag.as_slice();
            if values.len() >= 2 && values[0] == "p" {
                let pk = &values[1];
                if pk.len() == 64 && pk.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Some(pk.to_ascii_lowercase());
                }
            }
            None
        })
        .collect();
    follows.sort_unstable();
    follows.dedup();
    Some(ListUpdate {
        kind,
        pubkey: event.pubkey.to_hex(),
        follows,
        event_id: event.id.to_hex(),
        created_at: i64::try_from(event.created_at.as_u64()).unwrap_or(i64::MAX),
    })
}

// Commit before publishing changes to readers. A database error leaves the graph untouched;
// restart can reconstruct any committed batch even if the process exits while applying it.
fn persist_and_apply(db: &Database, graph: &WotGraph, updates: &[ListUpdate]) -> Result<usize> {
    // Select once for persistence and publication so superseded events cannot
    // allocate nodes or repeatedly rebuild the same author's adjacency.
    let mut winners = std::collections::HashMap::new();
    for (index, update) in updates.iter().enumerate() {
        let slot = winners
            .entry((update.kind, update.pubkey.as_str()))
            .or_insert(index);
        let previous = &updates[*slot];
        if update.created_at > previous.created_at
            || (update.created_at == previous.created_at && update.event_id < previous.event_id)
        {
            *slot = index;
        }
    }
    let mut indices: Vec<usize> = winners.into_values().collect();
    indices.sort_unstable();
    let selected: Vec<&ListUpdate> = indices.into_iter().map(|index| &updates[index]).collect();
    let items = |kind| {
        selected
            .iter()
            .filter(|u| u.kind == kind)
            .map(|u| FollowUpdateBatch {
                pubkey: &u.pubkey,
                follows: &u.follows,
                event_id: Some(&u.event_id),
                created_at: Some(u.created_at),
            })
            .collect::<Vec<_>>()
    };
    let count = db.update_follows_batch(&items(3))? + db.update_mutes_batch(&items(MUTE_KIND))?;
    for update in selected {
        if update.kind == MUTE_KIND {
            graph.update_mutes(
                &update.pubkey,
                &update.follows,
                Some(update.event_id.clone()),
                Some(update.created_at),
            );
        } else {
            graph.update_follows(
                &update.pubkey,
                &update.follows,
                Some(update.event_id.clone()),
                Some(update.created_at),
            );
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn accept_relay_socket(
        listener: &tokio::net::TcpListener,
    ) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            // The SDK also requests a NIP-11 document over ordinary HTTP. Only
            // upgraded WebSocket connections belong to the event test protocol.
            if let Ok(ws) = tokio_tungstenite::accept_async(socket).await {
                return ws;
            }
        }
    }

    #[test]
    fn public_mute_lists_ignore_encrypted_content() {
        let keys = Keys::generate();
        let target = Keys::generate().public_key();
        let event = EventBuilder::new(
            Kind::Custom(MUTE_KIND),
            "encrypted-private-items",
            [Tag::public_key(target)],
        )
        .to_event(&keys)
        .unwrap();
        let update = process_event(&event).expect("public mute list");
        assert_eq!(update.kind, MUTE_KIND);
        assert_eq!(update.follows, vec![target.to_hex()]);
    }

    fn update(kind: u16, at: i64, follows: &[&str]) -> ListUpdate {
        ListUpdate {
            kind,
            pubkey: "a".into(),
            follows: follows.iter().map(|s| s.to_string()).collect(),
            event_id: format!("{at:064x}"),
            created_at: at,
        }
    }

    #[test]
    fn committed_follow_and_mute_updates_survive_restart() {
        let db = Database::open(":memory:").unwrap();
        let graph = WotGraph::new();
        persist_and_apply(
            &db,
            &graph,
            &[update(3, 20, &["b"]), update(MUTE_KIND, 10, &["b"])],
        )
        .unwrap();
        let restored = WotGraph::new();
        db.load_graph(&restored).unwrap();
        assert_eq!(restored.get_follows("a"), Some(vec!["b".into()]));
        assert!(restored.mute_evidence("a", "b").source_mutes_target);
        persist_and_apply(&db, &graph, &[update(MUTE_KIND, 30, &[])]).unwrap();
        assert!(!graph.mute_evidence("a", "b").source_mutes_target);
    }

    #[test]
    fn superseded_batch_targets_never_enter_the_live_graph() {
        let db = Database::open(":memory:").unwrap();
        let graph = WotGraph::new();
        let mut tie_winner = update(3, 20, &["follow-winner"]);
        tie_winner.event_id = "00".into();
        let updates = [
            update(3, 10, &["obsolete-follow"]),
            update(MUTE_KIND, 10, &["obsolete-mute"]),
            update(3, 20, &["losing-tie"]),
            tie_winner,
            update(MUTE_KIND, 20, &[]),
        ];
        assert_eq!(persist_and_apply(&db, &graph, &updates).unwrap(), 2);
        assert_eq!(graph.get_node_id("obsolete-follow"), None);
        assert_eq!(graph.get_node_id("obsolete-mute"), None);
        assert_eq!(graph.get_node_id("losing-tie"), None);
        assert_eq!(graph.get_follows("a"), Some(vec!["follow-winner".into()]));
        assert_eq!(graph.get_mutes_page("a", 0, 10), Some((vec![], 0)));
        let restored = WotGraph::new();
        db.load_graph(&restored).unwrap();
        assert_eq!(graph.stats().node_count, restored.stats().node_count);
    }

    #[test]
    fn database_failure_does_not_publish_graph_updates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wot.db");
        let db = Database::open(&path).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER fail_write BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;").unwrap();
        let graph = WotGraph::new();
        assert!(persist_and_apply(&db, &graph, &[update(3, 20, &["b"])]).is_err());
        assert_eq!(graph.get_node_id("a"), None);
        conn.execute_batch("DROP TRIGGER fail_write;").unwrap();
        persist_and_apply(&db, &graph, &[update(3, 20, &["b"])]).unwrap();
        assert_eq!(graph.get_follows("a"), Some(vec!["b".into()]));
    }

    #[tokio::test]
    async fn small_batch_is_flushed_without_waiting_for_capacity() {
        let db = Arc::new(Database::open(":memory:").unwrap());
        let graph = Arc::new(WotGraph::new());
        let ingestion = Ingestion::new(graph.clone(), db.clone(), vec![]);
        let mut batch = vec![update(3, 20, &["b"])];
        ingestion.flush(&mut batch).await.unwrap();
        assert!(batch.is_empty());
        assert_eq!(graph.get_follows("a"), Some(vec!["b".into()]));
        assert_eq!(ingestion.status.snapshot().persisted_events, 1);
    }

    #[test]
    fn readiness_requires_recent_ingestion() {
        let status = SyncStatus::default();
        assert!(!status.ready());
        status.running.store(true, Ordering::Relaxed);
        status
            .last_event_received_at
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
        assert!(status.ready());
        status.failed.store(true, Ordering::Relaxed);
        assert!(!status.ready());
    }

    #[tokio::test]
    async fn lag_recovery_replays_events_already_seen_by_sdk() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relays = vec![format!("ws://{}", listener.local_addr().unwrap())];
        let keys = Keys::generate();
        let target = Keys::generate().public_key();
        let event = EventBuilder::new(Kind::ContactList, "", [Tag::public_key(target)])
            .to_event(&keys)
            .unwrap();
        let expected_id = event.id;
        let author = keys.public_key().to_hex();
        let (send_history, history_requested) = tokio::sync::oneshot::channel();
        let relay = tokio::spawn(async move {
            let mut history_requested = Some(history_requested);
            let mut sockets = Vec::new();
            for connection in 0..2 {
                let mut ws = accept_relay_socket(&listener).await;
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Text(text) = message {
                        let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if request[0] != "REQ" {
                            continue;
                        }
                        if let Some(requested) = history_requested.take() {
                            requested.await.unwrap();
                        }
                        let response = serde_json::json!(["EVENT", request[1], event]).to_string();
                        // Only the first copy yields an SDK Event notification, but every
                        // copy yields a Message. Overflow the default 4096-slot receiver
                        // without generating thousands of expensive event signatures.
                        let copies = if connection == 0 { 5000 } else { 1 };
                        for _ in 0..copies {
                            ws.send(Message::Text(response.clone())).await.unwrap();
                        }
                        ws.send(Message::Text(
                            serde_json::json!(["EOSE", request[1]]).to_string(),
                        ))
                        .await
                        .unwrap();
                        break;
                    }
                }
                // Keep the first connection open until the recovery code disconnects it.
                sockets.push(ws);
            }
            std::future::pending::<()>().await;
            drop(sockets);
        });

        let (client, mut stalled) = connect_list_client(&relays).await.unwrap();
        let mut observer = client.notifications();
        send_history.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match observer.recv().await {
                    Ok(RelayPoolNotification::Message {
                        message: RelayMessage::EndOfStoredEvents(_),
                        ..
                    }) => break,
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(error) => panic!("relay closed before history completed: {error}"),
                }
            }
        })
        .await
        .expect("relay history completed");
        assert!(matches!(
            stalled.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        assert_eq!(
            client.database().check_id(&expected_id).await.unwrap(),
            DatabaseEventStatus::Saved
        );

        let (recovered, mut notifications) = restart_list_client(&client, &relays).await.unwrap();
        let replayed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let RelayPoolNotification::Event { event, .. } =
                    notifications.recv().await.unwrap()
                {
                    break event;
                }
            }
        })
        .await
        .expect("dropped event replayed despite old SDK dedup state");
        assert_eq!(replayed.id, expected_id);
        let db = Database::open(":memory:").unwrap();
        let graph = WotGraph::new();
        persist_and_apply(&db, &graph, &[process_event(&replayed).unwrap()]).unwrap();
        let restored = WotGraph::new();
        db.load_graph(&restored).unwrap();
        assert_eq!(restored.get_follows(&author), Some(vec![target.to_hex()]));
        recovered.disconnect().await.unwrap();
        relay.abort();
    }

    #[tokio::test]
    async fn local_relay_shutdown_drains_received_event() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let keys = Keys::generate();
        let target = Keys::generate().public_key();
        let event = EventBuilder::new(Kind::ContactList, "", [Tag::public_key(target)])
            .to_event(&keys)
            .unwrap();
        let author = keys.public_key().to_hex();
        let relay = tokio::spawn(async move {
            let mut ws = accept_relay_socket(&listener).await;
            while let Some(Ok(message)) = ws.next().await {
                if let Message::Text(text) = message {
                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if request[0] == "REQ" {
                        let response = serde_json::json!(["EVENT", request[1], event]);
                        ws.send(Message::Text(response.to_string())).await.unwrap();
                        ws.send(Message::Text(
                            serde_json::json!(["EOSE", request[1]]).to_string(),
                        ))
                        .await
                        .unwrap();
                    }
                }
            }
        });
        let db = Arc::new(Database::open(":memory:").unwrap());
        let graph = Arc::new(WotGraph::new());
        let ingestion = Ingestion::new(graph.clone(), db.clone(), vec![format!("ws://{address}")]);
        let status = ingestion.status.clone();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(async move { ingestion.start(stopped).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while status.last_event_received_at.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("local relay event received");
        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let restored = WotGraph::new();
        db.load_graph(&restored).unwrap();
        assert_eq!(restored.get_follows(&author), Some(vec![target.to_hex()]));
        assert!(!status.snapshot().running);
        relay.abort();
    }
}
