use anyhow::Result;
use lru::LruCache;
use nostr_sdk::prelude::*;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

use crate::db::{Database, FollowUpdateBatch};
use crate::graph::WotGraph;

const SEEN_CACHE_CAPACITY: usize = 100_000;

/// Tracks the latest seen event for a pubkey (for deduplication)
#[derive(Debug, Clone)]
struct SeenEvent {
    created_at: u64,
    #[allow(dead_code)]
    event_id: EventId,
}

pub struct Ingestion {
    graph: Arc<WotGraph>,
    db: Arc<Database>,
    relays: Vec<String>,
}

#[derive(Debug)]
struct FollowUpdate {
    pubkey: String,
    follows: Vec<String>,
    event_id: String,
    created_at: i64,
}

impl Ingestion {
    pub fn new(graph: Arc<WotGraph>, db: Arc<Database>, relays: Vec<String>) -> Self {
        Self { graph, db, relays }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting ingestion from {} relays", self.relays.len());

        // Channel for database persistence
        let (persist_tx, persist_rx) = mpsc::channel::<FollowUpdate>(10000);

        // Start persistence worker
        let db = self.db.clone();
        tokio::spawn(async move {
            persistence_worker(db, persist_rx).await;
        });

        // Create nostr client
        let client = Client::default();

        // Add relays
        for relay_url in &self.relays {
            match client.add_relay(relay_url).await {
                Ok(_) => info!("Added relay: {}", relay_url),
                Err(e) => warn!("Failed to add relay {}: {}", relay_url, e),
            }
        }

        // Connect to relays
        client.connect().await;

        // Subscribe to kind:3 (contact list) events
        let filter = Filter::new().kind(Kind::ContactList);

        info!("Subscribing to kind:3 events...");

        let graph = self.graph.clone();
        let persist_tx = persist_tx.clone();

        // LRU cache for deduplication: pubkey bytes → latest seen event
        // Evicts oldest entries when full, never clears entirely
        let seen_events: Arc<tokio::sync::RwLock<LruCache<[u8; 32], SeenEvent>>> =
            Arc::new(tokio::sync::RwLock::new(LruCache::new(
                NonZeroUsize::new(SEEN_CACHE_CAPACITY).unwrap()
            )));

        // Handle events
        client
            .subscribe(vec![filter], None)
            .await?;

        // Process events
        let mut notifications = client.notifications();
        let mut event_count: u64 = 0;
        let mut dedup_skip_count: u64 = 0;
        let mut last_log_time = std::time::Instant::now();

        loop {
            tokio::select! {
                Ok(notification) = notifications.recv() => {
                    if let RelayPoolNotification::Event { event, .. } = notification {
                        let pubkey_bytes = event.pubkey.to_bytes();
                        let event_created_at = event.created_at.as_u64();

                        // Early dedup check BEFORE parsing tags
                        // Skip if we've already seen a newer or equal event for this pubkey
                        let dominated = {
                            let seen = seen_events.read().await;
                            if let Some(existing) = seen.peek(&pubkey_bytes) {
                                event_created_at <= existing.created_at
                            } else {
                                false
                            }
                        };
                        if dominated {
                            dedup_skip_count += 1;
                            continue;
                        }

                        // Process the event (parse tags, extract follows)
                        if let Some(update) = process_event(&event) {
                            // Update in-memory graph (has its own timestamp check)
                            let updated = graph.update_follows(
                                &update.pubkey,
                                &update.follows,
                                Some(update.event_id.clone()),
                                Some(update.created_at),
                            );

                            if updated {
                                event_count += 1;

                                // Update seen cache AFTER successful graph update
                                {
                                    let mut seen = seen_events.write().await;
                                    seen.put(pubkey_bytes, SeenEvent {
                                        created_at: event_created_at,
                                        event_id: event.id,
                                    });
                                }

                                // Send to persistence worker
                                if let Err(e) = persist_tx.try_send(update) {
                                    warn!("Persistence queue full: {}", e);
                                }
                            }
                        }

                        // Log progress periodically
                        if last_log_time.elapsed() > Duration::from_secs(10) {
                            let stats = graph.stats();
                            let seen_size = seen_events.read().await.len();
                            info!(
                                "Sync progress: {} events, {} dedup skips, {} nodes, {} edges, seen_cache={}",
                                event_count, dedup_skip_count, stats.node_count, stats.edge_count, seen_size
                            );
                            last_log_time = std::time::Instant::now();
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // Periodic status log
                    let stats = graph.stats();
                    let seen_size = seen_events.read().await.len();
                    info!(
                        "Sync status: {} events, {} dedup skips, {} nodes, {} edges, seen_cache={}",
                        event_count, dedup_skip_count, stats.node_count, stats.edge_count, seen_size
                    );
                }
            }
        }
    }
}

fn process_event(event: &Event) -> Option<FollowUpdate> {
    if event.kind != Kind::ContactList {
        return None;
    }

    let pubkey = event.pubkey.to_hex();
    let event_id = event.id.to_hex();
    let created_at = i64::try_from(event.created_at.as_u64()).unwrap_or(i64::MAX);

    // Parse p-tags to get follow list
    let follows: Vec<String> = event
        .tags
        .iter()
        .filter_map(|tag| {
            let tag_vec = tag.as_slice();
            if tag_vec.len() >= 2 && tag_vec[0] == "p" {
                // Validate pubkey format (64 hex chars)
                let pk = &tag_vec[1];
                if pk.len() == 64 && pk.bytes().all(|b| b.is_ascii_hexdigit()) {
                    Some(pk.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    debug!(
        "Processed event from {} with {} follows",
        &pubkey[..8],
        follows.len()
    );

    Some(FollowUpdate {
        pubkey,
        follows,
        event_id,
        created_at,
    })
}

async fn persistence_worker(db: Arc<Database>, mut rx: mpsc::Receiver<FollowUpdate>) {
    info!("Persistence worker started");

    let mut batch: Vec<FollowUpdate> = Vec::with_capacity(100);
    let mut last_flush = std::time::Instant::now();

    loop {
        tokio::select! {
            Some(update) = rx.recv() => {
                batch.push(update);

                // Flush batch when full or after timeout
                if batch.len() >= 100 || last_flush.elapsed() > Duration::from_secs(5) {
                    flush_batch(&db, &mut batch).await;
                    last_flush = std::time::Instant::now();
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if !batch.is_empty() {
                    flush_batch(&db, &mut batch).await;
                    last_flush = std::time::Instant::now();
                }
            }
        }
    }
}

async fn flush_batch(db: &Arc<Database>, batch: &mut Vec<FollowUpdate>) {
    if batch.is_empty() {
        return;
    }

    debug!("Flushing {} updates to database", batch.len());

    // Take ownership of batch items for the blocking task; drain() empties the batch
    let owned_batch: Vec<FollowUpdate> = batch.drain(..).collect();
    let db = db.clone();

    tokio::task::spawn_blocking(move || {
        let updates: Vec<FollowUpdateBatch<'_>> = owned_batch
            .iter()
            .map(|u| FollowUpdateBatch {
                pubkey: &u.pubkey,
                follows: &u.follows,
                event_id: Some(u.event_id.as_str()),
                created_at: Some(u.created_at),
            })
            .collect();

        match db.update_follows_batch(&updates) {
            Ok(count) => debug!("Persisted {} updates in single transaction", count),
            Err(e) => error!("Failed to persist follow batch: {}", e),
        }
    }).await.ok();
}
