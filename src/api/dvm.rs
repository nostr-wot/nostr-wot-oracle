use anyhow::{Context, Result};
use nostr_sdk::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::cache::{CacheKey, QueryCache};
use crate::config::{Config, MAX_HOPS_LIMIT};
use crate::graph::{bfs, WotGraph};

const DVM_REQUEST_KIND: u16 = 5950;
const DVM_RESPONSE_KIND: u16 = 6950;

pub struct DvmService {
    graph: Arc<WotGraph>,
    cache: Arc<QueryCache>,
    config: Arc<Config>,
    keys: Keys,
    query_slots: Arc<tokio::sync::Semaphore>,
}

impl DvmService {
    pub fn new(
        graph: Arc<WotGraph>,
        cache: Arc<QueryCache>,
        config: Arc<Config>,
        private_key: &str,
        query_slots: Arc<tokio::sync::Semaphore>,
    ) -> Result<Self> {
        let keys = Keys::parse(private_key).context("Failed to parse DVM private key")?;

        info!("DVM service pubkey: {}", keys.public_key().to_hex());

        Ok(Self {
            graph,
            cache,
            config,
            keys,
            query_slots,
        })
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting DVM service...");

        let client = Client::new(&self.keys);

        // Add relays
        for relay_url in &self.config.relays {
            match client.add_relay(relay_url).await {
                Ok(_) => info!("DVM added relay: {}", relay_url),
                Err(e) => warn!("DVM failed to add relay {}: {}", relay_url, e),
            }
        }

        client.connect().await;

        // Subscribe to DVM requests (kind 5950)
        let filter = Filter::new()
            .kind(Kind::Custom(DVM_REQUEST_KIND))
            .since(Timestamp::now());

        client.subscribe(vec![filter], None).await?;

        info!("DVM listening for requests (kind {})", DVM_REQUEST_KIND);

        let mut notifications = client.notifications();

        loop {
            match notifications.recv().await {
                Ok(RelayPoolNotification::Event { event, .. }) => {
                    if event.kind == Kind::Custom(DVM_REQUEST_KIND) {
                        match self.handle_request(&client, &event).await {
                            Ok(_) => debug!("Processed DVM request: {}", event.id),
                            Err(e) => error!("Failed to process DVM request: {}", e),
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("Error receiving notification: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn handle_request(&self, client: &Client, request: &Event) -> Result<()> {
        debug!("Received DVM request: {}", request.id);

        // Parse request parameters from tags (NIP-90 standard)
        let mut inputs: Vec<String> = Vec::new();
        let mut max_hops: u8 = self.config.max_hops;

        for tag in request.tags.iter() {
            let tag_slice = tag.as_slice();
            if tag_slice.len() >= 3 && tag_slice[0] == "i" && tag_slice[2] == "text" {
                let value = &tag_slice[1];
                // Check for colon-separated format (backwards compatibility)
                if value.contains(':') {
                    let parts: Vec<&str> = value.split(':').collect();
                    if parts.len() == 2 {
                        inputs.push(parts[0].to_string());
                        inputs.push(parts[1].to_string());
                    }
                } else {
                    // NIP-90 standard: single pubkey per "i" tag
                    inputs.push(value.to_string());
                }
            } else if tag_slice.len() >= 3 && tag_slice[0] == "param" {
                match tag_slice[1].as_str() {
                    "max_hops" => {
                        // Validate and clamp max_hops to safe range (1-MAX_HOPS_LIMIT)
                        max_hops = match tag_slice[2].parse::<u8>() {
                            Ok(h) if (1..=MAX_HOPS_LIMIT).contains(&h) => h,
                            Ok(h) => {
                                warn!(
                                    "DVM request max_hops {} out of range, clamping to {}",
                                    h, MAX_HOPS_LIMIT
                                );
                                h.clamp(1, MAX_HOPS_LIMIT)
                            }
                            Err(_) => {
                                warn!(
                                    "DVM request invalid max_hops value, using default {}",
                                    self.config.max_hops
                                );
                                self.config.max_hops
                            }
                        };
                    }
                    "from" => {
                        if inputs.is_empty() {
                            inputs.push(tag_slice[2].to_string());
                        } else {
                            inputs.insert(0, tag_slice[2].to_string());
                        }
                    }
                    "to" => {
                        inputs.push(tag_slice[2].to_string());
                    }
                    _ => {}
                }
            }
        }

        let (from, to) = match inputs.as_slice() {
            [f, t] => (f.clone(), t.clone()),
            _ => {
                self.send_error(
                    client,
                    request,
                    "Expected two 'i' tags with pubkeys or 'from'/'to' params",
                )
                .await?;
                return Ok(());
            }
        };

        // Validate pubkeys (less verbose error messages)
        if from.len() != 64 || !from.chars().all(|c| c.is_ascii_hexdigit()) {
            self.send_error(client, request, "Invalid pubkey format")
                .await?;
            return Ok(());
        }

        if to.len() != 64 || !to.chars().all(|c| c.is_ascii_hexdigit()) {
            self.send_error(client, request, "Invalid pubkey format")
                .await?;
            return Ok(());
        }

        // Check cache first
        let from_id = self.graph.get_node_id(&from);
        let to_id = self.graph.get_node_id(&to);
        let include_bridges = true;

        let result = if let (Some(from_id), Some(to_id)) = (from_id, to_id) {
            let cache_key = CacheKey::new(from_id, to_id, max_hops, include_bridges);
            if let Some(cached_result) = self.cache.get(&cache_key, &self.graph) {
                debug!("DVM cache hit for {} -> {}", &from[..8], &to[..8]);
                cached_result
            } else {
                // Compute on blocking thread pool and cache
                let query = bfs::DistanceQuery {
                    from: Arc::from(from.as_str()),
                    to: Arc::from(to.as_str()),
                    max_hops,
                    include_bridges,
                };
                let graph = Arc::clone(&self.graph);
                let permit = self.query_slots.clone().acquire_owned().await?;
                let (result, revision) = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let revision = graph.revision();
                    (bfs::compute_distance(&graph, &query), revision)
                })
                .await
                .context("BFS computation task failed")?;
                self.cache.insert(cache_key, &result, &self.graph, revision);
                debug!(
                    "DVM cache miss for {} -> {}, computed and cached",
                    &from[..8],
                    &to[..8]
                );
                result
            }
        } else {
            // Node not in graph, compute on blocking thread pool without caching
            let query = bfs::DistanceQuery {
                from: Arc::from(from.as_str()),
                to: Arc::from(to.as_str()),
                max_hops,
                include_bridges,
            };
            let graph = Arc::clone(&self.graph);
            let permit = self.query_slots.clone().acquire_owned().await?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                bfs::compute_distance(&graph, &query)
            })
            .await
            .context("BFS computation task failed")?
        };

        // Build response (don't echo full request for security)
        let response_content = serde_json::to_string(&result)?;

        let mut tags = vec![
            Tag::parse(&["e", &request.id.to_hex()])?,
            Tag::parse(&["p", &request.pubkey.to_hex()])?,
        ];

        // Add result tags
        if let Some(hops) = result.hops {
            tags.push(Tag::parse(&["result", &hops.to_string(), "hops"])?);
        }

        let response_event =
            EventBuilder::new(Kind::Custom(DVM_RESPONSE_KIND), response_content, tags);

        client.send_event_builder(response_event).await?;

        info!(
            "Sent DVM response for {} -> {}: {:?} hops",
            &from[..8],
            &to[..8],
            result.hops
        );

        Ok(())
    }

    async fn send_error(&self, client: &Client, request: &Event, error_msg: &str) -> Result<()> {
        let tags = vec![
            Tag::parse(&["e", &request.id.to_hex()])?,
            Tag::parse(&["p", &request.pubkey.to_hex()])?,
            Tag::parse(&["status", "error", error_msg])?,
        ];

        let error_event = EventBuilder::new(
            Kind::Custom(DVM_RESPONSE_KIND),
            serde_json::json!({"error": error_msg}).to_string(),
            tags,
        );

        client.send_event_builder(error_event).await?;

        warn!("Sent DVM error response: {}", error_msg);

        Ok(())
    }
}
