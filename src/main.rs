mod api;
mod cache;
mod config;
mod db;
mod graph;
mod sync;

use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use api::{http::AppState, DvmService};
use cache::QueryCache;
use config::Config;
use db::Database;
use graph::WotGraph;
use sync::Ingestion;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("WoT Oracle v{} starting...", env!("CARGO_PKG_VERSION"));
    info!(
        "Tokio runtime: {} worker threads",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    );

    // Load configuration
    let config = Config::from_env();
    info!(
        "Configuration loaded: {} relays, HTTP port {}",
        config.relays.len(),
        config.http_port
    );

    // Initialize database
    let db = Arc::new(Database::open(&config.db_path)?);
    info!("Database opened at: {}", config.db_path);

    // Create graph and load from database
    let graph = Arc::new(WotGraph::new());
    let load_db = db.clone();
    let load_graph = graph.clone();
    tokio::task::spawn_blocking(move || load_db.load_graph(&load_graph)).await??;

    let initial_stats = graph.stats();
    info!(
        "Graph loaded: {} nodes, {} edges",
        initial_stats.node_count, initial_stats.edge_count
    );

    // Create shared config
    let config = Arc::new(config);

    // Create query cache
    let cache = Arc::new(QueryCache::new(config.cache_size, config.cache_ttl_secs));
    info!(
        "Query cache initialized: {} entries, {} second TTL",
        config.cache_size, config.cache_ttl_secs
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let query_slots = Arc::new(tokio::sync::Semaphore::new(4));
    let ingestion = Ingestion::new(graph.clone(), db.clone(), config.relays.clone());

    // Create app state for HTTP server
    let app_state = AppState {
        graph: graph.clone(),
        config: config.clone(),
        cache: cache.clone(),
        query_slots: query_slots.clone(),
        sync: ingestion.status.clone(),
    };

    // Every service is supervised; persistence errors exit nonzero for the container restart policy.
    let ingestion_shutdown = shutdown_rx.clone();
    let mut ingestion_handle =
        tokio::spawn(async move { ingestion.start(ingestion_shutdown).await });

    // Start DVM service if enabled
    let _dvm_handle = if config.dvm_enabled {
        if let Some(ref private_key) = config.dvm_private_key {
            match DvmService::new(
                graph.clone(),
                cache.clone(),
                config.clone(),
                private_key,
                query_slots.clone(),
            ) {
                Ok(dvm) => {
                    let handle = tokio::spawn(async move {
                        if let Err(e) = dvm.start().await {
                            error!("DVM error: {}", e);
                        }
                    });
                    Some(handle)
                }
                Err(e) => {
                    error!("Failed to create DVM service: {}", e);
                    None
                }
            }
        } else {
            error!("DVM enabled but DVM_PRIVATE_KEY not set");
            None
        }
    } else {
        info!("DVM service disabled");
        None
    };

    // Start HTTP server
    let http_port = config.http_port;
    let rate_limit = config.rate_limit_per_minute;
    let mut http_handle = tokio::spawn(async move {
        api::http::start_server(app_state, http_port, rate_limit, shutdown_rx).await
    });

    let mut ingestion_finished = false;
    let mut http_finished = false;
    let outcome = tokio::select! {
        signal = shutdown_signal() => signal,
        result = &mut http_handle => {
            http_finished = true;
            match result {
                Ok(Err(error)) => Err(error),
                Err(error) => Err(error.into()),
                Ok(Ok(())) => Err(anyhow!("HTTP server terminated unexpectedly")),
            }
        }
        result = &mut ingestion_handle => {
            ingestion_finished = true;
            match result {
                Ok(Err(error)) => Err(error),
                Err(error) => Err(error.into()),
                Ok(Ok(())) => Err(anyhow!("Ingestion terminated unexpectedly")),
            }
        }
    };
    info!("Shutting down; draining pending database writes");
    let _ = shutdown_tx.send(true);
    if !ingestion_finished {
        ingestion_handle.await??;
    }
    if !http_finished {
        http_handle.await??;
    }
    outcome
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
