use axum::{
    extract::{Query, State},
    http::{header, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor};
use tracing::{debug, info};

use crate::cache::{CacheKey, CacheStats, QueryCache};
use crate::config::{Config, MAX_HOPS_DEFAULT, MAX_HOPS_LIMIT, REQUEST_BODY_LIMIT};
use crate::graph::{bfs, LockMetricsSnapshot, WotGraph};

#[derive(Clone)]
pub struct AppState {
    pub graph: Arc<WotGraph>,
    #[allow(dead_code)] // Reserved for future config-based features (e.g., dynamic max_hops)
    pub config: Arc<Config>,
    pub cache: Arc<QueryCache>,
}

#[derive(Debug, Deserialize)]
pub struct DistanceQueryParams {
    pub from: String,
    pub to: String,
    #[serde(default = "default_max_hops")]
    pub max_hops: u8,
    #[serde(default)]
    pub include_bridges: bool,
    #[serde(default)]
    pub bypass_cache: bool,
}

fn default_max_hops() -> u8 {
    MAX_HOPS_DEFAULT
}

#[derive(Debug, Deserialize)]
pub struct FollowsQueryParams {
    pub pubkey: String,
    #[serde(default = "default_follows_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_follows_limit() -> usize {
    500
}

#[derive(Debug, Deserialize)]
pub struct CommonFollowsQueryParams {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize)]
pub struct PathQueryParams {
    pub from: String,
    pub to: String,
    #[serde(default = "default_max_hops")]
    pub max_hops: u8,
}

#[derive(Debug, Serialize)]
pub struct FollowsResponse {
    pub pubkey: String,
    pub follows: Vec<String>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct CommonFollowsResponse {
    pub from: String,
    pub to: String,
    pub common_follows: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct PathResponse {
    pub from: String,
    pub to: String,
    pub path: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct BatchDistanceRequest {
    pub from: String,
    pub targets: Vec<String>,
    #[serde(default = "default_max_hops")]
    pub max_hops: u8,
    #[serde(default)]
    pub include_bridges: bool,
    #[serde(default)]
    pub bypass_cache: bool,
}

#[derive(Debug, Serialize)]
pub struct BatchDistanceResponse {
    pub from: String,
    pub results: Vec<bfs::DistanceResult>,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub node_count: usize,
    pub edge_count: usize,
    pub nodes_with_follows: usize,
    pub cache: CacheStats,
    pub locks: LockMetricsSnapshot,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

impl ErrorResponse {
    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            error: msg.into(),
            code: "INTERNAL_ERROR".to_string(),
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        let status = match self.code.as_str() {
            "INTERNAL_ERROR" => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(self)).into_response()
    }
}

fn validate_pubkey(pubkey: &str) -> Result<(), ErrorResponse> {
    // Less verbose error messages to avoid leaking validation details
    if pubkey.len() != 64 {
        return Err(ErrorResponse {
            error: "Invalid pubkey format".to_string(),
            code: "INVALID_PUBKEY".to_string(),
        });
    }

    if !pubkey.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ErrorResponse {
            error: "Invalid pubkey format".to_string(),
            code: "INVALID_PUBKEY".to_string(),
        });
    }

    Ok(())
}

fn validate_max_hops(max_hops: u8) -> Result<(), ErrorResponse> {
    if !(1..=MAX_HOPS_LIMIT).contains(&max_hops) {
        return Err(ErrorResponse {
            error: format!("max_hops must be between 1 and {}", MAX_HOPS_LIMIT),
            code: "INVALID_MAX_HOPS".to_string(),
        });
    }
    Ok(())
}

pub async fn get_distance(
    State(state): State<AppState>,
    Query(params): Query<DistanceQueryParams>,
) -> Result<Json<bfs::DistanceResult>, ErrorResponse> {
    validate_pubkey(&params.from)?;
    validate_pubkey(&params.to)?;
    validate_max_hops(params.max_hops)?;

    // Convert pubkeys to node IDs immediately for compact cache lookup
    let from_id = state.graph.get_node_id(&params.from);
    let to_id = state.graph.get_node_id(&params.to);

    // Check cache first (lock-free, stays on async thread)
    if !params.bypass_cache {
        if let (Some(from_id), Some(to_id)) = (from_id, to_id) {
            let cache_key = CacheKey::new(from_id, to_id, params.max_hops, params.include_bridges);
            if let Some(cached_result) = state.cache.get(&cache_key, &state.graph) {
                debug!("Cache hit for {} -> {}", &params.from[..8], &params.to[..8]);
                return Ok(Json(cached_result));
            }
        }
    }

    // CPU-bound BFS → blocking thread pool (keeps async workers free)
    let graph = state.graph.clone();
    let query = bfs::DistanceQuery {
        from: Arc::from(params.from.as_str()),
        to: Arc::from(params.to.as_str()),
        max_hops: params.max_hops,
        include_bridges: params.include_bridges,
    };

    let result = tokio::task::spawn_blocking(move || {
        bfs::compute_distance(&graph, &query)
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

    // Cache insert (lock-free, back on async thread)
    // Re-lookup only if the first lookup was None (node may have been added during BFS)
    let from_id = from_id.or_else(|| state.graph.get_node_id(&params.from));
    let to_id = to_id.or_else(|| state.graph.get_node_id(&params.to));
    if let (Some(from_id), Some(to_id)) = (from_id, to_id) {
        let cache_key = CacheKey::new(from_id, to_id, params.max_hops, params.include_bridges);
        state.cache.insert(cache_key, &result, &state.graph);
    }
    debug!("Cache miss for {} -> {}, computed and cached", &params.from[..8], &params.to[..8]);

    Ok(Json(result))
}

pub async fn batch_distance(
    State(state): State<AppState>,
    Json(request): Json<BatchDistanceRequest>,
) -> Result<Json<BatchDistanceResponse>, ErrorResponse> {
    validate_pubkey(&request.from)?;
    validate_max_hops(request.max_hops)?;

    if request.targets.len() > 100 {
        return Err(ErrorResponse {
            error: "Maximum 100 targets allowed per batch".to_string(),
            code: "TOO_MANY_TARGETS".to_string(),
        });
    }

    for target in &request.targets {
        validate_pubkey(target)?;
    }

    // Check cache for all targets first (lock-free, stays on async thread)
    let from_id = state.graph.get_node_id(&request.from);
    let bypass_cache = request.bypass_cache;

    let mut results: Vec<bfs::DistanceResult> = Vec::with_capacity(request.targets.len());
    // Use Arc<str> to avoid String clones in the blocking closure
    let mut uncached_targets: Vec<(usize, Arc<str>)> = Vec::new();

    for (idx, target) in request.targets.iter().enumerate() {
        let mut found_in_cache = false;

        if !bypass_cache {
            if let Some(from_id) = from_id {
                if let Some(to_id) = state.graph.get_node_id(target) {
                    let cache_key = CacheKey::new(from_id, to_id, request.max_hops, request.include_bridges);
                    if let Some(cached_result) = state.cache.get(&cache_key, &state.graph) {
                        results.push(cached_result);
                        found_in_cache = true;
                    }
                }
            }
        }

        if !found_in_cache {
            // Placeholder - will be filled by spawn_blocking
            results.push(bfs::DistanceResult::not_found(
                Arc::from(""),
                Arc::from(""),
            ));
            uncached_targets.push((idx, Arc::from(target.as_str())));
        }
    }

    // CPU-bound BFS for uncached targets → blocking thread pool
    if !uncached_targets.is_empty() {
        let graph = state.graph.clone();
        // Convert to Arc<str> once - clones in loop are just ref count bumps
        let from: Arc<str> = Arc::from(request.from.as_str());
        let max_hops = request.max_hops;
        let include_bridges = request.include_bridges;

        let computed: Vec<(usize, bfs::DistanceResult)> = tokio::task::spawn_blocking(move || {
            uncached_targets
                .into_iter()
                .map(|(idx, target)| {
                    let query = bfs::DistanceQuery {
                        from: Arc::clone(&from), // Cheap ref count bump
                        to: target,              // Already Arc<str>, moved
                        max_hops,
                        include_bridges,
                    };
                    (idx, bfs::compute_distance(&graph, &query))
                })
                .collect()
        })
        .await
        .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

        // Fill in computed results and cache them
        for (idx, result) in computed {
            // Cache insert first (by reference), then move into results
            if let (Some(from_id), Some(to_id)) = (
                state.graph.get_node_id(&request.from),
                state.graph.get_node_id(&result.to),
            ) {
                let cache_key = CacheKey::new(from_id, to_id, max_hops, include_bridges);
                state.cache.insert(cache_key, &result, &state.graph);
            }
            results[idx] = result; // Move, no clone
        }
    }

    Ok(Json(BatchDistanceResponse {
        from: request.from,
        results,
    }))
}

pub async fn get_follows(
    State(state): State<AppState>,
    Query(params): Query<FollowsQueryParams>,
) -> Result<Json<FollowsResponse>, ErrorResponse> {
    validate_pubkey(&params.pubkey)?;

    let follows = state.graph.get_follows(&params.pubkey).unwrap_or_default();
    let total = follows.len();
    let follows: Vec<String> = follows.into_iter()
        .skip(params.offset)
        .take(params.limit.min(5000))
        .collect();

    Ok(Json(FollowsResponse {
        pubkey: params.pubkey,
        follows,
        total,
    }))
}

pub async fn get_common_follows(
    State(state): State<AppState>,
    Query(params): Query<CommonFollowsQueryParams>,
) -> Result<Json<CommonFollowsResponse>, ErrorResponse> {
    validate_pubkey(&params.from)?;
    validate_pubkey(&params.to)?;

    let graph = state.graph.clone();
    let from = params.from.clone();
    let to = params.to.clone();

    let common_follows = tokio::task::spawn_blocking(move || {
        let from_follows = graph.get_follows(&from).unwrap_or_default();
        let to_follows = graph.get_follows(&to).unwrap_or_default();
        let from_set: std::collections::HashSet<_> = from_follows.into_iter().collect();
        to_follows.into_iter().filter(|f| from_set.contains(f)).collect::<Vec<String>>()
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

    Ok(Json(CommonFollowsResponse {
        from: params.from,
        to: params.to,
        common_follows,
    }))
}

pub async fn get_path(
    State(state): State<AppState>,
    Query(params): Query<PathQueryParams>,
) -> Result<Json<PathResponse>, ErrorResponse> {
    validate_pubkey(&params.from)?;
    validate_pubkey(&params.to)?;
    validate_max_hops(params.max_hops)?;

    let graph = state.graph.clone();
    let query = bfs::PathQuery {
        from: std::sync::Arc::from(params.from.as_str()),
        to: std::sync::Arc::from(params.to.as_str()),
        max_hops: params.max_hops,
    };

    let result = tokio::task::spawn_blocking(move || {
        bfs::compute_path(&graph, &query)
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

    Ok(Json(PathResponse {
        from: params.from,
        to: params.to,
        path: result.path.map(|p| p.into_iter().map(|s| s.to_string()).collect()),
    }))
}

pub async fn get_stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.graph.stats();
    let cache_stats = state.cache.stats();
    let lock_metrics = state.graph.lock_metrics();
    Json(StatsResponse {
        node_count: stats.node_count,
        edge_count: stats.edge_count,
        nodes_with_follows: stats.nodes_with_follows,
        cache: cache_stats,
        locks: lock_metrics,
    })
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

pub fn create_router(state: AppState, rate_limit_per_minute: u32) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE]);

    // Per-IP rate limiting with token bucket algorithm
    let per_second = std::cmp::max(1, rate_limit_per_minute / 60);
    let burst_size = std::cmp::max(5, rate_limit_per_minute / 6); // 10 sec burst

    let governor_conf = GovernorConfigBuilder::default()
        .per_second(per_second as u64)
        .burst_size(burst_size)
        .key_extractor(SmartIpKeyExtractor)
        .finish()
        .expect("Invalid rate limit configuration");

    info!(
        "Rate limiter: {} req/sec, burst size {}, body limit {}KB",
        per_second, burst_size, REQUEST_BODY_LIMIT / 1024
    );

    Router::new()
        .route("/health", get(health))
        .route("/stats", get(get_stats))
        .route("/distance", get(get_distance))
        .route("/distance/batch", post(batch_distance))
        .route("/follows", get(get_follows))
        .route("/common-follows", get(get_common_follows))
        .route("/path", get(get_path))
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(REQUEST_BODY_LIMIT))
        .layer(GovernorLayer {
            config: Arc::new(governor_conf),
        })
        .with_state(state)
}

pub async fn start_server(state: AppState, port: u16, rate_limit_per_minute: u32) -> anyhow::Result<()> {
    let router = create_router(state, rate_limit_per_minute);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    info!("HTTP server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Test router without rate limiting (SmartIpKeyExtractor fails in tests)
    fn create_test_router(state: AppState) -> Router {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE]);

        Router::new()
            .route("/health", get(health))
            .route("/stats", get(get_stats))
            .route("/distance", get(get_distance))
            .route("/distance/batch", post(batch_distance))
            .route("/follows", get(get_follows))
            .route("/common-follows", get(get_common_follows))
            .route("/path", get(get_path))
            .layer(cors)
            .with_state(state)
    }

    fn create_test_state() -> AppState {
        let graph = Arc::new(WotGraph::new());

        // Set up test data
        graph.update_follows(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()],
            None,
            None,
        );

        let config = Arc::new(Config::from_env());
        let cache = Arc::new(QueryCache::new(config.cache_size, config.cache_ttl_secs));

        AppState {
            graph,
            config,
            cache,
        }
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let response = router
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let response = router
            .oneshot(Request::builder().uri("/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_distance_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let from = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/distance?from={}&to={}", from, to))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_pubkey() {
        let state = create_test_state();
        let router = create_test_router(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/distance?from=invalid&to=alsoinvalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_bypass_cache() {
        let state = create_test_state();
        let router = create_test_router(state.clone());

        let from = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        // First request without bypass_cache (populates cache)
        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/distance?from={}&to={}", from, to))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify cache was populated
        let from_id = state.graph.get_node_id(from).unwrap();
        let to_id = state.graph.get_node_id(to).unwrap();
        let cache_key = CacheKey::new(from_id, to_id, MAX_HOPS_DEFAULT, false);
        assert!(state.cache.get(&cache_key, &state.graph).is_some());

        // Second request with bypass_cache=true should still succeed
        let router2 = create_test_router(state);
        let response = router2
            .oneshot(
                Request::builder()
                    .uri(format!("/distance?from={}&to={}&bypass_cache=true", from, to))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_follows_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let pubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/follows?pubkey={}", pubkey))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_common_follows_endpoint() {
        let graph = Arc::new(WotGraph::new());

        // Set up test data where alice and bob both follow carol
        graph.update_follows(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &[
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_string(),
            ],
            None,
            None,
        );
        graph.update_follows(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            &[
                "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_string(),
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string(),
            ],
            None,
            None,
        );

        let config = Arc::new(Config::from_env());
        let cache = Arc::new(QueryCache::new(config.cache_size, config.cache_ttl_secs));
        let state = AppState { graph, config, cache };
        let router = create_test_router(state);

        let from = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/common-follows?from={}&to={}", from, to))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_path_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let from = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let to = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let response = router
            .oneshot(
                Request::builder()
                    .uri(format!("/path?from={}&to={}", from, to))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
