use crate::graph::store::MuteEvidence;
use crate::sync::ingestion::{SyncSnapshot, SyncStatus};
use axum::{
    extract::{Query, State},
    http::{header, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

use super::governor_key::OracleSmartIpKeyExtractor;
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
    pub query_slots: Arc<Semaphore>,
    pub sync: Arc<SyncStatus>,
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
    pub mute_edge_count: usize,
    pub nodes_with_mute_lists: usize,
    pub sync: SyncSnapshot,
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
            "QUERY_BUSY" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(self)).into_response()
    }
}

fn query_permit(state: &AppState) -> Result<tokio::sync::OwnedSemaphorePermit, ErrorResponse> {
    state
        .query_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| ErrorResponse {
            error: "Query capacity exhausted; retry later".to_string(),
            code: "QUERY_BUSY".to_string(),
        })
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

    let permit = query_permit(&state)?;
    let (result, revision) = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let revision = graph.revision();
        (bfs::compute_distance(&graph, &query), revision)
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

    // Cache insert (lock-free, back on async thread)
    // Re-lookup only if the first lookup was None (node may have been added during BFS)
    let from_id = from_id.or_else(|| state.graph.get_node_id(&params.from));
    let to_id = to_id.or_else(|| state.graph.get_node_id(&params.to));
    if let (Some(from_id), Some(to_id)) = (from_id, to_id) {
        let cache_key = CacheKey::new(from_id, to_id, params.max_hops, params.include_bridges);
        state
            .cache
            .insert(cache_key, &result, &state.graph, revision);
    }
    debug!(
        "Cache miss for {} -> {}, computed and cached",
        &params.from[..8],
        &params.to[..8]
    );

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

    // Compute each distinct target once, then restore the caller's ordering.
    let mut unique = Vec::new();
    let mut indices = HashMap::new();
    let mut order = Vec::with_capacity(request.targets.len());
    for target in &request.targets {
        let next = unique.len();
        let index = *indices.entry(target.clone()).or_insert_with(|| {
            unique.push(target.clone());
            next
        });
        order.push(index);
    }
    let mut computed = Vec::with_capacity(unique.len());
    for target in unique {
        computed.push(
            get_distance(
                State(state.clone()),
                Query(DistanceQueryParams {
                    from: request.from.clone(),
                    to: target,
                    max_hops: request.max_hops,
                    include_bridges: request.include_bridges,
                    bypass_cache: request.bypass_cache,
                }),
            )
            .await?
            .0,
        );
    }
    let results = order
        .into_iter()
        .map(|index| computed[index].clone())
        .collect();

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

    let (follows, total) = state
        .graph
        .get_follows_page(&params.pubkey, params.offset, params.limit.min(5000))
        .unwrap_or_default();

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

    let permit = query_permit(&state)?;
    let common_follows = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        graph.common_follows(&from, &to)
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

    let permit = query_permit(&state)?;
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        bfs::compute_path(&graph, &query)
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;

    Ok(Json(PathResponse {
        from: params.from,
        to: params.to,
        path: result
            .path
            .map(|p| p.into_iter().map(|s| s.to_string()).collect()),
    }))
}

#[derive(Debug, Serialize)]
pub struct MutesResponse {
    pub pubkey: String,
    pub mutes: Vec<String>,
    pub total: usize,
    pub public_list_known: bool,
}

pub async fn get_mutes(
    State(state): State<AppState>,
    Query(params): Query<FollowsQueryParams>,
) -> Result<Json<MutesResponse>, ErrorResponse> {
    validate_pubkey(&params.pubkey)?;
    let page = state
        .graph
        .get_mutes_page(&params.pubkey, params.offset, params.limit.min(5000));
    let public_list_known = page.is_some();
    let (mutes, total) = page.unwrap_or_default();
    Ok(Json(MutesResponse {
        pubkey: params.pubkey,
        mutes,
        total,
        public_list_known,
    }))
}

#[derive(Debug, Serialize)]
pub struct TrustResponse {
    pub follow_distance: bfs::DistanceResult,
    pub public_mute_evidence: MuteEvidence,
}

/// Separate public signals; missing public mutes are not an endorsement.
pub async fn get_trust(
    State(state): State<AppState>,
    Query(params): Query<DistanceQueryParams>,
) -> Result<Json<TrustResponse>, ErrorResponse> {
    let from = params.from.clone();
    let to = params.to.clone();
    let follow_distance = get_distance(State(state.clone()), Query(params)).await?.0;
    let permit = query_permit(&state)?;
    let graph = state.graph.clone();
    let public_mute_evidence = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        graph.mute_evidence(&from, &to)
    })
    .await
    .map_err(|_| ErrorResponse::internal("Internal computation error"))?;
    Ok(Json(TrustResponse {
        follow_distance,
        public_mute_evidence,
    }))
}

pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let status = if state.sync.ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(state.sync.snapshot()))
}

pub async fn get_stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let stats = state.graph.stats();
    let cache_stats = state.cache.stats();
    let lock_metrics = state.graph.lock_metrics();
    Json(StatsResponse {
        node_count: stats.node_count,
        edge_count: stats.edge_count,
        nodes_with_follows: stats.nodes_with_follows,
        mute_edge_count: stats.mute_edge_count,
        nodes_with_mute_lists: stats.nodes_with_mute_lists,
        sync: state.sync.snapshot(),
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

fn rate_refill_period(rate_limit_per_minute: u32) -> std::time::Duration {
    // tower_governor 0.4 takes the interval for ONE replacement token.
    std::time::Duration::from_secs_f64(60.0 / rate_limit_per_minute.max(1) as f64)
}

pub fn create_router(state: AppState, rate_limit_per_minute: u32) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE]);

    // Per-IP rate limiting with token bucket algorithm
    let refill_period = rate_refill_period(rate_limit_per_minute);
    let burst_size = std::cmp::max(5, rate_limit_per_minute / 6); // 10 sec burst

    let governor_conf = GovernorConfigBuilder::default()
        .period(refill_period)
        .burst_size(burst_size)
        .key_extractor(OracleSmartIpKeyExtractor)
        .finish()
        .expect("Invalid rate limit configuration");

    info!(
        "Rate limiter: {} req/min, burst size {}, body limit {}KB",
        rate_limit_per_minute,
        burst_size,
        REQUEST_BODY_LIMIT / 1024
    );

    Router::new()
        .route("/mutes", get(get_mutes))
        .route("/trust", get(get_trust))
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
        .merge(
            Router::new()
                .route("/health", get(health))
                .route("/ready", get(ready)),
        )
        .with_state(state)
}

pub async fn start_server(
    state: AppState,
    port: u16,
    rate_limit_per_minute: u32,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let router = create_router(state, rate_limit_per_minute);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    info!("HTTP server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    })
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Test router without rate limiting (governor needs `ConnectInfo` from `serve` in real runs)
    fn create_test_router(state: AppState) -> Router {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([header::CONTENT_TYPE]);

        Router::new()
            .route("/health", get(health))
            .route("/ready", get(ready))
            .route("/mutes", get(get_mutes))
            .route("/trust", get(get_trust))
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
            query_slots: Arc::new(Semaphore::new(2)),
            sync: Arc::new(SyncStatus::default()),
        }
    }

    async fn json_request(router: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let response = router
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .extension(axum::extract::ConnectInfo(
                        "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn public_mutes_are_separate_and_removed_follows_invalidate_http_cache() {
        let state = create_test_state();
        let from = "a".repeat(64);
        let to = "b".repeat(64);
        state.graph.update_mutes(&from, &[to.clone()], None, None);
        let router = create_router(state.clone(), 6000);
        let (status, trust) =
            json_request(router.clone(), &format!("/trust?from={from}&to={to}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(trust["follow_distance"]["hops"], 1);
        assert_eq!(trust["public_mute_evidence"]["source_mutes_target"], true);
        assert!(trust.get("score").is_none());
        let (_, mutes) =
            json_request(router.clone(), &format!("/mutes?pubkey={from}&limit=0")).await;
        assert_eq!(mutes["total"], 1);
        assert_eq!(mutes["mutes"], serde_json::json!([]));
        assert_eq!(mutes["public_list_known"], true);
        let (_, unknown) = json_request(router.clone(), &format!("/mutes?pubkey={to}")).await;
        assert_eq!(unknown["public_list_known"], false);
        state.graph.update_mutes(&to, &[], None, None);
        let (_, known_empty) = json_request(router.clone(), &format!("/mutes?pubkey={to}")).await;
        assert_eq!(known_empty["public_list_known"], true);
        state.graph.update_follows(&from, &[], None, None);
        let (_, distance) = json_request(router, &format!("/distance?from={from}&to={to}")).await;
        assert!(distance["hops"].is_null());
    }

    #[test]
    fn rate_configuration_uses_token_interval() {
        assert_eq!(
            rate_refill_period(600),
            std::time::Duration::from_millis(100)
        );
        assert_eq!(rate_refill_period(30), std::time::Duration::from_secs(2));
    }

    #[tokio::test]
    async fn production_router_readiness_and_rate_limit() {
        let router = create_router(create_test_state(), 1);
        let (status, _) = json_request(router.clone(), "/ready").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        for _ in 0..5 {
            assert_eq!(
                json_request(router.clone(), "/stats").await.0,
                StatusCode::OK
            );
        }
        assert_eq!(
            json_request(router.clone(), "/health").await.0,
            StatusCode::OK
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .extension(axum::extract::ConnectInfo(
                        "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn saturated_queries_return_503_without_affecting_liveness() {
        let state = create_test_state();
        let _permits = state
            .query_slots
            .clone()
            .acquire_many_owned(2)
            .await
            .unwrap();
        let router = create_router(state, 6000);
        let (status, body) = json_request(
            router.clone(),
            &format!("/distance?from={}&to={}", "a".repeat(64), "b".repeat(64)),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "QUERY_BUSY");
        assert_eq!(json_request(router, "/health").await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn batch_preserves_duplicate_order_and_values() {
        let state = create_test_state();
        let from = "a".repeat(64);
        let to = "b".repeat(64);
        let response = create_router(state, 6000)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/distance/batch")
                    .header(header::CONTENT_TYPE, "application/json")
                    .extension(axum::extract::ConnectInfo(
                        "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
                    ))
                    .body(Body::from(
                        serde_json::json!({"from":from,"targets":[to,from,to],"bypass_cache":true})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1_000_000)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["results"][0]["hops"], 1);
        assert_eq!(value["results"][1]["hops"], 0);
        assert_eq!(value["results"][0], value["results"][2]);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let state = create_test_state();
        let router = create_test_router(state);

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
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
                    .uri(format!(
                        "/distance?from={}&to={}&bypass_cache=true",
                        from, to
                    ))
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
        let state = AppState {
            graph,
            config,
            cache,
            query_slots: Arc::new(Semaphore::new(2)),
            sync: Arc::new(SyncStatus::default()),
        };
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
