use std::env;

// Security limits
pub const MAX_HOPS_LIMIT: u8 = 5;
pub const MAX_HOPS_DEFAULT: u8 = 3;
pub const CACHE_SIZE_MAX: usize = 100_000;
pub const CACHE_SIZE_DEFAULT: usize = 10_000;
pub const RATE_LIMIT_MAX: u32 = 1000;
pub const RATE_LIMIT_DEFAULT: u32 = 100;
#[allow(dead_code)] // Reserved for future timeout configuration
pub const REQUEST_TIMEOUT_SECS: u64 = 30;
pub const REQUEST_BODY_LIMIT: usize = 1024 * 1024; // 1MB

#[derive(Clone)]
pub struct Config {
    pub relays: Vec<String>,
    pub http_port: u16,
    pub db_path: String,
    pub dvm_enabled: bool,
    pub dvm_private_key: Option<String>,
    pub rate_limit_per_minute: u32,
    pub max_hops: u8,
    pub cache_size: usize,
    pub cache_ttl_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let relays = env::var("RELAYS")
            .unwrap_or_else(|_| "wss://relay.damus.io,wss://nos.lol,wss://relay.primal.net/,wss://relay.mostr.pub/".into())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let http_port = env::var("HTTP_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

        let db_path = env::var("DB_PATH").unwrap_or_else(|_| "wot.db".into());

        let dvm_enabled = env::var("DVM_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let dvm_private_key = env::var("DVM_PRIVATE_KEY").ok();

        // Bounded rate limit (1-1000 req/min)
        let rate_limit_per_minute = env::var("RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|r| r.parse().ok())
            .map(|r: u32| r.clamp(1, RATE_LIMIT_MAX))
            .unwrap_or(RATE_LIMIT_DEFAULT);

        // Bounded max_hops (1-5)
        let max_hops = env::var("MAX_HOPS")
            .ok()
            .and_then(|h| h.parse().ok())
            .map(|h: u8| h.clamp(1, MAX_HOPS_LIMIT))
            .unwrap_or(MAX_HOPS_DEFAULT);

        // Bounded cache size (100-100,000)
        let cache_size = env::var("CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(|s: usize| s.clamp(100, CACHE_SIZE_MAX))
            .unwrap_or(CACHE_SIZE_DEFAULT);

        // Bounded cache TTL (10-3600 seconds)
        let cache_ttl_secs = env::var("CACHE_TTL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(|s: u64| s.clamp(10, 3600))
            .unwrap_or(300);

        Self {
            relays,
            http_port,
            db_path,
            dvm_enabled,
            dvm_private_key,
            rate_limit_per_minute,
            max_hops,
            cache_size,
            cache_ttl_secs,
        }
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("relays", &self.relays)
            .field("http_port", &self.http_port)
            .field("db_path", &self.db_path)
            .field("dvm_enabled", &self.dvm_enabled)
            .field("dvm_private_key", &self.dvm_private_key.as_ref().map(|_| "[REDACTED]"))
            .field("rate_limit_per_minute", &self.rate_limit_per_minute)
            .field("max_hops", &self.max_hops)
            .field("cache_size", &self.cache_size)
            .field("cache_ttl_secs", &self.cache_ttl_secs)
            .finish()
    }
}
