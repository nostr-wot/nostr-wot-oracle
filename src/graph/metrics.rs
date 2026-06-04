use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Simple lock metrics for monitoring contention
pub struct LockMetrics {
    // Write lock metrics
    write_lock_count: AtomicU64,
    write_lock_total_ns: AtomicU64,
    write_lock_max_ns: AtomicU64,

    // Read lock metrics
    read_lock_count: AtomicU64,
    read_lock_total_ns: AtomicU64,
    read_lock_max_ns: AtomicU64,
}

impl LockMetrics {
    pub const fn new() -> Self {
        Self {
            write_lock_count: AtomicU64::new(0),
            write_lock_total_ns: AtomicU64::new(0),
            write_lock_max_ns: AtomicU64::new(0),
            read_lock_count: AtomicU64::new(0),
            read_lock_total_ns: AtomicU64::new(0),
            read_lock_max_ns: AtomicU64::new(0),
        }
    }

    pub fn record_write(&self, duration: Duration) {
        let ns = duration.as_nanos() as u64;
        self.write_lock_count.fetch_add(1, Ordering::Relaxed);
        self.write_lock_total_ns.fetch_add(ns, Ordering::Relaxed);
        self.write_lock_max_ns.fetch_max(ns, Ordering::Relaxed);
    }

    pub fn record_read(&self, duration: Duration) {
        let ns = duration.as_nanos() as u64;
        self.read_lock_count.fetch_add(1, Ordering::Relaxed);
        self.read_lock_total_ns.fetch_add(ns, Ordering::Relaxed);
        self.read_lock_max_ns.fetch_max(ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LockMetricsSnapshot {
        let write_count = self.write_lock_count.load(Ordering::Relaxed);
        let write_total_ns = self.write_lock_total_ns.load(Ordering::Relaxed);
        let read_count = self.read_lock_count.load(Ordering::Relaxed);
        let read_total_ns = self.read_lock_total_ns.load(Ordering::Relaxed);

        LockMetricsSnapshot {
            write_lock_count: write_count,
            write_lock_avg_us: write_total_ns
                .checked_div(write_count)
                .map(|ns| ns / 1000)
                .unwrap_or(0),
            write_lock_max_us: self.write_lock_max_ns.load(Ordering::Relaxed) / 1000,
            read_lock_count: read_count,
            read_lock_avg_us: read_total_ns
                .checked_div(read_count)
                .map(|ns| ns / 1000)
                .unwrap_or(0),
            read_lock_max_us: self.read_lock_max_ns.load(Ordering::Relaxed) / 1000,
        }
    }

    #[allow(dead_code)] // Public API for metrics management after warmup
    pub fn reset(&self) {
        self.write_lock_count.store(0, Ordering::Relaxed);
        self.write_lock_total_ns.store(0, Ordering::Relaxed);
        self.write_lock_max_ns.store(0, Ordering::Relaxed);
        self.read_lock_count.store(0, Ordering::Relaxed);
        self.read_lock_total_ns.store(0, Ordering::Relaxed);
        self.read_lock_max_ns.store(0, Ordering::Relaxed);
    }
}

impl Default for LockMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LockMetricsSnapshot {
    pub write_lock_count: u64,
    pub write_lock_avg_us: u64,
    pub write_lock_max_us: u64,
    pub read_lock_count: u64,
    pub read_lock_avg_us: u64,
    pub read_lock_max_us: u64,
}

/// RAII guard for timing lock duration
pub struct LockTimer<'a> {
    metrics: &'a LockMetrics,
    start: Instant,
    is_write: bool,
}

impl<'a> LockTimer<'a> {
    pub fn write(metrics: &'a LockMetrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            is_write: true,
        }
    }

    pub fn read(metrics: &'a LockMetrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
            is_write: false,
        }
    }
}

impl Drop for LockTimer<'_> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        if self.is_write {
            self.metrics.record_write(duration);
        } else {
            self.metrics.record_read(duration);
        }
    }
}
