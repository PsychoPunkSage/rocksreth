use metrics::{Counter, Gauge, Histogram};
use std::sync::Arc;

/// Metrics collector for RocksDB operations
#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    /// Read transaction count
    pub tx_read: Counter,
    /// Write transaction count
    pub tx_write: Counter,
    /// Cursor operations count
    pub cursor_ops: Counter,
    /// Database size in bytes
    pub db_size: Gauge,
    /// Read latency histogram
    pub read_latency: Histogram,
    /// Write latency histogram
    pub write_latency: Histogram,
    /// Transaction duration histogram
    pub tx_duration: Histogram,
    /// Number of compactions
    pub compactions: Counter,
    /// Cache hit ratio
    pub cache_hit_ratio: Gauge,
    /// Number of keys in database
    pub total_keys: Gauge,
}

impl DatabaseMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            tx_read: metrics::counter!("db_tx_read_total"),
            tx_write: metrics::counter!("db_tx_write_total"),
            cursor_ops: metrics::counter!("db_cursor_ops_total"),
            db_size: metrics::gauge!("db_size_bytes"),
            read_latency: metrics::histogram!("db_read_latency"),
            write_latency: metrics::histogram!("db_write_latency"),
            tx_duration: metrics::histogram!("db_tx_duration"),
            compactions: metrics::counter!("db_compactions_total"),
            cache_hit_ratio: metrics::gauge!("db_cache_hit_ratio"),
            total_keys: metrics::gauge!("db_total_keys"),
        }
    }

    /// Record transaction start
    pub fn record_tx_start(&self, is_write: bool) {
        if is_write {
            self.tx_write.increment(1);
        } else {
            self.tx_read.increment(1);
        }
    }

    /// Record operation latency
    pub fn record_operation_latency(&self, is_write: bool, duration: std::time::Duration) {
        let latency = duration.as_secs_f64();
        if is_write {
            self.write_latency.record(latency);
        } else {
            self.read_latency.record(latency);
        }
    }

    /// Record cursor operation
    pub fn record_cursor_op(&self) {
        self.cursor_ops.increment(1);
    }

    /// Update database size
    pub fn update_db_size(&self, size: u64) {
        self.db_size.set(size as f64);
    }

    /// Record transaction duration
    pub fn record_tx_duration(&self, duration: std::time::Duration) {
        self.tx_duration.record(duration.as_secs_f64());
    }

    /// Record compaction
    pub fn record_compaction(&self) {
        self.compactions.increment(1);
    }

    /// Update cache statistics
    pub fn update_cache_stats(&self, hits: u64, misses: u64) {
        let ratio = if hits + misses > 0 { hits as f64 / (hits + misses) as f64 } else { 0.0 };
        self.cache_hit_ratio.set(ratio);
    }

    /// Update total keys count
    pub fn update_total_keys(&self, count: u64) {
        self.total_keys.set(count as f64);
    }
}

/// RocksDB specific metrics collector
#[derive(Debug, Clone)]
pub struct RocksDBMetrics {
    /// Common database metrics
    pub common: DatabaseMetrics,
    /// Write amplification factor
    pub write_amp: Gauge,
    /// Read amplification factor
    pub read_amp: Gauge,
    /// Memory usage metrics
    pub memory_usage: RocksDBMemoryMetrics,
    /// Per-level metrics
    pub level_metrics: RocksDBLevelMetrics,
}

/// Memory usage metrics for RocksDB
#[derive(Debug, Clone)]
pub struct RocksDBMemoryMetrics {
    /// Index and filter blocks memory
    pub index_filter_blocks: Gauge,
    /// Memtable memory usage
    pub memtable: Gauge,
    /// Block cache memory usage
    pub block_cache: Gauge,
}

/// Per-level metrics for RocksDB
#[derive(Debug, Clone)]
pub struct RocksDBLevelMetrics {
    /// Size of each level
    pub level_size: Vec<Gauge>,
    /// Files in each level
    pub level_files: Vec<Gauge>,
    /// Read hits for each level
    pub level_read_hits: Vec<Counter>,
    /// Write amplification for each level
    pub level_write_amp: Vec<Gauge>,
}

impl RocksDBMetrics {
    /// Create new RocksDB metrics collector
    pub fn new() -> Self {
        Self {
            common: DatabaseMetrics::new(),
            write_amp: metrics::gauge!("rocksdb_write_amplification"),
            read_amp: metrics::gauge!("rocksdb_read_amplification"),
            memory_usage: RocksDBMemoryMetrics {
                index_filter_blocks: metrics::gauge!("rocksdb_memory_index_filter_blocks_bytes"),
                memtable: metrics::gauge!("rocksdb_memory_memtable_bytes"),
                block_cache: metrics::gauge!("rocksdb_memory_block_cache_bytes"),
            },
            level_metrics: RocksDBLevelMetrics {
                level_size: (0..7)
                    .map(|level| metrics::gauge!(format!("rocksdb_level_{}_size_bytes", level)))
                    .collect(),
                level_files: (0..7)
                    .map(|level| metrics::gauge!(format!("rocksdb_level_{}_files", level)))
                    .collect(),
                level_read_hits: (0..7)
                    .map(|level| metrics::counter!(format!("rocksdb_level_{}_read_hits", level)))
                    .collect(),
                level_write_amp: (0..7)
                    .map(|level| {
                        metrics::gauge!(format!("rocksdb_level_{}_write_amplification", level))
                    })
                    .collect(),
            },
        }
    }

    /// Update metrics from RocksDB statistics
    pub fn update_from_stats(&self, stats: &str) {
        // Parse RocksDB statistics and update metrics
        for line in stats.lines() {
            match line {
                s if s.starts_with("Uptime(secs)") => {
                    // Extract uptime
                }
                s if s.starts_with("Cumulative writes") => {
                    // Extract write stats
                }
                s if s.starts_with("Cumulative WAL") => {
                    // Extract WAL stats
                }
                s if s.starts_with("Block cache") => {
                    // Extract block cache stats
                    if let Some(hits) = extract_stat(s, "hit count") {
                        if let Some(misses) = extract_stat(s, "miss count") {
                            self.common.update_cache_stats(hits, misses);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Update level-specific metrics
    pub fn update_level_metrics(
        &self,
        level: usize,
        size: u64,
        files: u64,
        read_hits: u64,
        write_amp: f64,
    ) {
        if level < self.level_metrics.level_size.len() {
            self.level_metrics.level_size[level].set(size as f64);
            self.level_metrics.level_files[level].set(files as f64);
            self.level_metrics.level_read_hits[level].increment(read_hits);
            self.level_metrics.level_write_amp[level].set(write_amp);
        }
    }
}

/// Helper function to extract numeric values from RocksDB stats
fn extract_stat(line: &str, pattern: &str) -> Option<u64> {
    if let Some(pos) = line.find(pattern) {
        let start = pos + pattern.len();
        let end =
            line[start..].find(|c: char| !c.is_digit(10)).map(|e| start + e).unwrap_or(line.len());
        line[start..end].trim().parse().ok()
    } else {
        None
    }
}
