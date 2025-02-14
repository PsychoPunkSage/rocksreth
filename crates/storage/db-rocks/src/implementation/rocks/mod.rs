use crate::tables::utils::TableUtils;
use parking_lot::RwLock;
use reth_db_api::{
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use rocksdb::{ColumnFamilyDescriptor, Options, ReadOptions, WriteOptions, DB};
use std::{path::Path, sync::Arc};

pub mod cursor;
pub mod dupsort;
pub mod tx;

/// Default write buffer size (64 MB)
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// Default block cache size (512 MB)
const DEFAULT_BLOCK_CACHE_SIZE: usize = 512 * 1024 * 1024;

/// Configuration for RocksDB
#[derive(Debug, Clone)]
pub struct RocksDBConfig {
    /// Path to database
    pub path: String,
    /// Maximum number of open files
    pub max_open_files: i32,
    /// Size of block cache
    pub block_cache_size: usize,
    /// Size of write buffer
    pub write_buffer_size: usize,
    /// Whether to create database if missing
    pub create_if_missing: bool,
    /// Whether to use direct I/O for reads and writes
    pub use_direct_io: bool,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        Self {
            path: String::from("rocksdb"),
            max_open_files: 512,
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            create_if_missing: true,
            use_direct_io: false,
        }
    }
}

/// RocksDB instance wrapper
#[derive(Debug)]
pub struct RocksDB {
    /// The RocksDB instance
    db: Arc<DB>,
    /// Database configuration
    config: RocksDBConfig,
    /// Write options
    write_opts: WriteOptions,
    /// Read options
    read_opts: ReadOptions,
}

impl RocksDB {
    /// Opens RocksDB with given configuration
    pub fn open(config: RocksDBConfig) -> Result<Self, DatabaseError> {
        let path = Path::new(&config.path);

        // Create database options
        let mut opts = Options::default();
        opts.create_if_missing(config.create_if_missing);
        opts.set_max_open_files(config.max_open_files);
        opts.set_write_buffer_size(config.write_buffer_size);
        opts.set_use_direct_io_for_flush_and_compaction(config.use_direct_io);
        opts.set_use_direct_reads(config.use_direct_io);

        // Setup block cache
        let cache = rocksdb::Cache::new_lru_cache(config.block_cache_size)?;
        opts.set_block_cache(&cache);

        // Get column family descriptors
        let cf_descriptors = if path.exists() {
            TableUtils::get_existing_cf_descriptors(path)?
        } else {
            TableUtils::get_expected_table_names()
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        };

        // Open database with column families
        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| DatabaseError::Other(format!("Failed to open RocksDB: {}", e)))?;

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        Ok(Self { db: Arc::new(db), config, write_opts, read_opts })
    }

    /// Create read-only transaction
    pub fn transaction(&self) -> Result<tx::RocksTransaction<false>, DatabaseError> {
        Ok(tx::RocksTransaction::new(self.db.clone(), false))
    }

    /// Create read-write transaction
    pub fn transaction_mut(&self) -> Result<tx::RocksTransaction<true>, DatabaseError> {
        Ok(tx::RocksTransaction::new(self.db.clone(), true))
    }

    /// Get database statistics
    pub fn get_statistics(&self) -> Option<String> {
        self.db.property_value("rocksdb.stats").unwrap_or_default()
    }

    /// Trigger compaction on all column families
    pub fn compact_all(&self) -> Result<(), DatabaseError> {
        for cf_name in TableUtils::get_expected_table_names() {
            if let Some(cf) = self.db.cf_handle(&cf_name) {
                self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
            }
        }
        Ok(())
    }

    /// Get approximate size of all tables
    pub fn get_estimated_table_sizes(&self) -> Result<Vec<(String, u64)>, DatabaseError> {
        let mut sizes = Vec::new();

        for cf_name in TableUtils::get_expected_table_names() {
            if let Some(cf) = self.db.cf_handle(&cf_name) {
                if let Some(size) =
                    self.db.property_int_value_cf(&cf, "rocksdb.estimate-live-data-size")?
                {
                    sizes.push((cf_name, size));
                }
            }
        }

        Ok(sizes)
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        // Ensure proper cleanup
        if Arc::strong_count(&self.db) == 1 {
            // We're the last reference, flush everything
            for cf_name in TableUtils::get_expected_table_names() {
                if let Some(cf) = self.db.cf_handle(&cf_name) {
                    let _ = self.db.flush_cf(&cf);
                }
            }
        }
    }
}
