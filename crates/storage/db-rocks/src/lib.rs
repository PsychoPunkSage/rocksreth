/*
Cargo.toml           -> Defines dependencies and package configuration for the RocksDB implementation
benches/criterion.rs -> Main benchmark configuration and setup for RocksDB performance testing
benches/get.rs      -> Specific benchmarks for testing database read operations
benches/utils.rs    -> Shared utilities and helper functions used across benchmarks

src/lib.rs          -> Main entry point that exports public API and manages module organization
src/implementation/mod.rs      -> Manages database implementation modules and common traits
src/implementation/rocks/cursor.rs -> Implements iterators/cursors for traversing RocksDB data
src/implementation/rocks/mod.rs    -> Core RocksDB wrapper and database operations implementation
src/implementation/rocks/tx.rs     -> Handles RocksDB transaction management and batching

src/metrics.rs      -> Defines and collects performance and operational metrics for monitoring
src/tables/codecs/mod.rs -> Handles serialization/deserialization of data for storage
src/tables/mod.rs   -> Defines database table structures and schemas
src/tables/raw.rs   -> Low-level table operations without encoding/decoding
src/tables/utils.rs -> Helper functions for table management and operations
src/version.rs      -> Manages database versioning and migration logic
*/
//! RocksDB implementation for RETH
//!
//! This crate provides a RocksDB-backed implementation of the database interfaces defined in reth-db-api.

//! RocksDB implementation for RETH
//!
//! This crate provides a RocksDB-backed storage implementation for RETH.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(missing_copy_implementations)]
#![warn(rust_2018_idioms)]

mod db;
mod errors;
mod implementation;
mod metrics;
mod tables;
mod test;
mod version;

pub use errors::RocksDBError;
use implementation::rocks::RocksDB;
use metrics::DatabaseMetrics;
use std::{path::Path, sync::Arc};

// Re-export important types
pub use implementation::rocks::{tx::RocksTransaction, RocksDBConfig};

/// Database environment for RocksDB
#[derive(Debug)]
pub struct DatabaseEnv {
    /// The underlying RocksDB instance
    inner: Arc<RocksDB>,
    /// Metrics collector
    metrics: Option<Arc<DatabaseMetrics>>,
    /// Version manager
    version_manager: Arc<version::VersionManager>,
}

impl DatabaseEnv {
    /// Opens a new database environment
    pub fn open(path: &Path, config: RocksDBConfig) -> Result<Self, RocksDBError> {
        // Initialize RocksDB
        let inner = Arc::new(RocksDB::open(config)?);

        // Initialize version manager
        let version_manager = Arc::new(version::VersionManager::new(inner.as_ref())?);

        // Run migrations if needed
        if version_manager.needs_migration() {
            version_manager.migrate(inner.as_ref())?;
        }

        Ok(Self { inner, metrics: None, version_manager })
    }

    /// Enable metrics collection
    pub fn with_metrics(mut self) -> Self {
        self.metrics = Some(Arc::new(DatabaseMetrics::new()));
        self
    }

    /// Get reference to metrics
    pub fn metrics(&self) -> Option<Arc<DatabaseMetrics>> {
        self.metrics.clone()
    }

    /// Get database version
    pub fn version(&self) -> u32 {
        self.version_manager.current_version()
    }

    /// Compact the entire database
    pub fn compact(&self) -> Result<(), RocksDBError> {
        self.inner.compact_all()?;
        Ok(())
    }

    /// Get database statistics
    pub fn get_stats(&self) -> Option<String> {
        self.inner.get_statistics()
    }
}

impl reth_db_api::Database for DatabaseEnv {
    type TX = RocksTransaction<false>;
    type TXMut = RocksTransaction<true>;

    fn tx(&self) -> Result<Self::TX, reth_db_api::DatabaseError> {
        if let Some(metrics) = &self.metrics {
            metrics.record_tx_start(false);
        }
        self.inner.transaction().map_err(Into::into)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, reth_db_api::DatabaseError> {
        if let Some(metrics) = &self.metrics {
            metrics.record_tx_start(true);
        }
        self.inner.transaction_mut().map_err(Into::into)
    }
}

impl Drop for DatabaseEnv {
    fn drop(&mut self) {
        // Ensure metrics are flushed
        if let Some(metrics) = &self.metrics {
            if let Some(stats) = self.get_stats() {
                // Final metrics update
                metrics.record_final_stats(&stats);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_database_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = RocksDBConfig {
            path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let db = DatabaseEnv::open(temp_dir.path(), config).unwrap();
        assert_eq!(db.version(), 1);
    }

    #[test]
    fn test_metrics_collection() {
        let temp_dir = TempDir::new().unwrap();
        let config = RocksDBConfig {
            path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let db = DatabaseEnv::open(temp_dir.path(), config).unwrap().with_metrics();

        assert!(db.metrics().is_some());
    }
}

/*
> This codebase implements a RocksDB storage layer for RETH. At its core, it provides a way to store and retrieve blockchain data using RocksDB instead of MDBX. The implementation handles database operations through tables (like accounts, transactions, etc.) where each table is a separate column family in RocksDB.
> The cursor system lets you iterate through data in these tables, similar to how you'd scan through entries in a database. The DUPSORT feature (which MDBX has natively but RocksDB doesn't) is implemented manually to allow multiple values per key, which is crucial for certain blockchain data structures like state history.
> All database operations are wrapped in transactions, either read-only or read-write, to maintain data consistency. The metrics module tracks performance and usage statistics, while the version management ensures proper database schema upgrades.
> The codecs part handles how data is serialized and deserialized - converting Ethereum types (like addresses and transactions) into bytes for storage and back. Error handling is centralized to provide consistent error reporting across all database operations.
> Think of it as a specialized database adapter that makes RocksDB behave exactly how RETH expects its storage layer to work, with all the specific features needed for an Ethereum client. It's basically translating RETH's storage requirements into RocksDB operations while maintaining all the necessary blockchain-specific functionality.
*/
