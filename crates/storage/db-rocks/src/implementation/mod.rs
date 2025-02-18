pub mod rocks;

use parking_lot::RwLock;
use reth_db_api::{
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use rocksdb::{ColumnFamilyDescriptor, Options, ReadOptions, WriteOptions, DB};
use std::{path::Path, sync::Arc};

/// RocksDB instance wrapper
#[derive(Debug)]
pub struct RocksDB {
    /// The RocksDB instance
    db: Arc<DB>,
    /// Write options
    write_opts: WriteOptions,
    /// Read options
    read_opts: ReadOptions,
}

impl RocksDB {
    /// Opens a new RocksDB instance at the given path
    pub fn open(path: &Path, create_if_missing: bool) -> Result<Self, DatabaseError> {
        let mut opts = Options::default();
        opts.create_if_missing(create_if_missing);

        // Configure RocksDB options
        opts.set_max_open_files(1024);
        opts.set_use_fsync(false);
        opts.set_keep_log_file_num(1);

        // Setup column families for tables
        let cfs = Self::setup_column_families(&opts, path)?;

        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| DatabaseError::Other(format!("Failed to open RocksDB: {}", e)))?;

        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        Ok(Self { db: Arc::new(db), write_opts, read_opts })
    }

    /// Create a read-only transaction
    pub fn transaction(&self) -> Result<RocksTransaction<false>, DatabaseError> {
        Ok(RocksTransaction::new(self.db.clone(), false))
    }

    /// Create a read-write transaction  
    pub fn transaction_mut(&self) -> Result<RocksTransaction<true>, DatabaseError> {
        Ok(RocksTransaction::new(self.db.clone(), true))
    }

    /// Setup column families for all tables
    fn setup_column_families(
        opts: &Options,
        path: &Path,
    ) -> Result<Vec<ColumnFamilyDescriptor>, DatabaseError> {
        // TODO: Implement column family setup based on Tables enum
        Ok(vec![])
    }
}
