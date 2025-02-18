use crate::{
    implementation::rocks::RocksTransaction,
    tables::{AccountTrieTable, StorageTrieTable, TrieTable, TrieTableConfigs},
};
use reth_db_api::{database::Database, DatabaseError};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;
use std::sync::Arc;

/// RocksDB database implementation
#[derive(Debug)]
pub struct RocksDB {
    /// Inner database instance
    db: Arc<DB>,
}

impl RocksDB {
    /// Open database at the given path
    pub fn open(path: &Path) -> Result<Self, DatabaseError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Initialize column families for trie tables
        let trie_config = TrieTableConfigs::default();
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(TrieTable::NAME, trie_config.column_config()),
            ColumnFamilyDescriptor::new(AccountTrieTable::NAME, trie_config.column_config()),
            ColumnFamilyDescriptor::new(StorageTrieTable::NAME, trie_config.column_config()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| DatabaseError::Other(format!("Failed to open database: {}", e)))?;

        Ok(Self { db: Arc::new(db) })
    }
}

impl Database for RocksDB {
    type TX = RocksTransaction<false>;
    type TXMut = RocksTransaction<true>;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        Ok(RocksTransaction::new(self.db.clone(), false))
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        Ok(RocksTransaction::new(self.db.clone(), true))
    }
}
