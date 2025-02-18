use crate::tables::{AccountTrieTable, StorageTrieTable, TrieTable};
use reth_db_api::{cursor::DbCursorRO, DatabaseError};
use reth_primitives::{Account, H256};
use reth_trie_db::{
    cursor::{TrieCursor, TrieCursorFactory}, // PP:: Cursor???
    HashedCursor,
};
use rocksdb::{ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
use std::sync::Arc;

/// RocksDB implementation of trie cursor
pub struct RocksTrieCursor<T> {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    iter: rocksdb::DBIterator<Arc<DB>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> RocksTrieCursor<T> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);
        read_opts.set_prefetch_size(1024 * 1024); // 1MB prefetch

        let iter = db.iterator_cf_opt(&cf, read_opts, IteratorMode::Start);

        Ok(Self { db, cf, iter, _marker: std::marker::PhantomData })
    }
}

impl<T> TrieCursor for RocksTrieCursor<T> {
    fn seek_exact(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError> {
        match self.iter.seek(key) {
            Some(Ok((k, v))) if k == key => Ok(Some(v.to_vec())),
            _ => Ok(None),
        }
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, DatabaseError> {
        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((k.to_vec(), v.to_vec()))),
            _ => Ok(None),
        }
    }
}

/// Factory for creating trie cursors
pub struct RocksTrieCursorFactory {
    db: Arc<DB>,
}

impl RocksTrieCursorFactory {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }
}

impl TrieCursorFactory for RocksTrieCursorFactory {
    type TrieCursor = RocksTrieCursor<TrieTable>;
    type AccountCursor = RocksTrieCursor<AccountTrieTable>;
    type StorageCursor = RocksTrieCursor<StorageTrieTable>;

    fn create_trie_cursor(&self) -> Result<Self::TrieCursor, DatabaseError> {
        let cf = self
            .db
            .cf_handle(TrieTable::NAME)
            .ok_or_else(|| DatabaseError::Other("Trie column family not found".into()))?;
        RocksTrieCursor::new(self.db.clone(), cf.clone())
    }

    fn create_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        let cf = self
            .db
            .cf_handle(AccountTrieTable::NAME)
            .ok_or_else(|| DatabaseError::Other("Account trie column family not found".into()))?;
        RocksTrieCursor::new(self.db.clone(), cf.clone())
    }

    fn create_storage_cursor(&self) -> Result<Self::StorageCursor, DatabaseError> {
        let cf = self
            .db
            .cf_handle(StorageTrieTable::NAME)
            .ok_or_else(|| DatabaseError::Other("Storage trie column family not found".into()))?;
        RocksTrieCursor::new(self.db.clone(), cf.clone())
    }
}
