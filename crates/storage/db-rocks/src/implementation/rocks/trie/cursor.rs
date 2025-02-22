use crate::tables::trie::{AccountTrieTable, StorageTrieTable, TrieTable};
use alloy_primitives::B256;
use reth_db_api::{cursor::DbCursorRO, DatabaseError};
use reth_db_api::{Decode, Encode};
use reth_primitives::Account;
use reth_trie_db::{
    cursor::{TrieCursor, TrieCursorFactory}, // PP:: Cursor???
    HashedCursor,
}; // For encoding/decoding
   // use reth_trie::trie_cursor::{TrieCursor, TrieCursorFactory};
   // use reth_trie::{BranchNodeCompact, Nibbles};
use rocksdb::{ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
use std::sync::Arc;

/// RocksDB implementation of trie cursor
pub struct RocksTrieCursor<'a, T> {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    iter: rocksdb::DBIteratorWithThreadMode<'a, DB>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> RocksTrieCursor<'_, T> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);
        read_opts.set_prefetch_size(1024 * 1024); // 1MB prefetch

        let iter = db.as_ref().iterator_cf_opt(&cf, read_opts, IteratorMode::Start);

        Ok(Self { db, cf, iter, _marker: std::marker::PhantomData })
    }
}

impl<T> TrieCursor for RocksTrieCursor<'_, T> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Convert Nibbles to bytes for RocksDB key
        let key_bytes = key.as_bytes();
        match self.iter.seek(key_bytes) {
            Some(Ok((k, v))) if k == key_bytes => {
                self.current_key = Some(key.clone());
                // Convert RocksDB value to BranchNodeCompact
                let node = BranchNodeCompact::decode(&v)?;
                Ok(Some((key, node)))
            }
            _ => {
                self.current_key = None;
                Ok(None)
            }
        }
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let key_bytes = key.as_bytes();
        match self.iter.seek(key_bytes) {
            Some(Ok((k, v))) => {
                let found_key = Nibbles::from_bytes(&k)?;
                self.current_key = Some(found_key.clone());
                let node = BranchNodeCompact::decode(&v)?;
                Ok(Some((found_key, node)))
            }
            _ => {
                self.current_key = None;
                Ok(None)
            }
        }
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        match self.iter.next() {
            Some(Ok((k, v))) => {
                let key = Nibbles::from_bytes(&k)?;
                self.current_key = Some(key.clone());
                let node = BranchNodeCompact::decode(&v)?;
                Ok(Some((key, node)))
            }
            _ => {
                self.current_key = None;
                Ok(None)
            }
        }
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.current_key.clone())
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
    type AccountTrieCursor = RocksTrieCursor<'static, AccountTrieTable>;
    type StorageTrieCursor = RocksTrieCursor<'static, StorageTrieTable>;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        let cf = self
            .db
            .cf_handle(AccountTrieTable::NAME)
            .ok_or_else(|| DatabaseError::Other("Account trie column family not found".into()))?;
        RocksTrieCursor::new(self.db.clone(), cf.clone())
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor, DatabaseError> {
        let cf = self
            .db
            .cf_handle(StorageTrieTable::NAME)
            .ok_or_else(|| DatabaseError::Other("Storage trie column family not found".into()))?;

        // You might want to store the hashed_address in the cursor or use it to set initial position
        let mut cursor = RocksTrieCursor::new(self.db.clone(), cf.clone())?;

        // Optionally seek to the hashed_address position
        // cursor.seek_to_address(hashed_address)?;

        Ok(cursor)
    }
}
