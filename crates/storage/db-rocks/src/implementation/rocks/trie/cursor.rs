use crate::tables::trie::{AccountTrieTable, StorageTrieTable, TrieTable};
use crate::RocksTransaction;
use alloy_primitives::B256;
use reth_db::cursor;
use reth_db::transaction::DbTx;
use reth_db_api::{cursor::DbCursorRO, DatabaseError};
use reth_db_api::{Decode, Encode};
use reth_primitives::Account;
use reth_trie::{
    hashed_cursor::{HashedCursor, HashedCursorFactory},
    trie_cursor::{TrieCursor, TrieCursorFactory},
};
use reth_trie::{BranchNodeCompact, Nibbles}; // For encoding/decoding
use reth_trie_common::{StoredNibbles, StoredNibblesSubKey};
use reth_trie_db::trie_cursor::TrieCursor;
use rocksdb::{ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
use std::sync::Arc;

/// RocksDB implementation of account trie cursor
pub struct RocksAccountTrieCursor<'tx> {
    /// Transaction reference
    tx: &'tx RocksTransaction<false>,
    /// Current cursor position
    current_key: Option<Nibbles>,
}

/// RocksDB implementation of storage trie cursor
pub struct RocksStorageTrieCursor<'tx> {
    /// Transaction reference
    tx: &'tx RocksTransaction<false>,
    /// Account hash for storage trie
    hashed_address: B256,
    /// Current cursor position
    current_key: Option<Nibbles>,
}

impl<'tx> RocksAccountTrieCursor<'tx> {
    pub fn new(tx: &'tx RocksTransaction<false>) -> Self {
        Self { tx, current_key: None }
    }
}

impl<'tx> RocksStorageTrieCursor<'tx> {
    pub fn new(tx: &'tx RocksTransaction<false>, hashed_address: B256) -> Self {
        Self { tx, hashed_address, current_key: None }
    }
}

impl<'tx> TrieCursor for RocksAccountTrieCursor<'tx> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // create cursor via txn
        let mut cursor = self.tx.cursor_read::<AccountTrieTable>()?;

        // Use seek_extract with StoredNibble
        let res = cursor.seek_exact(StoredNibbles(key.clone()))?.map(|val| (val.0 .0, val.1))?;

        if let Some((found_key, _)) = &res {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(res)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create cursor from txn
        let mut cursor = self.tx.cursor_read::<AccountTrieTable>()?;

        // Use seek with StoredNibbles
        let res = cursor.seek(StoredNibbles(key))?.map(|val| (val.0 .0, val.1));

        if let Some((found_key, _)) = &result {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(result)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create cursor from txn
        let mut cursor = self.tx.cursor_read::<AccountTrieTable>()?;

        // if have current key ? Position cursor
        if let Some(current) = &self.current_key {
            if let Some(_) = cursor.seek(StoredNibbles(current.clone()))? {
                // Move to next entry after current
                cursor.next()?
            } else {
                // Current key not found, start from beginning
                cursor.first()?
            }
        } else {
            // No current position, start from beginning
            cursor.first()?
        };

        // Get current entry after positioning
        let res = cursor.current()?.map(|val| (val.0 .0, val.1))?;

        if let Some((found_key, _)) = &res {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(res)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.current_key)
    }
}

impl<'tx> TrieCursor for RocksStorageTrieCursor<'tx> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let mut cursor = self.tx.cursor_dup_read::<StorageTrieTable>()?;

        // Convert to the correct type
        let subkey = StoredNibblesSubKey(key.clone());

        // Use seek_by_key_subkey
        if let Some(entry) = cursor.seek_by_key_subkey(self.hashed_address, subkey)? {
            self.current_key = Some(key.clone());
            return Ok(Some((key, entry.node)));
        }

        self.current_key = None;
        Ok(None)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create cursor via transaction
        let mut cursor = self.tx.cursor_dup_read::<StorageTrieTable>()?;

        // Use seek_by_key_subkey with hashed_address and StoredNibblesSubKey
        let result = cursor
            .seek_by_key_subkey(self.hashed_address, StoredNibblesSubKey(key))?
            .map(|value| (value.nibbles.0, value.node));

        if let Some((found_key, _)) = &result {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(result)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create cursor via transaction
        let mut cursor = self.tx.cursor_dup_read::<StorageTrieTable>()?;

        // Position cursor if we have a current key
        if let Some(current) = &self.current_key {
            cursor.seek_by_key_subkey(self.hashed_address, StoredNibblesSubKey(current.clone()))?;
        } else {
            cursor.seek_by_key(self.hashed_address)?;
        }

        // Move to next entry with same key (duplicate)
        let result = cursor.next_dup()?.map(|(_, v)| (v.nibbles.0, v.node));

        if let Some((found_key, _)) = &result {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(result)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.current_key.clone())
    }
}

/// Factory for creating trie cursors
pub struct RocksTrieCursorFactory {
    /// Transaction reference - provides context for all created cursors
    tx: &'tx RocksTransaction<false>,
}

impl<'tx> RocksTrieCursorFactory<'tx> {
    /// Create a new factory
    pub fn new(tx: &'tx RocksTransaction<false>) -> Self {
        Self { tx }
    }
}

impl TrieCursorFactory for RocksTrieCursorFactory {
    type AccountTrieCursor = RocksTrieCursor<'tx, AccountTrieTable>;
    type StorageTrieCursor = RocksTrieCursor<'tx, StorageTrieTable>;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        Ok(RocksTrieCursor::new(self.tx))
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor, DatabaseError> {
        // Convert hashed_address to bytes to use as prefix
        let prefix = hashed_address.as_bytes().to_vec();

        // Create cursor with the address prefix
        Ok(RocksTrieCursor::new(self.tx).with_prefix(Some(prefix)))
    }
}
