use super::super::dupsort::DupSortHelper;
use crate::tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles, TrieNodeValue};
use crate::RocksTransaction;
use alloy_primitives::{FixedBytes, B256};
use reth_db::transaction::DbTx;
use reth_db_api::{cursor::DbCursorRO, DatabaseError};
use reth_trie::trie_cursor::{TrieCursor, TrieCursorFactory};
use reth_trie::{BranchNodeCompact, Nibbles, TrieMask}; // For encoding/decoding
use reth_trie_common::StoredNibbles;

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
    // *** [Temporary SOln] Need to see How DbCursorRO in rest of the REPO
    // *** cursor: RocksDupCursor<StorageTrieTable, false>,  // Or whatever the concrete type is
    cursor: Box<dyn DbCursorRO<StorageTrieTable> + Send + Sync + 'tx>, // *** [Temporary SOln] Need to see How DbCursorRO in rest of the REPO
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
    pub fn new(
        cursor: Box<dyn DbCursorRO<StorageTrieTable> + Send + Sync + 'tx>,
        hashed_address: B256,
    ) -> Self {
        Self { cursor, hashed_address, current_key: None }
    }

    // Helper method to convert TrieNodeValue to BranchNodeCompact :::> BETTER TO HAVE IT REMOVED
    fn value_to_branch_node(value: TrieNodeValue) -> Result<BranchNodeCompact, DatabaseError> {
        // Placeholder implementation - need to implement this based on your specific data model
        // This might involve RLP decoding or other transformations
        // let branch_node = BranchNodeCompact::from_hash(value.node);
        // Ok(branch_node)
        let state_mask = TrieMask::new(0);
        let tree_mask = TrieMask::new(0);
        let hash_mask = TrieMask::new(0);

        // No hashes in this minimal representation
        let hashes = Vec::new();

        // Use the node hash from the value as the root hash
        let root_hash = Some(value.node);

        // Create a new BranchNodeCompact with these values
        let branch_node =
            BranchNodeCompact::new(state_mask, tree_mask, hash_mask, hashes, root_hash);

        Ok(branch_node)
    }
}

impl<'tx> TrieCursor for RocksAccountTrieCursor<'tx> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // create cursor via txn
        let mut cursor = self.tx.cursor_read::<AccountTrieTable>()?;

        let res = cursor.seek_exact(TrieNibbles(key.clone()))?.map(|val| (val.0 .0, val.1));

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
        let res = cursor.seek(TrieNibbles(key))?.map(|val| (val.0 .0, val.1));

        if let Some((found_key, _)) = &res {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(res)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create cursor from txn
        let mut cursor = self.tx.cursor_read::<AccountTrieTable>()?;

        // if have current key ? Position cursor
        if let Some(current) = &self.current_key {
            if let Some(_) = cursor.seek(TrieNibbles(current.clone()))? {
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
        let res = cursor.current()?.map(|val| (val.0 .0, val.1));

        if let Some((found_key, _)) = &res {
            self.current_key = Some(found_key.clone());
        } else {
            self.current_key = None;
        }

        Ok(res)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.current_key.clone())
    }
}

impl<'tx> TrieCursor for RocksStorageTrieCursor<'tx> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create subkey from nibbles
        let subkey = StoredNibbles(key.clone());

        // Create composite key using DupSortHelper
        let composite_key_vec =
            DupSortHelper::create_composite_key::<StorageTrieTable>(&self.hashed_address, &subkey)?;

        let composite_key =
            FixedBytes::<32>::try_from(composite_key_vec.as_slice()).map_err(|_| {
                DatabaseError::Other(format!("Failed to convert Vec<u8> to FixedBytes<32>"))
            })?;

        // Use seek_exact with the composite key
        if let Some((_, value)) = self.cursor.seek_exact(composite_key)? {
            let nibbles = value.nibbles.0.clone();
            self.current_key = Some(nibbles.clone());
            return Ok(Some((nibbles, Self::value_to_branch_node(value)?)));
        }

        self.current_key = None;
        Ok(None)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create prefix for scanning all entries with this account hash
        let prefix_vec = DupSortHelper::create_prefix::<StorageTrieTable>(&self.hashed_address)?;

        let prefix = FixedBytes::<32>::try_from(prefix_vec.as_slice()).map_err(|_| {
            DatabaseError::Other(format!("Failed to convert Vec<u8> to FixedBytes<32>"))
        })?;

        // Seek to the first entry with this prefix
        if let Some((_, value)) = self.cursor.seek(prefix)? {
            // Check if the found key has a prefix matching our search key
            let found_key = &value.nibbles.0;
            if found_key.has_prefix(&key) {
                self.current_key = Some(found_key.clone());
                return Ok(Some((found_key.clone(), Self::value_to_branch_node(value)?)));
            }
        }

        self.current_key = None;
        Ok(None)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        // Create prefix for scanning all entries with this account hash
        let prefix = DupSortHelper::create_prefix::<StorageTrieTable>(&self.hashed_address)?;

        // Move to the next entry
        if let Some((composite_key, value)) = self.cursor.next()? {
            // Check if the key still has our prefix
            if composite_key.starts_with(&prefix) {
                self.current_key = Some(value.nibbles.0.clone());
                return Ok(Some((value.clone().nibbles.0, Self::value_to_branch_node(value)?)));
            }
        }

        self.current_key = None;
        Ok(None)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.current_key.clone())
    }
}

/// Factory for creating trie cursors
#[derive(Clone)]
pub struct RocksTrieCursorFactory<'tx> {
    /// Transaction reference - provides context for all created cursors
    tx: &'tx RocksTransaction<false>,
}

impl<'tx> RocksTrieCursorFactory<'tx> {
    /// Create a new factory
    pub fn new(tx: &'tx RocksTransaction<false>) -> Self {
        Self { tx }
    }
}

impl<'tx> TrieCursorFactory for RocksTrieCursorFactory<'tx> {
    type AccountTrieCursor = RocksAccountTrieCursor<'tx>;
    type StorageTrieCursor = RocksStorageTrieCursor<'tx>; // *** Need internal lifetime managers

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor, DatabaseError> {
        Ok(RocksAccountTrieCursor::new(self.tx))
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor, DatabaseError> {
        // Convert hashed_address to bytes to use as prefix
        let cursor = self.tx.cursor_read::<StorageTrieTable>()?;
        let boxed_cursor: Box<dyn DbCursorRO<StorageTrieTable> + Send + Sync> = Box::new(cursor);

        // Create cursor with the address prefix
        Ok(RocksStorageTrieCursor::new(boxed_cursor, hashed_address))
    }
}
