use crate::tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles, TrieNodeValue};
use crate::RocksTransaction;
use alloy_primitives::B256;
use reth_db::transaction::DbTx;
use reth_db_api::{cursor::DbCursorRO, DatabaseError};
use reth_trie::trie_cursor::{TrieCursor, TrieCursorFactory};
use reth_trie::{BranchNodeCompact, Nibbles, TrieMask}; // For encoding/decoding

/// RocksDB implementation of account trie cursor
pub struct RocksAccountTrieCursor<'tx> {
    /// Transaction reference
    tx: &'tx RocksTransaction<false>,
    /// Current cursor position
    current_key: Option<Nibbles>,
}
/// RocksDB implementation of storage trie cursor
pub struct RocksStorageTrieCursor<'tx> {
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
    pub fn new(
        // cursor: Box<dyn DbCursorRO<StorageTrieTable> + Send + Sync + 'tx>,
        tx: &'tx RocksTransaction<false>,
        hashed_address: B256,
    ) -> Self {
        Self { tx, hashed_address, current_key: None }
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
        let mut cursor = self.tx.cursor_read::<StorageTrieTable>()?;

        if let Some((addr, value)) = cursor.seek_exact(self.hashed_address)? {
            // Get first entry
            if addr == self.hashed_address {
                // Check if this entry has the right nibbles
                if value.nibbles.0 == key {
                    self.current_key = Some(key.clone());
                    return Ok(Some((key, Self::value_to_branch_node(value)?)));
                }

                // Scan for next entries with same account hash
                let mut next_entry = cursor.next()?;
                while let Some((next_addr, next_value)) = next_entry {
                    if next_addr != self.hashed_address {
                        break;
                    }

                    if next_value.nibbles.0 == key {
                        self.current_key = Some(key.clone());
                        return Ok(Some((key, Self::value_to_branch_node(next_value)?)));
                    }

                    next_entry = cursor.next()?;
                }
            }
        }

        self.current_key = None;
        Ok(None)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let mut cursor = self.tx.cursor_read::<StorageTrieTable>()?;

        if let Some((addr, value)) = cursor.seek_exact(self.hashed_address)? {
            // Check first entry
            if addr == self.hashed_address {
                if value.nibbles.0 >= key {
                    let found_nibbles = value.nibbles.0.clone();
                    self.current_key = Some(found_nibbles.clone());
                    return Ok(Some((found_nibbles, Self::value_to_branch_node(value)?)));
                }

                // Scan for next entries with same account hash
                let mut next_entry = cursor.next()?;
                while let Some((next_addr, next_value)) = next_entry {
                    if next_addr != self.hashed_address {
                        break;
                    }

                    if next_value.nibbles.0 >= key {
                        let found_nibbles = next_value.nibbles.0.clone();
                        self.current_key = Some(found_nibbles.clone());
                        return Ok(Some((found_nibbles, Self::value_to_branch_node(next_value)?)));
                    }

                    next_entry = cursor.next()?;
                }
            }
        }

        self.current_key = None;
        Ok(None)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        if let Some(current_key) = &self.current_key {
            let mut cursor = self.tx.cursor_read::<StorageTrieTable>()?;

            // Find current position
            if let Some((addr, value)) = cursor.seek_exact(self.hashed_address)? {
                if addr == self.hashed_address {
                    // Check if this is our current entry
                    if value.nibbles.0 == *current_key {
                        // Move to next entry
                        if let Some((next_addr, next_value)) = cursor.next()? {
                            if next_addr == self.hashed_address {
                                let next_nibbles = next_value.nibbles.0.clone();
                                self.current_key = Some(next_nibbles.clone());
                                return Ok(Some((
                                    next_nibbles,
                                    Self::value_to_branch_node(next_value)?,
                                )));
                            }
                        }
                    } else {
                        // Scan for our current position
                        let mut next_entry = cursor.next()?;
                        while let Some((next_addr, next_value)) = next_entry {
                            if next_addr != self.hashed_address {
                                break;
                            }

                            if next_value.nibbles.0 == *current_key {
                                // Found our current position, now get the next one
                                if let Some((next_next_addr, next_next_value)) = cursor.next()? {
                                    if next_next_addr == self.hashed_address {
                                        let next_nibbles = next_next_value.nibbles.0.clone();
                                        self.current_key = Some(next_nibbles.clone());
                                        return Ok(Some((
                                            next_nibbles,
                                            Self::value_to_branch_node(next_next_value)?,
                                        )));
                                    }
                                }
                                break;
                            }

                            next_entry = cursor.next()?;
                        }
                    }
                }
            }
        } else {
            // No current position, return first entry
            let mut cursor = self.tx.cursor_read::<StorageTrieTable>()?;
            if let Some((addr, value)) = cursor.seek_exact(self.hashed_address)? {
                if addr == self.hashed_address {
                    let nibbles = value.nibbles.0.clone();
                    self.current_key = Some(nibbles.clone());
                    return Ok(Some((nibbles, Self::value_to_branch_node(value)?)));
                }
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
        Ok(RocksStorageTrieCursor::new(self.tx, hashed_address))
    }
}
