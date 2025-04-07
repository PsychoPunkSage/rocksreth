use crate::RocksTransaction;
use alloy_primitives::StorageValue;
use alloy_primitives::B256;
use reth_db::transaction::DbTx;
use reth_db::HashedAccounts;
use reth_db::HashedStorages;
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_trie::hashed_cursor::HashedCursor;
use reth_trie::hashed_cursor::HashedCursorFactory;
use reth_trie::hashed_cursor::HashedStorageCursor;
use std::marker::PhantomData;

/// RocksDB implementation of HashedCursorFactory
pub struct RocksHashedCursorFactory<'tx> {
    tx: &'tx RocksTransaction<false>,
}

impl<'tx> RocksHashedCursorFactory<'tx> {
    pub fn new(tx: &'tx RocksTransaction<false>) -> Self {
        Self { tx }
    }
}

impl<'tx> HashedCursorFactory for RocksHashedCursorFactory<'tx> {
    type AccountCursor = RocksHashedAccountCursor<'tx>;
    type StorageCursor = RocksHashedStorageCursor<'tx>;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, reth_db::DatabaseError> {
        let cursor = self.tx.cursor_read::<HashedAccounts>()?;
        Ok(RocksHashedAccountCursor { cursor, _phantom: PhantomData })
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: alloy_primitives::B256,
    ) -> Result<Self::StorageCursor, reth_db::DatabaseError> {
        let cursor = self.tx.cursor_read::<HashedStorages>()?;
        let dup_cursor = self.tx.cursor_dup_read::<HashedStorages>()?;
        Ok(RocksHashedStorageCursor { cursor, dup_cursor, hashed_address, _phantom: PhantomData })
    }
}

// Basic implementation of HashedAccountCursor

pub struct RocksHashedAccountCursor<'tx> {
    cursor: <RocksTransaction<false> as DbTx>::Cursor<HashedAccounts>,
    _phantom: PhantomData<&'tx ()>,
}

impl<'tx> HashedCursor for RocksHashedAccountCursor<'tx> {
    type Value = reth_primitives::Account;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        self.cursor.seek(key)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        self.cursor.next()
    }
}

// Basic implementation of HashedStorageCursor

pub struct RocksHashedStorageCursor<'tx> {
    cursor: <RocksTransaction<false> as DbTx>::Cursor<HashedStorages>,
    dup_cursor: <RocksTransaction<false> as DbTx>::DupCursor<HashedStorages>,
    hashed_address: B256,
    _phantom: PhantomData<&'tx ()>,
}

impl<'tx> HashedStorageCursor for RocksHashedStorageCursor<'tx> {
    fn is_storage_empty(&mut self) -> Result<bool, reth_db::DatabaseError> {
        Ok(self.cursor.seek_exact(self.hashed_address)?.is_none())
    }
}

impl<'tx> HashedCursor for RocksHashedStorageCursor<'tx> {
    type Value = StorageValue;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        if let Some((found_address, _)) =
            DbCursorRO::seek_exact(&mut self.cursor, self.hashed_address)?
        {
            if found_address == self.hashed_address {
                // We're using the appropriate address, now seek for the key
                if let Some(entry) = DbDupCursorRO::seek_by_key_subkey(
                    &mut self.dup_cursor,
                    self.hashed_address,
                    key,
                )? {
                    // Convert StorageEntry to StorageValue
                    return Ok(Some((key, StorageValue::from(entry.value))));
                }
            }
        }
        Ok(None)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, reth_db::DatabaseError> {
        // Check if we have any values for this address
        if let Some((address, _)) = DbCursorRO::seek_exact(&mut self.cursor, self.hashed_address)? {
            if address == self.hashed_address {
                // Use next_dup to get the next storage value for this address
                if let Some((key, value)) = DbDupCursorRO::next_dup(&mut self.dup_cursor)? {
                    // Extract the storage key from the value
                    // This depends on how your storage values are structured
                    // For simplicity, we're just using a default value here
                    let storage_key = key;
                    return Ok(Some((storage_key, StorageValue::from(value.value))));
                }
            }
        }
        Ok(None)
    }
}
