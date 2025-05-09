use crate::RocksTransaction;
use alloy_primitives::StorageValue;
use alloy_primitives::B256;
use reth_db::transaction::DbTx;
use reth_db::DatabaseError;
use reth_db::HashedAccounts;
use reth_db::HashedStorages;
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO};
use reth_primitives::Account;
use reth_trie::hashed_cursor::HashedCursor;
use reth_trie::hashed_cursor::HashedCursorFactory;
use reth_trie::hashed_cursor::HashedStorageCursor;
use std::marker::PhantomData;

/// Factory for creating hashed cursors specific to RocksDB
#[derive(Clone, Debug)]
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

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor, DatabaseError> {
        let cursor = self.tx.cursor_read::<HashedAccounts>()?;
        Ok(RocksHashedAccountCursor { cursor, _phantom: PhantomData })
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor, DatabaseError> {
        let cursor = self.tx.cursor_read::<HashedStorages>()?;
        let dup_cursor = self.tx.cursor_dup_read::<HashedStorages>()?;
        Ok(RocksHashedStorageCursor { cursor, dup_cursor, hashed_address, _phantom: PhantomData })
    }
}

/// Implementation of HashedCursor for accounts
pub struct RocksHashedAccountCursor<'tx> {
    cursor: <RocksTransaction<false> as DbTx>::Cursor<HashedAccounts>,
    _phantom: PhantomData<&'tx ()>,
}

impl<'tx> HashedCursor for RocksHashedAccountCursor<'tx> {
    type Value = Account;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        println!("HashedAccountCursor: seeking key {:?}", key);
        let result = self.cursor.seek(key)?;

        match &result {
            Some((found_key, _)) => println!("HashedAccountCursor: found key {:?}", found_key),
            None => println!("HashedAccountCursor: key not found"),
        }

        Ok(result)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        // Log the current position for debugging
        let current = self.cursor.current()?;
        println!(
            "HashedAccountCursor: next() called, current position: {:?}",
            current.as_ref().map(|(key, _)| key)
        );

        println!("HashedAccountCursor: calling next() on underlying cursor");
        let result = self.cursor.next();

        match &result {
            Ok(Some((key, _))) => {
                println!("HashedAccountCursor: next() found entry with key {:?}", key)
            }
            Ok(None) => println!("HashedAccountCursor: no more entries"),
            Err(e) => println!("HashedAccountCursor: error in next(): {:?}", e),
        }

        println!("HashedAccountCursor: next() result: {:?}", result);
        result
    }
}

/// Implementation of HashedStorageCursor
pub struct RocksHashedStorageCursor<'tx> {
    cursor: <RocksTransaction<false> as DbTx>::Cursor<HashedStorages>,
    dup_cursor: <RocksTransaction<false> as DbTx>::DupCursor<HashedStorages>,
    hashed_address: B256,
    _phantom: PhantomData<&'tx ()>,
}

impl<'tx> HashedCursor for RocksHashedStorageCursor<'tx> {
    type Value = StorageValue;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        println!(
            "HashedStorageCursor: seeking slot {:?} for address {:?}",
            key, self.hashed_address
        );

        if let Some((found_address, _)) = self.cursor.seek_exact(self.hashed_address)? {
            if found_address == self.hashed_address {
                // We're using the appropriate address, now seek for the key
                if let Some(entry) = self.dup_cursor.seek_by_key_subkey(self.hashed_address, key)? {
                    println!("HashedStorageCursor: found slot {:?}", key);
                    return Ok(Some((key, entry.value)));
                }
            }
        }

        println!("HashedStorageCursor: no matching slot found");
        Ok(None)
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        println!("HashedStorageCursor: next() called for address {:?}", self.hashed_address);

        // Check if we have any values for this address
        if let Some((address, _)) = self.cursor.seek_exact(self.hashed_address)? {
            if address == self.hashed_address {
                // Use next_dup to get the next storage value for this address
                if let Some((_, entry)) = self.dup_cursor.next_dup()? {
                    // Extract the storage key and value from the entry
                    let storage_key = entry.key;
                    println!("HashedStorageCursor: next() found slot {:?}", storage_key);
                    return Ok(Some((storage_key, entry.value)));
                }
            }
        }

        println!("HashedStorageCursor: next() found no more entries");
        Ok(None)
    }
}

impl<'tx> HashedStorageCursor for RocksHashedStorageCursor<'tx> {
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        println!(
            "HashedStorageCursor: checking if storage is empty for address {:?}",
            self.hashed_address
        );

        // Check if there are any entries for this address
        let result = self.cursor.seek_exact(self.hashed_address)?.is_none();

        println!("HashedStorageCursor: storage is empty: {}", result);
        Ok(result)
    }
}
