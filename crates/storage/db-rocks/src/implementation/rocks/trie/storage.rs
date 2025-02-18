use crate::{
    implementation::rocks::RocksTransaction,
    tables::{AccountTrieTable, StorageTrieTable, TrieTable},
};
use reth_db_api::{transaction::DbTxMut, DatabaseError};
use reth_primitives::{Account, Address, H256};
use reth_trie_db::{DatabaseStateRoot, DatabaseStorageRoot, StateCommitment, TrieChanges};

/// Implementation of trie storage operations
impl<const WRITE: bool> RocksTransaction<WRITE> {
    /// Get a trie node by its hash
    pub fn get_node(&self, hash: H256) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.get::<TrieTable>(hash)
    }

    /// Get an account by its hash
    pub fn get_account(&self, hash: H256) -> Result<Option<Account>, DatabaseError> {
        self.get::<AccountTrieTable>(hash)
    }

    /// Get storage value for account and key
    pub fn get_storage(&self, account: H256, key: H256) -> Result<Option<H256>, DatabaseError> {
        self.get::<StorageTrieTable>((account, key))
    }
}

impl StateCommitment for RocksTransaction<true> {
    fn commit_changes(&mut self, changes: TrieChanges) -> Result<H256, DatabaseError> {
        // Write trie nodes
        for (hash, node) in changes.nodes {
            self.put::<TrieTable>(hash, node)?;
        }

        // Write account changes
        for (hash, account) in changes.accounts {
            self.put::<AccountTrieTable>(hash, account)?;
        }

        // Write storage changes
        for ((account, key), value) in changes.storage {
            self.put::<StorageTrieTable>((account, key), value)?;
        }

        Ok(changes.root)
    }
}

impl DatabaseStateRoot for RocksTransaction<false> {
    fn state_root(&self, block_hash: H256) -> Result<Option<H256>, DatabaseError> {
        self.get::<TrieTable>(block_hash)
    }
}

impl DatabaseStorageRoot for RocksTransaction<false> {
    fn storage_root(
        &self,
        address: Address,
        block_hash: H256,
    ) -> Result<Option<H256>, DatabaseError> {
        self.get::<StorageTrieTable>((address.into(), block_hash))
    }
}
