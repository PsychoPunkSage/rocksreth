use crate::{
    implementation::rocks::tx::RocksTransaction,
    tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles, TrieNodeValue, TrieTable},
};
use alloy_primitives::{Address, B256};
use eyre::Ok;
use reth_db_api::transaction::DbTx;
use reth_db_api::{transaction::DbTxMut, DatabaseError};
use reth_execution_errors::StateRootError;
use reth_primitives::Account;
use reth_trie::{
    updates::TrieUpdates, BranchNodeCompact, HashedPostState, KeccakKeyHasher, StateRoot,
    StateRootProgress, TrieInput,
};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStateRoot, DatabaseStorageRoot, DatabaseTrieCursorFactory,
    PrefixSetLoader, StateCommitment,
};
use std::fmt::format;

/// Implementation of trie storage operations
impl<const WRITE: bool> RocksTransaction<WRITE> {
    /// Get a trie node by its hash
    pub fn get_node(&self, hash: B256) -> Result<Option<Vec<u8>>, DatabaseError> {
        self.get::<TrieTable>(hash)
    }

    /// Get an account by its hash
    pub fn get_account(
        &self,
        hash: TrieNibbles,
    ) -> Result<Option<BranchNodeCompact>, DatabaseError> {
        self.get::<AccountTrieTable>(hash)
    }

    /// Get storage value for account and key
    pub fn get_storage(
        &self,
        account: B256,
        key: B256,
    ) -> Result<Option<TrieNodeValue>, DatabaseError> {
        // self.get::<StorageTrieTable>((account, key))
        /*
                error[E0308]: mismatched types
          --> crates/storage/db-rocks/src/implementation/rocks/trie/storage.rs:37:38
           |
        37 |         self.get::<StorageTrieTable>((account, key))
           |              ----------------------- ^^^^^^^^^^^^^^ expected `FixedBytes<32>`, found `(FixedBytes<32>, ...)`
           |              |
           |              arguments to this method are incorrect
           |
           = note: expected struct `FixedBytes<32>`
                       found tuple `(FixedBytes<32>, FixedBytes<32>)`
        note: method defined here
          --> /home/psychopunk_sage/dev/Workplace/Supra/reth/crates/storage/db-api/src/transaction.rs:15:8
           |
        15 |     fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, Databas...
           |        ^^^
        */
        todo!("Implement get storage")
    }
}

impl<'a> DatabaseStateRoot<'a, RocksTransaction<false>> for &'a RocksTransaction<false> {
    fn from_tx(tx: &'a RocksTransaction<false>) -> Self {
        tx
    }

    fn incremental_root_calculator(
        tx: &'a RocksTransaction<false>,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<Self, reth_execution_errors::StateRootError> {
        Ok(tx).map_err(|e| {
            reth_execution_errors::StateRootError::Database(DatabaseError::Other(format!(
                "ErrReport: {:?}",
                e
            )))
        })
    }

    fn incremental_root(
        tx: &'a RocksTransaction<false>,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<B256, reth_execution_errors::StateRootError> {
        // Create a StateRoot calculator with txn + load the prefix sets for the range.
        let loaded_prefix_sets = PrefixSetLoader::<_, KeccakKeyHasher>::new(tx).load(range)?;

        // Create a stateroot calculator with the txn and prefix sets
        let calculator = StateRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            DatabaseHashedCursorFactory::new(tx), // maybe I have to implement DatabaseHashedCursorFactory
        )
        .with_prefix_sets(loaded_prefix_sets);

        calculator.root()
    }

    fn incremental_root_with_updates(
        tx: &'a RocksTransaction<false>,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<(B256, TrieUpdates), reth_execution_errors::StateRootError> {
        // Computes root and collects updates
        let loaded_prefix_sets = PrefixSetLoader::<_, KeccakKeyHasher>::new(tx).load(range)?;

        // Create StateRoot calculator with txn and prefix-sets
        let calculator = StateRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            DatabaseHashedCursorFactory::new(tx),
        )
        .with_prefix_sets(loaded_prefix_sets);

        calculator.root_with_updates()
    }

    fn incremental_root_with_progress(
        tx: &'a RocksTransaction<false>,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<StateRootProgress, reth_execution_errors::StateRootError> {
        // Computes root with progress tracking
        todo!("Implement incremental root with progress")
    }

    fn overlay_root(
        tx: &'a RocksTransaction<false>,
        post_state: HashedPostState,
    ) -> Result<B256, reth_execution_errors::StateRootError> {
        // Calculate root from post state
        todo!("Implement overlay root")
    }

    fn overlay_root_with_updates(
        tx: &'a RocksTransaction<false>,
        post_state: HashedPostState,
    ) -> Result<(B256, TrieUpdates), reth_execution_errors::StateRootError> {
        // Calculate root and collect updates
        todo!("Implement overlay root with updates")
    }

    fn overlay_root_from_nodes(
        tx: &'a RocksTransaction<false>,
        input: TrieInput,
    ) -> Result<B256, reth_execution_errors::StateRootError> {
        // Calculate root using provided nodes
        todo!("Implement overlay root from nodes")
    }

    fn overlay_root_from_nodes_with_updates(
        tx: &'a RocksTransaction<false>,
        input: TrieInput,
    ) -> Result<(B256, TrieUpdates), reth_execution_errors::StateRootError> {
        // Calculate root and collect updates using provided nodes
        todo!("Implement overlay root from nodes with updates")
    }
}

impl<'a> DatabaseStorageRoot<'a, RocksTransaction<false>> for &'a RocksTransaction<false> {
    fn from_tx(tx: &'a RocksTransaction<false>, address: Address) -> Self {
        tx
    }

    fn from_tx_hashed(tx: &'a RocksTransaction<false>, hashed_address: B256) -> Self {
        tx
    }

    fn overlay_root(
        tx: &'a RocksTransaction<false>,
        address: Address,
        hashed_storage: reth_trie::HashedStorage,
    ) -> Result<B256, reth_execution_errors::StorageRootError> {
        // Implement storage root calculation
        todo!()
    }
}
