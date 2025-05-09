use crate::{
    implementation::rocks::tx::RocksTransaction,
    tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles, TrieNodeValue, TrieTable},
};
use alloy_primitives::{keccak256, Address, B256};
use eyre::Ok;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    transaction::DbTx,
    DatabaseError,
};
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory, trie_cursor::InMemoryTrieCursorFactory,
    updates::TrieUpdates, BranchNodeCompact, HashedPostState, KeccakKeyHasher, StateRoot,
    StateRootProgress, StorageRoot, StoredNibbles, TrieInput,
};
#[cfg(feature = "metrics")]
use reth_trie::{metrics::TrieRootMetrics, TrieType};
use reth_trie_db::{
    DatabaseHashedCursorFactory, DatabaseStateRoot, DatabaseStorageRoot, DatabaseTrieCursorFactory,
    PrefixSetLoader,
};

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
        key: StoredNibbles,
    ) -> Result<Option<TrieNodeValue>, DatabaseError> {
        // Create a cursor for the StorageTrieTable
        let mut cursor = self.cursor_dup_read::<StorageTrieTable>()?;

        // First seek to the account hash
        if let Some((found_account, _)) = cursor.seek(account)? {
            // If we found the account, check if it's the one we're looking for
            if found_account == account {
                // Now seek to the specific storage key (which is the subkey)
                return cursor
                    .seek_by_key_subkey(account, key)?
                    .map(|value| Ok(Some(value)))
                    .unwrap_or(Ok(None))
                    .map_err(|e| DatabaseError::Other(format!("ErrReport: {:?}", e)));
            }
        }

        // Account not found or no matching storage key
        Ok(None).map_err(|e| DatabaseError::Other(format!("ErrReport: {:?}", e)))
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
        let loaded_prefix_set = PrefixSetLoader::<_, KeccakKeyHasher>::new(tx).load(range)?;

        // Create StateRoot calculator with txn and prefix-sets
        let calculator = StateRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            DatabaseHashedCursorFactory::new(tx),
        )
        .with_prefix_sets(loaded_prefix_set);

        calculator.root_with_progress()
    }

    fn overlay_root(
        tx: &'a RocksTransaction<false>,
        post_state: HashedPostState,
    ) -> Result<B256, reth_execution_errors::StateRootError> {
        let prefix_sets = post_state.construct_prefix_sets().freeze();

        let state_sorted = post_state.into_sorted();

        // Create StateRoot calculator with txn and prefix-sets
        StateRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
        )
        .with_prefix_sets(prefix_sets)
        .root()
    }

    fn overlay_root_with_updates(
        tx: &'a RocksTransaction<false>,
        post_state: HashedPostState,
    ) -> Result<(B256, TrieUpdates), reth_execution_errors::StateRootError> {
        let prefix_sets = post_state.construct_prefix_sets().freeze();

        let state_sorted = post_state.into_sorted();

        // Create StateRoot calculator with txn and prefix-sets
        StateRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
        )
        .with_prefix_sets(prefix_sets)
        .root_with_updates()
    }

    fn overlay_root_from_nodes(
        tx: &'a RocksTransaction<false>,
        input: TrieInput,
    ) -> Result<B256, reth_execution_errors::StateRootError> {
        let state_sorted = input.state.into_sorted();
        let nodes_sorted = input.nodes.into_sorted();

        // Create a StateRoot calculator with the transaction, in-memory nodes, post state, and prefix sets
        StateRoot::new(
            InMemoryTrieCursorFactory::new(DatabaseTrieCursorFactory::new(tx), &nodes_sorted),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
        )
        .with_prefix_sets(input.prefix_sets.freeze())
        .root()
    }

    fn overlay_root_from_nodes_with_updates(
        tx: &'a RocksTransaction<false>,
        input: TrieInput,
    ) -> Result<(B256, TrieUpdates), reth_execution_errors::StateRootError> {
        let state_sorted = input.state.into_sorted();
        let nodes_sorted = input.nodes.into_sorted();

        StateRoot::new(
            InMemoryTrieCursorFactory::new(DatabaseTrieCursorFactory::new(tx), &nodes_sorted),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
        )
        .with_prefix_sets(input.prefix_sets.freeze())
        .root_with_updates()
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
        let hashed_address = keccak256(address);

        let prefix_set = hashed_storage.construct_prefix_set().freeze();

        let state_sorted =
            HashedPostState::from_hashed_storage(hashed_address, hashed_storage).into_sorted();

        StorageRoot::new(
            DatabaseTrieCursorFactory::new(tx),
            HashedPostStateCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &state_sorted),
            address,
            prefix_set,
            #[cfg(feature = "metrics")]
            TrieRootMetrics::new(TrieType::Storage),
        )
        .root()
    }
}
