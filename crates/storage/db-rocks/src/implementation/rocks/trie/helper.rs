use crate::{
    implementation::rocks::tx::RocksTransaction,
    tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles},
};
use alloy_primitives::B256;
use reth_db_api::transaction::DbTxMut;
use reth_execution_errors::StateRootError;
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory, updates::TrieUpdates, HashedPostState, StateRoot,
};

/// Helper function to calculate state root directly from post state
pub fn calculate_state_root(
    tx: &RocksTransaction<false>,
    post_state: HashedPostState,
) -> Result<B256, StateRootError> {
    let prefix_sets = post_state.construct_prefix_sets().freeze();
    let state_sorted = post_state.into_sorted();

    let calculator = StateRoot::new(
        tx.trie_cursor_factory(),
        HashedPostStateCursorFactory::new(tx.hashed_cursor_factory(), &state_sorted),
    )
    .with_prefix_sets(prefix_sets);

    calculator.root()
}

/// Calculate state root from post state and store all trie nodes
pub fn calculate_state_root_with_updates(
    read_tx: &RocksTransaction<false>,
    write_tx: &RocksTransaction<true>,
    post_state: HashedPostState,
) -> Result<B256, StateRootError> {
    let prefix_sets = post_state.construct_prefix_sets().freeze();
    let state_sorted = post_state.into_sorted();

    // Calculate the root and get all the updates (nodes)
    let (root, updates) = StateRoot::new(
        read_tx.trie_cursor_factory(),
        HashedPostStateCursorFactory::new(read_tx.hashed_cursor_factory(), &state_sorted),
    )
    .with_prefix_sets(prefix_sets)
    .root_with_updates()?;

    // Store all the trie nodes
    commit_trie_updates(write_tx, updates)?;

    Ok(root)
}

/// Stores all trie nodes in the database
fn commit_trie_updates(
    tx: &RocksTransaction<true>,
    updates: TrieUpdates,
) -> Result<(), StateRootError> {
    // Store all account trie nodes
    for (hash, node) in updates.account_nodes {
        tx.put::<AccountTrieTable>(TrieNibbles(hash), node)
            .map_err(|e| StateRootError::Database(e))?;
    }

    // // Store all storage trie nodes
    // for (_, storage_updates) in updates.storage_tries {
    //     for (hash, node_rlp) in storage_updates.storage_nodes {
    //         tx.put::<StorageTrieTable>(hash, node_rlp).map_err(|e| StateRootError::Database(e))?;
    //     }
    // }

    Ok(())
}

/// For convenience, calculate and commit in one operation with a single transaction
pub fn calculate_and_commit_state_root(
    tx: &mut RocksTransaction<true>,
    post_state: HashedPostState,
) -> Result<B256, StateRootError> {
    // Create a read-only view for calculating the state
    let read_tx = RocksTransaction::<false>::new(tx.get_db_clone(), false);

    // Calculate root and get updates
    let prefix_sets = post_state.construct_prefix_sets().freeze();
    let state_sorted = post_state.into_sorted();

    let (root, updates) = StateRoot::new(
        read_tx.trie_cursor_factory(),
        HashedPostStateCursorFactory::new(read_tx.hashed_cursor_factory(), &state_sorted),
    )
    .with_prefix_sets(prefix_sets)
    .root_with_updates()?;

    // Store all the trie nodes
    commit_trie_updates(tx, updates)?;

    Ok(root)
}
