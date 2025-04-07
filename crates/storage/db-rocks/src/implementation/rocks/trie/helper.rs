use crate::implementation::rocks::tx::RocksTransaction;
use alloy_primitives::B256;
use reth_execution_errors::StateRootError;
use reth_trie::hashed_cursor::HashedPostStateCursorFactory;
use reth_trie::HashedPostState;
use reth_trie::StateRoot;

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
