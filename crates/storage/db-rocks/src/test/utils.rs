use crate::{
    calculate_state_root_with_updates,
    tables::trie::{AccountTrieTable, StorageTrieTable, TrieNodeValue, TrieTable},
    Account, HashedPostState, RocksTransaction,
};
use alloy_primitives::{keccak256, Address, B256, U256};
use reth_db::{HashedAccounts, HashedStorages};
use reth_db_api::table::Table;
use reth_trie::{BranchNodeCompact, Nibbles, StoredNibbles, TrieMask};
use rocksdb::{Options, DB};
use std::sync::Arc;
use tempfile::TempDir;

pub fn create_test_db() -> (Arc<DB>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    // create options
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Define column families
    let cf_names = vec![
        TrieTable::NAME,
        AccountTrieTable::NAME,
        StorageTrieTable::NAME,
        HashedAccounts::NAME,
        HashedStorages::NAME,
    ];

    // create column family descriptor
    let cf_descriptors = cf_names
        .iter()
        .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect::<Vec<_>>();

    // Open the Database with column families
    let db = DB::open_cf_descriptors(&opts, path, cf_descriptors).unwrap();

    (Arc::new(db), temp_dir)
}

pub fn setup_test_state(
    read_tx: &RocksTransaction<false>,
    write_tx: &RocksTransaction<true>,
) -> (B256, Address, Address, B256) {
    // Create test Accounts
    let address1 = Address::from([1; 20]);
    let hashed_address1 = keccak256(address1);
    let address2 = Address::from([2; 20]);
    let hashed_address2 = keccak256(address2);

    let account1 = Account {
        nonce: 1,
        balance: U256::from(1000),
        bytecode_hash: Some(B256::from([0x11; 32])),
    };

    let account2 = Account {
        nonce: 5,
        balance: U256::from(5000),
        bytecode_hash: Some(B256::from([0x22; 32])),
    };

    let storage_key = B256::from([0x33; 32]);
    let storage_value = U256::from(42);

    let mut post_state = HashedPostState::default();
    post_state.accounts.insert(hashed_address1, Some(account1));
    post_state.accounts.insert(hashed_address2, Some(account2));

    let mut storage = reth_trie::HashedStorage::default();
    storage.storage.insert(storage_key, storage_value);
    post_state.storages.insert(hashed_address1, storage);

    // Calculate state root and commit trie
    let state_root = calculate_state_root_with_updates(read_tx, write_tx, post_state).unwrap();

    (state_root, address1, address2, storage_key)
}

fn create_trie_node_value(nibbles_str: &str, node_hash: B256) -> TrieNodeValue {
    let nibbles = Nibbles::from_nibbles(
        &nibbles_str.chars().map(|c| c.to_digit(16).unwrap() as u8).collect::<Vec<_>>(),
    );

    TrieNodeValue { nibbles: StoredNibbles(nibbles), node: node_hash }
}

pub fn create_test_branch_node() -> BranchNodeCompact {
    let state_mask = TrieMask::new(0);
    let tree_mask = TrieMask::new(0);
    let hash_mask = TrieMask::new(0);
    let hashes = Vec::new();
    let root_hash = Some(B256::from([1; 32]));

    BranchNodeCompact::new(state_mask, tree_mask, hash_mask, hashes, root_hash)
}
