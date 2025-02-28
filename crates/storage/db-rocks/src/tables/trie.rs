use super::codecs::trie::StorageTrieKey;
use crate::tables::codecs::trie::TrieNodeCodec;
use crate::tables::TableConfig;
use alloy_primitives::B256;
// use reth_db_api::table::DupSort;
use reth_db_api::table::Table;
use reth_primitives::Account;
use reth_trie::{BranchNodeCompact, Nibbles}; // For encoding/decoding
use reth_trie_common::{StoredNibbles, StoredNibblesSubKey};
use rocksdb::{ColumnFamilyDescriptor, Options};

/// Table storing the trie nodes.
#[derive(Debug)]
pub struct TrieTable;

impl Table for TrieTable {
    const NAME: &'static str = "trie";
    const DUPSORT: bool = false;

    type Key = B256; // Node hash
    type Value = Vec<u8>; // RLP encoded node data
}

/// Table storing account trie nodes.
#[derive(Debug)]
pub struct AccountTrieTable;

impl Table for AccountTrieTable {
    const NAME: &'static str = "account_trie";
    const DUPSORT: bool = false;

    type Key = B256;
    type Value = Account;
}

/// Table storing storage trie nodes.
#[derive(Debug)]
pub struct StorageTrieTable;

impl Table for StorageTrieTable {
    const NAME: &'static str = "storage_trie";
    const DUPSORT: bool = true;

    // type Value = B256; // Storage Value
    // type Key = StorageTrieKey; // (Account Hash, Storage Hash)

    type Key = B256; // (Account Hash, Storage Hash)
    type Value = StorageTrieEntry; // Storage Value
}

// Define StorageTrieEntry
#[derive(Debug, Clone)]
pub struct StorageTrieEntry {
    pub nibbles: StoredNibblesSubKey,
    pub node: BranchNodeCompact,
}

impl DupSort for StorageTrieTable {
    type SubKey = StoredNibblesSubKey;

    fn compose_key(key: &B256, _subkey: &StoredNibblesSubKey) -> Self::Key {
        *key // Now this works because Key is B256
    }
}
