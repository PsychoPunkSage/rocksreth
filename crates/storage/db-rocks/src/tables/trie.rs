//! Table definitions for trie data.
use super::codecs::TrieNodeCodec;
use reth_db_api::table::{Table, TableConfig};
use reth_primitives::{Account, H256};
use rocksdb::{ColumnFamilyDescriptor, Options};

/// Table storing the trie nodes.
#[derive(Debug)]
pub struct TrieTable;

impl Table for TrieTable {
    const NAME: &'static str = "trie";

    type Key = H256; // Node hash
    type Value = Vec<u8>; // RLP encoded node data
    type Config = TrieTableConfigs;
}

/// Table storing account trie nodes.
#[derive(Debug)]
pub struct AccountTrieTable;

impl Table for AccountTrieTable {
    const NAME: &'static str = "account_trie";

    type Key = H256;
    type Value = Account;
    type Config = TrieTableConfigs;
}

/// Table storing storage trie nodes.
#[derive(Debug)]
pub struct StorageTrieTable;

impl Table for StorageTrieTable {
    const NAME: &'static str = "storage_trie";

    type Key = (H256, H256); // (Account Hash, Storage Hash)
    type Value = H256; // Storage Value
    type Config = TrieTableConfigs;
}

/// Defines configurations for trie tables.
#[derive(Debug, Default)]
pub struct TrieTableConfigs;

impl TableConfig for TrieTableConfigs {
    fn descriptor(&self) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        // Configure for trie workload
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_block_size(16 * 1024); // 16KB blocks
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB write buffer
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB target file size

        ColumnFamilyDescriptor::new(Self::NAME, opts)
    }
}
