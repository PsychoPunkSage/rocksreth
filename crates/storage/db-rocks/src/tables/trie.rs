use super::codecs::trie::StorageTrieKey;
use crate::tables::codecs::trie::TrieNodeCodec;
use crate::tables::TableConfig;
use alloy_primitives::B256;
// use reth_db_api::table::DupSort;
use reth_db_api::table::{Decode, DupSort, Encode, Table};
use reth_primitives::Account;
use reth_trie::{BranchNodeCompact, Nibbles}; // For encoding/decoding
use reth_trie_common::{StoredNibbles, StoredNibblesSubKey};
use rocksdb::{ColumnFamilyDescriptor, Options};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

    type Key = TrieNibbles; // Changed from B256 to Nibbles
    type Value = BranchNodeCompact; // Changed from Account to BranchNodeCompact
}

/// Table storing storage trie nodes.
#[derive(Debug)]
pub struct StorageTrieTable;

impl Table for StorageTrieTable {
    const NAME: &'static str = "storage_trie";
    const DUPSORT: bool = true;

    type Key = B256; // (Account hash)
    type Value = TrieNodeValue;
}

// Define StorageTrieEntry
impl DupSort for StorageTrieTable {
    type SubKey = StoredNibbles;
}

/// Wrapper type for Nibbles that implements necessary database traits
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TrieNibbles(pub Nibbles);

impl Encode for TrieNibbles {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        // Convert Nibbles to bytes
        Vec::<u8>::from(self.0)
    }
}

impl Decode for TrieNibbles {
    fn decode(bytes: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        // Create Nibbles from bytes
        let byt = bytes.to_vec();
        // Check if all bytes are valid nibbles (0-15) before creating Nibbles
        if byt.iter().any(|&b| b > 0xf) {
            return Err(reth_db::DatabaseError::Decode);
        }

        // Since we've verified the bytes are valid, this won't panic
        let nibbles = Nibbles::from_nibbles(&bytes);
        Ok(TrieNibbles(nibbles))
    }
}

// Implement serde traits which are needed for the Key trait
impl serde::Serialize for TrieNibbles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as bytes
        let bytes: Vec<u8> = Vec::<u8>::from(self.0.clone());
        bytes.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for TrieNibbles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        // Check if all bytes are valid nibbles (0-15) before creating Nibbles
        if bytes.iter().any(|&b| b > 0xf) {
            return Err(serde::de::Error::custom("Invalid nibble value"));
        }

        // Since we've verified the bytes are valid, this won't panic
        let nibbles = Nibbles::from_nibbles(&bytes);
        Ok(TrieNibbles(nibbles))
    }
}

// Add conversion methods for convenience
impl From<Nibbles> for TrieNibbles {
    fn from(nibbles: Nibbles) -> Self {
        TrieNibbles(nibbles)
    }
}

impl From<TrieNibbles> for Nibbles {
    fn from(trie_nibbles: TrieNibbles) -> Self {
        trie_nibbles.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrieNodeValue {
    pub nibbles: StoredNibbles,
    pub node: B256, // Value hash
}

impl Encode for TrieNodeValue {
    type Encoded = Vec<u8>;

    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.nibbles.encode());
        bytes.extend_from_slice(self.node.as_slice());
        bytes
    }
}

impl Decode for TrieNodeValue {
    fn decode(bytes: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        if bytes.len() < 32 {
            return Err(reth_db_api::DatabaseError::Decode);
        }

        // Split bytes between nibbles part and value hash
        let (nibbles_bytes, value_bytes) = bytes.split_at(bytes.len() - 32);

        Ok(Self {
            nibbles: StoredNibbles::decode(nibbles_bytes)?,
            node: B256::from_slice(value_bytes),
        })
    }
}

impl reth_db_api::table::Compress for TrieNodeValue {
    type Compressed = Vec<u8>;

    fn compress(self) -> Vec<u8> {
        // WILL Properly implement later
        self.encode()
    }

    fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        todo!("Will do later")
    }
}

impl reth_db_api::table::Decompress for TrieNodeValue {
    fn decompress(bytes: &[u8]) -> Result<Self, reth_db_api::DatabaseError> {
        // Will implement it properly later
        Self::decode(bytes)
    }
}

impl Serialize for TrieNodeValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to a format that can be serialized
        // This is just an example - you'll need to adjust based on your types
        let bytes = self.clone().encode();
        bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TrieNodeValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::decode(&bytes).map_err(serde::de::Error::custom)
    }
}
