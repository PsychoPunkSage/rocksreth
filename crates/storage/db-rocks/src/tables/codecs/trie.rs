//! Codecs for trie data structures.
use alloy_primitives::B256;
use reth_db_api::table::{Decode, Encode};
use reth_db_api::DatabaseError;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageTrieKey {
    pub account_hash: B256,
    pub storage_hash: B256,
}

impl StorageTrieKey {
    pub fn new(account_hash: B256, storage_hash: B256) -> Self {
        Self { account_hash, storage_hash }
    }
}

impl Encode for StorageTrieKey {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(self.account_hash.as_slice());
        bytes.extend_from_slice(self.storage_hash.as_slice());
        bytes
    }
}

impl Decode for StorageTrieKey {
    fn decode(bytes: &[u8]) -> Result<Self, DatabaseError> {
        if bytes.len() != 64 {
            return Err(DatabaseError::Decode("Invalid length for StorageTrieKey".into()));
        }

        let account_hash = B256::from_slice(&bytes[0..32]);
        let storage_hash = B256::from_slice(&bytes[32..64]);

        Ok(Self::new(account_hash, storage_hash))
    }
}

impl From<(B256, B256)> for StorageTrieKey {
    fn from((account_hash, storage_hash): (B256, B256)) -> Self {
        Self::new(account_hash, storage_hash)
    }
}

impl From<StorageTrieKey> for (B256, B256) {
    fn from(key: StorageTrieKey) -> Self {
        (key.account_hash, key.storage_hash)
    }
}

// Keep existing TrieNodeCodec implementation...
#[derive(Debug, Default)]
pub struct TrieNodeCodec<T>(PhantomData<T>);

impl<T: Encode + Decode> Encode for TrieNodeCodec<T> {
    type Encoded = Vec<u8>;

    fn encode(self) -> Self::Encoded {
        todo!("Implement encoding logic")
    }
}

impl<T: Encode + Decode> Decode for TrieNodeCodec<T> {
    fn decode(value: &[u8]) -> Result<Self, DatabaseError> {
        todo!("Implement decoding logic")
    }
}
