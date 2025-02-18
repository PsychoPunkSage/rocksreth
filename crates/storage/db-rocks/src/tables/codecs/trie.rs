//! Codecs for trie data structures.
use reth_db_api::table::{Decode, Encode, TableCodec}; // PP:: TableCodec???
use reth_primitives::H256; // PP::H256??
use std::marker::PhantomData;

/// Codec for trie nodes
#[derive(Debug, Default)]
pub struct TrieNodeCodec<T>(PhantomData<T>);

impl<T> TableCodec for TrieNodeCodec<T> {
    type Key = H256;
    type Value = T;

    fn encode_key(key: &Self::Key) -> Vec<u8> {
        key.as_bytes().to_vec()
    }

    fn decode_key(key: &[u8]) -> Result<Self::Key, reth_db_api::DatabaseError> {
        Ok(H256::from_slice(key))
    }
}
