use alloy_primitives::B256;
use bytes::{BufMut, BytesMut};
use reth_db_api::table::Decode;
use reth_db_api::{
    table::{DupSort, Encode},
    DatabaseError,
};

/// Delimiter used to separate key and subkey in DUPSORT tables
const DELIMITER: u8 = 0xFF;

/// Helper functions for DUPSORT implementation in RocksDB
pub(crate) struct DupSortHelper;

impl DupSortHelper {
    /// Create a composite key from key and subkey for DUPSORT tables
    pub(crate) fn create_composite_key<T: DupSort>(
        key: &T::Key,
        subkey: &T::SubKey,
    ) -> Result<Vec<u8>, DatabaseError> {
        let mut bytes = BytesMut::new();

        // Encode main key
        let key_bytes = key.clone().encode();
        bytes.put_slice(key_bytes.as_ref());

        // Add delimiter
        bytes.put_u8(DELIMITER);

        // Encode subkey
        let subkey_bytes = subkey.clone().encode();
        bytes.put_slice(subkey_bytes.as_ref());

        Ok(bytes.to_vec())
    }

    /// Extract key and subkey from composite key
    pub(crate) fn split_composite_key<T: DupSort>(
        composite: &[u8],
    ) -> Result<(T::Key, T::SubKey), DatabaseError> {
        if let Some(pos) = composite.iter().position(|&b| b == DELIMITER) {
            let (key_bytes, subkey_bytes) = composite.split_at(pos);
            // Skip delimiter
            let subkey_bytes = &subkey_bytes[1..];

            Ok((T::Key::decode(key_bytes)?, T::SubKey::decode(subkey_bytes)?))
        } else {
            Err(DatabaseError::Decode)
        }
    }

    /// Create prefix for scanning all subkeys of a key
    pub(crate) fn create_prefix<T: DupSort>(key: &T::Key) -> Result<Vec<u8>, DatabaseError> {
        let mut bytes = BytesMut::new();
        let key_bytes = key.clone().encode();
        bytes.put_slice(key_bytes.as_ref());
        bytes.put_u8(DELIMITER);
        Ok(bytes.to_vec())
    }

    pub(crate) fn encode_composite_key<T: DupSort>(
        composite_key_vec: Vec<u8>,
    ) -> Result<T::Key, DatabaseError>
    where
        T::Key: Decode,
    {
        match T::Key::decode(&composite_key_vec) {
            Ok(key) => Ok(key),
            Err(_) => {
                // If standard decoding fails, try alternative approach
                if composite_key_vec.len() >= 32 {
                    // Take first 32 bytes for B256
                    let mut buffer = [0u8; 32];
                    buffer.copy_from_slice(&composite_key_vec[0..32]);

                    // Try to decode as B256 first
                    match B256::decode(&buffer) {
                        Ok(b256) => {
                            // Re-encode the B256 to get bytes
                            let encoded_bytes = b256.encode();

                            // Now try to decode those bytes as T::Key
                            match T::Key::decode(encoded_bytes.as_ref()) {
                                Ok(key) => Ok(key),
                                Err(_) => Err(DatabaseError::Decode),
                            }
                        }
                        Err(_) => Err(DatabaseError::Decode),
                    }
                } else {
                    Err(DatabaseError::Decode)
                }
            }
        }
    }
}
