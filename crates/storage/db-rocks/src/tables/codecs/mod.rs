use bytes::{BufMut, BytesMut};
use reth_db_api::{
    table::{Compress, Decode, Decompress, Encode},
    DatabaseError,
};

/// Trait for RocksDB-specific encoding optimizations
pub(crate) trait RocksDbEncode: Encode {
    /// Encode directly to a bytes buffer
    fn encode_to_buf(&self, buf: &mut BytesMut);
}

/// Trait for RocksDB-specific decoding optimizations
pub(crate) trait RocksDbDecode: Decode {
    /// Decode from a bytes slice without copying
    fn decode_from_slice(slice: &[u8]) -> Result<Self, DatabaseError>;
}

// Implement encoding helpers
impl<T: Encode> RocksDbEncode for T {
    fn encode_to_buf(&self, buf: &mut BytesMut) {
        let encoded = self.encode();
        buf.put_slice(encoded.as_ref());
    }
}

// Implement decoding helpers
impl<T: Decode> RocksDbDecode for T {
    fn decode_from_slice(slice: &[u8]) -> Result<Self, DatabaseError> {
        Self::decode(slice)
    }
}

/// Compression utilities for RocksDB values
pub(crate) mod compression {
    use super::*;

    pub fn compress_to_buf<T: Compress>(value: &T, buf: &mut BytesMut) {
        if let Some(uncompressed) = value.uncompressable_ref() {
            buf.put_slice(uncompressed);
        } else {
            value.compress_to_buf(buf);
        }
    }

    pub fn decompress<T: Decompress>(value: &[u8]) -> Result<T, DatabaseError> {
        T::decompress(value)
    }
}
