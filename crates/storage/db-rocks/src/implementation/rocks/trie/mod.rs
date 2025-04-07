mod cursor;
mod hashed_cursor;
mod storage;
mod witness;

pub use cursor::*;
pub use hashed_cursor::*;
pub use storage::*;

use alloy_primitives::B256;
use reth_db_api::DatabaseError;
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseHashedStorageCursor};

/// Common trait for RocksDB trie cursors
pub trait RocksTrieCursorOps {
    /// Seek to an exact position in the trie
    fn seek_exact(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    /// Move to the next entry
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, DatabaseError>;

    /// Get the current key
    fn current(&self) -> Option<(&[u8], &[u8])>;
}
