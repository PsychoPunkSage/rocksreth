use std::fmt;
use thiserror::Error;

/// RocksDB specific errors
#[derive(Error, Debug)]
pub enum RocksDBError {
    /// Error from RocksDB itself
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),

    /// Error with column family operations
    #[error("Column family error: {0}")]
    ColumnFamily(String),

    /// Error during table operation
    #[error("Table operation error: {name} - {operation}")]
    TableOperation { name: String, operation: String },

    /// Error during encoding/decoding
    #[error("Codec error: {0}")]
    Codec(String),

    /// Error during migration
    #[error("Migration error: {0}")]
    Migration(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    Transaction(String),

    /// Invalid configuration
    #[error("Configuration error: {0}")]
    Config(String),
}

/// Maps RocksDB errors to DatabaseError
impl From<RocksDBError> for reth_db_api::DatabaseError {
    fn from(error: RocksDBError) -> Self {
        match error {
            RocksDBError::RocksDB(e) => Self::Other(format!("RocksDB error: {}", e)),
            RocksDBError::ColumnFamily(msg) => Self::Other(msg),
            RocksDBError::TableOperation { name, operation } => {
                Self::Other(format!("Table operation failed: {} - {}", name, operation))
            }
            RocksDBError::Codec(msg) => Self::Decode,
            RocksDBError::Migration(msg) => Self::Other(msg),
            RocksDBError::Transaction(msg) => Self::Other(format!("Transaction error: {}", msg)),
            RocksDBError::Config(msg) => Self::Other(msg),
        }
    }
}
