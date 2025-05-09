/*
RETH RocksDB Implementation Structure

>>> Root Files
- `Cargo.toml` - Package configuration, dependencies, and features for the RocksDB implementation
- `src/lib.rs` - Main library entry point, exports public API and manages module organization
- `src/db.rs` - Core database interface implementation and main DB struct definitions
- `src/errors.rs` - Custom error types and error handling for the RocksDB implementation
- `src/metrics.rs` - Performance metrics collection and monitoring infrastructure
- `src/version.rs` - Database versioning, schema migrations, and compatibility management

>>> Benchmarks
- `benches/criterion.rs` - Main benchmark configuration and setup for performance testing
- `benches/get.rs` - Specific benchmarks for database read operations and performance
- `benches/util.rs` - Shared utilities and helper functions for benchmarking

>>> Implementation Layer (`src/implementation/`)
#>> Core Implementation <<#
- `implementation/mod.rs` - Manages database implementation modules and common traits

#>> RocksDB Specific (`implementation/rocks/`) <<#
- `rocks/mod.rs` - Core RocksDB wrapper and primary database operations
- `rocks/cursor.rs` - Cursor implementations for iterating over RocksDB data
- `rocks/dupsort.rs` - Duplicate sort functionality for RocksDB
- `rocks/tx.rs` - Transaction management, batching, and ACID compliance

#>> Trie Implementation (`implementation/rocks/trie/`) <<#
- `trie/mod.rs` - Main trie functionality coordination
- `trie/cursor.rs` - Specialized cursors for trie traversal
- `trie/storage.rs` - Storage layer for trie data structures
- `trie/witness.rs` - Witness generation and verification for tries

>>> Tables Layer (`src/tables/`)
#>> Core Tables <<#
- `tables/mod.rs` - Table definitions, traits, and organization
- `tables/raw.rs` - Low-level table operations without encoding
- `tables/trie.rs` - Trie-specific table implementations
- `tables/utils.rs` - Helper functions for table management

#>> Codecs (`tables/codecs/`) <<#
- `codecs/mod.rs` - Codec management and common encoding traits
- `codecs/trie.rs` - Specialized codecs for trie data structures

>>> Tests (left)
- `test/mod.rs` - Test organization and shared test utilities
*/
//! RocksDB implementation for RETH
//!
//! This crate provides a RocksDB-backed implementation of the database interfaces defined in reth-db-api.

//! RocksDB implementation for RETH
//!
//! This crate provides a RocksDB-backed storage implementation for RETH.

#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![warn(missing_copy_implementations)]
#![warn(rust_2018_idioms)]

mod errors;
mod implementation;
mod tables;
mod test;

pub use errors::RocksDBError;
pub use implementation::rocks::trie::{calculate_state_root, calculate_state_root_with_updates};
pub use implementation::rocks::tx::RocksTransaction;
pub use reth_primitives_traits::Account;
pub use reth_trie::HashedPostState;
pub use test::utils;

// /*
// > This codebase implements a RocksDB storage layer for RETH. At its core, it provides a way to store and retrieve blockchain data using RocksDB instead of MDBX. The implementation handles database operations through tables (like accounts, transactions, etc.) where each table is a separate column family in RocksDB.
// > The cursor system lets you iterate through data in these tables, similar to how you'd scan through entries in a database. The DUPSORT feature (which MDBX has natively but RocksDB doesn't) is implemented manually to allow multiple values per key, which is crucial for certain blockchain data structures like state history.
// > All database operations are wrapped in transactions, either read-only or read-write, to maintain data consistency. The metrics module tracks performance and usage statistics, while the version management ensures proper database schema upgrades.
// > The codecs part handles how data is serialized and deserialized - converting Ethereum types (like addresses and transactions) into bytes for storage and back. Error handling is centralized to provide consistent error reporting across all database operations.
// > Think of it as a specialized database adapter that makes RocksDB behave exactly how RETH expects its storage layer to work, with all the specific features needed for an Ethereum client. It's basically translating RETH's storage requirements into RocksDB operations while maintaining all the necessary blockchain-specific functionality.
// */
