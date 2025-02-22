pub mod codecs;
pub mod raw;
pub mod trie;
pub mod utils;

use reth_db_api::table::Table;
use reth_db_api::DatabaseError;
use rocksdb::{ColumnFamilyDescriptor, Options};

// mod trie;
// pub use trie::*;

/// Trait for getting RocksDB-specific table configurations
pub(crate) trait TableConfig: Table {
    /// Get column family options for this table
    fn column_family_options() -> Options {
        let mut opts = Options::default();

        // Set basic options that apply to all tables
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        // If table is DUPSORT, we need to configure prefix extractor
        if Self::DUPSORT {
            // Configure prefix scanning for DUPSORT tables
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
        }

        opts
    }

    /// Get column family descriptor for this table
    fn descriptor() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(Self::NAME, Self::column_family_options())
    }
}

// Implement TableConfig for all Tables
impl<T: Table> TableConfig for T {}

/// Utility functions for managing tables in RocksDB
pub(crate) struct TableManagement;

impl TableManagement {
    /// Create all column families for given database
    pub fn create_column_families(db: &rocksdb::DB, tables: &[&str]) -> Result<(), DatabaseError> {
        for table in tables {
            if !db.cf_handle(table).is_some() {
                db.create_cf(table, &Options::default()).map_err(|e| {
                    DatabaseError::Other(format!("Failed to create column family: {}", e))
                })?;
            }
        }
        Ok(())
    }

    /// Get all column family descriptors for all tables
    pub fn get_all_column_family_descriptors() -> Vec<ColumnFamilyDescriptor> {
        // WHAT IS TABLES/TABLE????
        use reth_db::Tables;
        Tables::ALL
            .iter()
            .map(|table| {
                let mut opts = Options::default();

                // Configure options based on table type
                if table.is_dupsort() {
                    opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
                }

                ColumnFamilyDescriptor::new(table.name(), opts)
            })
            .collect()
    }
}
