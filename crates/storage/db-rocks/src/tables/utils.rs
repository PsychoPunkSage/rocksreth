use reth_db_api::{table::Table, DatabaseError};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;

/// Utility functions for table management
pub(crate) struct TableUtils;

impl TableUtils {
    /// List all column families in the database
    pub fn list_cf(path: &Path) -> Result<Vec<String>, DatabaseError> {
        let cfs = DB::list_cf(&Options::default(), path)
            .map_err(|e| DatabaseError::Other(format!("Failed to list column families: {}", e)))?;
        Ok(cfs)
    }

    /// Get all table names that should exist in the database
    pub fn get_expected_table_names() -> Vec<String> {
        use reth_db::Tables;
        Tables::ALL.iter().map(|t| t.name().to_string()).collect()
    }

    /// Get column family options for a specific table
    pub fn get_cf_options<T: Table>() -> Options {
        let mut opts = Options::default();

        // Set common options
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB

        // Special handling for DUPSORT tables
        if T::DUPSORT {
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(32));
            opts.set_memtable_prefix_bloom_ratio(0.1);
        }

        opts
    }

    /// Create column family descriptors for tables that exist in the database
    pub fn get_existing_cf_descriptors(
        path: &Path,
    ) -> Result<Vec<ColumnFamilyDescriptor>, DatabaseError> {
        let existing = Self::list_cf(path)?;

        Ok(existing
            .into_iter()
            .map(|name| {
                let opts = Options::default();
                ColumnFamilyDescriptor::new(name, opts)
            })
            .collect())
    }

    /// Check if a database exists and has the correct tables
    pub fn validate_database(path: &Path) -> Result<bool, DatabaseError> {
        if !path.exists() {
            return Ok(false);
        }

        let existing = Self::list_cf(path)?;
        let expected = Self::get_expected_table_names();

        Ok(existing.iter().all(|cf| expected.contains(cf)))
    }
}
