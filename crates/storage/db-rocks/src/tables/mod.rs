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
        use reth_db_api::Tables;

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

// src/version.rs

use reth_db_api::{table::Table, DatabaseError};
use rocksdb::{Options, DB};
use std::sync::atomic::{AtomicU32, Ordering};

/// Current database schema version
const CURRENT_VERSION: u32 = 1;
/// Version key used in RocksDB
const VERSION_KEY: &[u8] = b"db_version";
/// Default column family name
const DEFAULT_CF: &str = "default";

/// Database version management
#[derive(Debug)]
pub struct VersionManager {
    /// Current version
    version: AtomicU32,
}

impl VersionManager {
    /// Create new version manager
    pub fn new(db: &DB) -> Result<Self, DatabaseError> {
        // Try to read existing version
        let version = match db
            .get_cf(&*db.cf_handle(DEFAULT_CF).expect("Default CF always exists"), VERSION_KEY)?
        {
            Some(bytes) => {
                let ver = u32::from_be_bytes(
                    bytes
                        .try_into()
                        .map_err(|_| DatabaseError::Other("Invalid version format".to_string()))?,
                );
                ver
            }
            None => {
                // No version found, initialize with current version
                let version = CURRENT_VERSION;
                db.put_cf(
                    &*db.cf_handle(DEFAULT_CF).expect("Default CF always exists"),
                    VERSION_KEY,
                    &version.to_be_bytes(),
                )?;
                version
            }
        };

        Ok(Self { version: AtomicU32::new(version) })
    }

    /// Get current database version
    pub fn current_version(&self) -> u32 {
        self.version.load(Ordering::Relaxed)
    }

    /// Check if database needs migration
    pub fn needs_migration(&self) -> bool {
        self.current_version() < CURRENT_VERSION
    }

    /// Run necessary migrations
    pub fn migrate(&self, db: &DB) -> Result<(), DatabaseError> {
        let current = self.current_version();
        if current >= CURRENT_VERSION {
            return Ok(());
        }

        // Run migrations in sequence
        for version in current + 1..=CURRENT_VERSION {
            self.run_migration(version, db)?;

            // Update version after successful migration
            db.put_cf(
                &*db.cf_handle(DEFAULT_CF).expect("Default CF always exists"),
                VERSION_KEY,
                &version.to_be_bytes(),
            )?;
            self.version.store(version, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Run specific version migration
    fn run_migration(&self, version: u32, db: &DB) -> Result<(), DatabaseError> {
        match version {
            1 => {
                // Initial version - no migration needed
                Ok(())
            }
            // Add more version migrations here
            _ => Err(DatabaseError::Other(format!("Unknown version: {}", version))),
        }
    }
}

/// Migration utilities
pub(crate) struct MigrationUtils;

impl MigrationUtils {
    /// Recreate column family with new options
    pub fn recreate_column_family(
        db: &DB,
        cf_name: &str,
        new_opts: &Options,
    ) -> Result<(), DatabaseError> {
        // Drop existing CF
        db.drop_cf(cf_name)?;

        // Create new CF with updated options
        db.create_cf(cf_name, new_opts)?;

        Ok(())
    }

    /// Copy data between column families
    pub fn copy_cf_data(db: &DB, source_cf: &str, target_cf: &str) -> Result<(), DatabaseError> {
        let source = db
            .cf_handle(source_cf)
            .ok_or_else(|| DatabaseError::Other(format!("Source CF not found: {}", source_cf)))?;
        let target = db
            .cf_handle(target_cf)
            .ok_or_else(|| DatabaseError::Other(format!("Target CF not found: {}", target_cf)))?;

        let mut batch = rocksdb::WriteBatch::default();
        let iter = db.iterator_cf(&source, rocksdb::IteratorMode::Start);

        for result in iter {
            let (key, value) = result?;
            batch.put_cf(&target, key, value);
        }

        db.write(batch)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_version_management() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, temp_dir.path())?;
        let version_manager = VersionManager::new(&db)?;

        assert_eq!(version_manager.current_version(), CURRENT_VERSION);
        assert!(!version_manager.needs_migration());

        Ok(())
    }

    #[test]
    fn test_migration() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, temp_dir.path())?;

        // Manually set old version
        db.put_cf(&*db.cf_handle(DEFAULT_CF).unwrap(), VERSION_KEY, &0u32.to_be_bytes())?;

        let version_manager = VersionManager::new(&db)?;
        assert!(version_manager.needs_migration());

        version_manager.migrate(&db)?;
        assert_eq!(version_manager.current_version(), CURRENT_VERSION);

        Ok(())
    }
}
