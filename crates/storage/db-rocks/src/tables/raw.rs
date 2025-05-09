use rocksdb::{DBIterator, IteratorMode, DB};
use std::sync::Arc;

/// Raw table access wrapper
pub(crate) struct RawTable<'a> {
    db: Arc<DB>,
    cf_handle: &'a rocksdb::ColumnFamily,
}

impl<'a> RawTable<'a> {
    /// Create new raw table accessor
    pub(crate) fn new(db: Arc<DB>, cf_handle: &'a rocksdb::ColumnFamily) -> Self {
        Self { db, cf_handle }
    }

    /// Get raw value
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.db.get_cf(self.cf_handle, key)
    }

    /// Put raw value
    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.put_cf(self.cf_handle, key, value)
    }

    /// Delete raw value
    pub(crate) fn delete(&self, key: &[u8]) -> Result<(), rocksdb::Error> {
        self.db.delete_cf(self.cf_handle, key)
    }

    /// Create iterator over raw values
    pub(crate) fn iterator(&self, mode: IteratorMode) -> DBIterator {
        self.db.iterator_cf(self.cf_handle, mode)
    }
}
