use super::cursor::{ThreadSafeRocksCursor, ThreadSafeRocksDupCursor};
use super::trie::RocksHashedCursorFactory;
use crate::implementation::rocks::cursor::{RocksCursor, RocksDupCursor};
use crate::implementation::rocks::trie::RocksTrieCursorFactory;
use reth_db_api::table::TableImporter;
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO},
    table::{Compress, Decode, Decompress, DupSort, Encode, Table},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use rocksdb::{ColumnFamily, ReadOptions, WriteBatch, WriteOptions, DB};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

pub type CFPtr = *const ColumnFamily;

/// Generic transaction type for RocksDB
pub struct RocksTransaction<const WRITE: bool> {
    /// Reference to DB
    db: Arc<DB>,
    /// Write batch for mutations (only used in write transactions)
    batch: Option<Mutex<WriteBatch>>,
    /// Read options
    read_opts: ReadOptions,
    /// Write options
    write_opts: WriteOptions,
    /// Marker for transaction type
    _marker: PhantomData<bool>,
}

impl<const WRITE: bool> std::fmt::Debug for RocksTransaction<WRITE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksTransaction")
            .field("db", &self.db)
            .field("batch", &format!("<WriteOpts>"))
            .field("read_opts", &format!("<ReadOptions>"))
            .field("_marker", &self._marker)
            .finish()
    }
}

impl<const WRITE: bool> RocksTransaction<WRITE> {
    /// Create new transaction
    pub fn new(db: Arc<DB>, _write: bool) -> Self {
        let batch = if WRITE { Some(Mutex::new(WriteBatch::default())) } else { None };

        Self {
            db,
            batch,
            read_opts: ReadOptions::default(),
            write_opts: WriteOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Get the column family handle for a table
    fn get_cf<T: Table>(&self) -> Result<CFPtr, DatabaseError> {
        let table_name = T::NAME;

        // Try to get the column family
        match self.db.cf_handle(table_name) {
            Some(cf) => {
                // Convert the reference to a raw pointer
                // This is safe because the DB keeps CF alive as long as it exists
                let cf_ptr: CFPtr = cf as *const _;
                Ok(cf_ptr)
            }
            None => Err(DatabaseError::Other(format!("Column family not found: {}", table_name))),
        }
    }

    /// Create a trie cursor factory for this transaction
    #[allow(dead_code)]
    pub fn trie_cursor_factory(&self) -> RocksTrieCursorFactory<'_>
    where
        Self: Sized,
    {
        assert!(!WRITE, "trie_cursor_factory only works with read-only txn");
        // We need to create a read-only version to match the expected type
        let tx = Box::new(RocksTransaction::<false> {
            db: self.db.clone(),
            batch: None,
            read_opts: ReadOptions::default(),
            write_opts: WriteOptions::default(),
            _marker: PhantomData,
        });

        RocksTrieCursorFactory::new(Box::leak(tx))
    }

    pub fn hashed_cursor_factory(&self) -> RocksHashedCursorFactory<'_>
    where
        Self: Sized,
    {
        assert!(!WRITE, "hashed_cursor_factory only works with read-only txn");
        // We need to create a read-only version to match the expected type
        let tx = Box::new(RocksTransaction::<false> {
            db: self.db.clone(),
            batch: None,
            read_opts: ReadOptions::default(),
            write_opts: WriteOptions::default(),
            _marker: PhantomData,
        });
        RocksHashedCursorFactory::new(Box::leak(tx))
    }
}

// Implement read-only transaction
impl<const WRITE: bool> DbTx for RocksTransaction<WRITE> {
    type Cursor<T: Table> = ThreadSafeRocksCursor<T, WRITE>;
    type DupCursor<T: DupSort> = ThreadSafeRocksDupCursor<T, WRITE>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError>
    where
        T::Value: Decompress,
    {
        // Convert the raw pointer back to a reference safely
        // This is safe as long as the DB is alive, which it is in this context
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };

        let key_bytes = key.encode();

        match self
            .db
            .get_cf_opt(cf, key_bytes, &self.read_opts)
            .map_err(|e| DatabaseError::Other(format!("RocksDB Error: {}", e)))?
        {
            Some(value_bytes) => match T::Value::decompress(&value_bytes) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(e),
            },
            None => Ok(None),
        }
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError>
    where
        T::Value: Decompress,
    {
        // let cf = self.cf_to_arc_column_family(self.get_cf::<T>()?);
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };

        match self
            .db
            .get_cf_opt(cf, key, &self.read_opts)
            .map_err(|e| DatabaseError::Other(format!("RocksDB error: {}", e)))?
        {
            Some(value_bytes) => match T::Value::decompress(&value_bytes) {
                Ok(val) => Ok(Some(val)),
                Err(e) => Err(e),
            },
            None => Ok(None),
        }
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError>
    where
        T::Key: Encode + Decode + Clone,
    {
        let cf_ptr = self.get_cf::<T>()?;

        // Create a regular cursor first and handle the Result
        let inner_cursor = RocksCursor::new(self.db.clone(), cf_ptr)?;
        // Now wrap the successful cursor in the thread-safe wrapper
        Ok(ThreadSafeRocksCursor::new(inner_cursor))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError>
    where
        T::Key: Encode + Decode + Clone + PartialEq,
        T::SubKey: Encode + Decode + Clone,
    {
        let cf_ptr = self.get_cf::<T>()?;
        // Create a regular cursor first and handle the Result
        let inner_cursor = RocksDupCursor::new(self.db.clone(), cf_ptr)?;
        // Now wrap the successful cursor in the thread-safe wrapper
        Ok(ThreadSafeRocksDupCursor::new(inner_cursor))
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        if WRITE {
            if let Some(batch) = &self.batch {
                let mut batch_guard = match batch.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };

                // Create a new empty batch
                let empty_batch = WriteBatch::default();

                // Swap the empty batch with the current one to get ownership
                let real_batch = std::mem::replace(&mut *batch_guard, empty_batch);

                // Drop the guard before writing to avoid deadlocks
                drop(batch_guard);

                self.db.write_opt(real_batch, &self.write_opts).map_err(|e| {
                    DatabaseError::Other(format!("Failed to commit transaction: {}", e))
                })?;
            }
        }
        // For both read-only and write transactions after committing, just drop
        Ok(true)
    }

    fn abort(self) {
        // For read-only transactions, just drop
        // PPS:: Should we leave it as is??
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };
        let mut count = 0;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for _ in iter {
            count += 1;
        }
        Ok(count)
    }

    fn disable_long_read_transaction_safety(&mut self) {
        // No-op for RocksDB
    }
}

// Implement write transaction capabilities
impl DbTxMut for RocksTransaction<true> {
    type CursorMut<T: Table> = ThreadSafeRocksCursor<T, true>;
    type DupCursorMut<T: DupSort> = ThreadSafeRocksDupCursor<T, true>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError>
    where
        T::Value: Compress,
    {
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };

        if let Some(batch) = &self.batch {
            let mut batch_guard = match batch.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let key_bytes = key.encode();
            let value_bytes: Vec<u8> = value.compress().into();
            batch_guard.put_cf(cf, key_bytes, value_bytes);
        }
        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };

        if let Some(batch) = &self.batch {
            let mut batch_guard = match batch.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let key_bytes = key.encode();
            batch_guard.delete_cf(cf, key_bytes);
        }
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        let cf_ptr = self.get_cf::<T>()?;
        let cf = unsafe { &*cf_ptr };

        // Use a batch delete operation to clear all data in the column family
        if let Some(batch) = &self.batch {
            let mut batch_guard = match batch.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            // Delete all data in the column family using a range delete
            // These are the minimum and maximum possible key values
            let start_key = vec![0u8];
            let end_key = vec![255u8; 32]; // Adjust size if needed for your key format

            batch_guard.delete_range_cf(cf, start_key, end_key);
            return Ok(());
        }

        Err(DatabaseError::Other("Cannot clear column family without a write batch".to_string()))
        // Drop and recreate column family
        // self.db
        //     .drop_cf(cf_name)
        //     .map_err(|e| DatabaseError::Other(format!("Failed to drop Column family: {}", e)))?;
        // self.db
        //     .create_cf(cf_name, &Options::default())
        //     .map_err(|e| DatabaseError::Other(format!("Failed to create Column family: {}", e)))?;
        // Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError>
    where
        T::Key: Encode + Decode + Clone,
    {
        let cf_ptr = self.get_cf::<T>()?;
        // Create a regular cursor first and handle the Result
        let inner_cursor = RocksCursor::new(self.db.clone(), cf_ptr)?;
        // Now wrap the successful cursor in the thread-safe wrapper
        Ok(ThreadSafeRocksCursor::new(inner_cursor))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError>
    where
        T::Key: Encode + Decode + Clone + PartialEq,
        T::SubKey: Encode + Decode + Clone,
    {
        let cf_ptr = self.get_cf::<T>()?;
        // Create a regular cursor first and handle the Result
        let inner_cursor = RocksDupCursor::new(self.db.clone(), cf_ptr)?;
        // Now wrap the successful cursor in the thread-safe wrapper
        Ok(ThreadSafeRocksDupCursor::new(inner_cursor))
    }
}

impl TableImporter for RocksTransaction<true> {
    fn import_table<T: Table, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError>
    where
        T::Key: Encode + Decode + Clone,
        T::Value: Compress + Decompress,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let mut current = source_cursor.first()?;
        while let Some((key, value)) = current {
            destination_cursor.upsert(key, &value)?;
            current = source_cursor.next()?;
        }

        Ok(())
    }

    fn import_table_with_range<T: Table, R: DbTx>(
        &self,
        source_tx: &R,
        from: Option<<T as Table>::Key>,
        to: <T as Table>::Key,
    ) -> Result<(), DatabaseError>
    where
        T::Key: Default + Encode + Decode + Clone + PartialEq + Ord,
        T::Value: Compress + Decompress,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let mut current = match from {
            Some(from_key) => source_cursor.seek(from_key)?,
            None => source_cursor.first()?,
        };

        while let Some((key, value)) = current {
            if key > to {
                break;
            }

            destination_cursor.upsert(key, &value)?;
            current = source_cursor.next()?;
        }

        Ok(())
    }

    fn import_dupsort<T: DupSort, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError>
    where
        T::Key: Encode + Decode + Clone + PartialEq,
        T::Value: Compress + Decompress,
        T::SubKey: Encode + Decode + Clone,
    {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut source_cursor = source_tx.cursor_dup_read::<T>()?;

        let mut current = source_cursor.first()?;

        while let Some((key, value)) = current {
            // Use the DbCursorRW trait method, not a direct method on ThreadSafeRocksDupCursor
            DbCursorRW::upsert(&mut destination_cursor, key.clone(), &value)?;

            // Try to get next value with same key
            let next_with_same_key = source_cursor.next_dup()?;

            if next_with_same_key.is_some() {
                current = next_with_same_key;
            } else {
                // Move to next key group
                current = source_cursor.next_no_dup()?;
            }
        }

        Ok(())
    }
}
