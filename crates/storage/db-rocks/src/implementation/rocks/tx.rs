use crate::implementation::rocks::cursor::{RocksCursor, RocksDupCursor};
use crate::implementation::rocks::trie::RocksTrieCursorFactory;
use parking_lot::Mutex;
use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    table::{Compress, Decode, Decompress, DupSort, Encode, Table},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
// use reth_trie_db::{hashed_cursor, trie_cursor};
use reth_db_api::table::TableImporter;
use rocksdb::{ColumnFamilyDescriptor, Options, ReadOptions, WriteBatch, WriteOptions, DB};
use std::marker::PhantomData;
use std::sync::Arc;

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
    /// Trie cursor factory
    // trie_cursor_factory: RocksTrieCursorFactory,
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
    pub(crate) fn new(db: Arc<DB>, _write: bool) -> Self {
        let batch = if WRITE { Some(Mutex::new(WriteBatch::default())) } else { None };
        // let trie_cursor_factory = RocksTrieCursorFactory::new(db.clone());

        Self {
            db,
            batch,
            read_opts: ReadOptions::default(),
            write_opts: WriteOptions::default(),
            // trie_cursor_factory,
            _marker: PhantomData,
        }
    }

    /// Get the column family handle for a table
    fn get_cf<T: Table>(&self) -> Result<Arc<rocksdb::ColumnFamily>, DatabaseError> {
        self.db
            .cf_handle(T::NAME)
            .ok_or_else(|| DatabaseError::Other(format!("Column family not found: {}", T::NAME)))
    }

    /// Create a trie cursor factory for this transaction
    pub fn trie_cursor_factory(&self) -> RocksTrieCursorFactory
    where
        Self: Sized,
    {
        assert!(!WRITE, "trie_Cursor_factory only works with read-only txn");
        RocksTrieCursorFactory::new(self)
    }
}

// Implement read-only transaction
impl<const WRITE: bool> DbTx for RocksTransaction<WRITE> {
    type Cursor<T: Table> = super::cursor::RocksCursor<T, WRITE>;
    type DupCursor<T: DupSort> = super::cursor::RocksDupCursor<T, WRITE>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let cf = self.get_cf::<T>()?;
        let key_bytes = key.encode();

        match self
            .db
            .get_cf_opt(&*cf, key_bytes, &self.read_opts)
            .map_err(|e| DatabaseError::Other(format!("RocksDB Error: {}", e)))?
        {
            Some(value_bytes) => match T::Value::decode(&value_bytes) {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(e),
            },
            None => Ok(None),
        }
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as reth_db_api::table::Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let cf = self.get_cf::<T>()?;

        match self
            .db
            .get_cf_opt(&*cf, key, &self.read_opts)
            .map_err(|e| DatabaseError::Other(format!("RocksDB error: {}", e)))?
        {
            Some(value_bytes) => match T::Value::decode(&value_bytes) {
                Ok(val) => Ok(Some(val)),
                Err(e) => Err(e),
            },
            None => Ok(None),
        }
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        super::cursor::RocksCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
        // *** cloen needed??
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        super::cursor::RocksDupCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
        // *** clone needed??
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        // For read-only transactions, just drop
        Ok(true)
    }

    fn abort(self) {
        // For read-only transactions, just drop
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        let cf = self.get_cf::<T>()?;
        let mut count = 0;
        let iter = self.db.iterator_cf(&*cf, rocksdb::IteratorMode::Start);
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
    type CursorMut<T: Table> = RocksCursor<T, true>;
    type DupCursorMut<T: DupSort> = RocksDupCursor<T, true>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let cf = self.get_cf::<T>()?;
        if let Some(batch) = &self.batch {
            let mut batch = batch.lock();
            let key_bytes = key.encode();
            let value_bytes = value.compress();
            batch.put_cf(&cf, key_bytes, value_bytes);
        }
        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let cf = self.get_cf::<T>()?;
        if let Some(batch) = &self.batch {
            let mut batch = batch.lock();
            let key_bytes = key.encode();
            batch.delete_cf(&*cf, key_bytes);
        }
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        let cf = self.get_cf::<T>()?;
        // Drop and recreate column family
        self.db
            .drop_cf(T::NAME)
            .map_err(|e| DatabaseError::Other(format!("Failed to drop Column family: {}", e)))?;
        self.db
            .create_cf(T::NAME, &Options::default())
            .map_err(|e| DatabaseError::Other(format!("Failed to create Column family: {}", e)))?;
        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        // super::cursor::RocksCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
        RocksCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        // super::cursor::RocksDupCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
        RocksDupCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
    }
}

impl TableImporter for RocksTransaction<true> {
    fn import_table<T: Table, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_write::<T>()?;

        for kv in source_tx.cursor_read::<T>()?.walk(None)? {
            let (k, v) = kv?;
            destination_cursor.append(k, &v)?;
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
        T::Key: Default,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let source_range = match from {
            Some(from) => source_cursor.walk_range(from..=to),
            None => source_cursor.walk_range(..=to),
        };
        for row in source_range? {
            let (key, value) = row?;
            destination_cursor.append(key, &value)?;
        }

        Ok(())
    }

    fn import_dupsort<T: DupSort, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut cursor = source_tx.cursor_dup_read::<T>()?;

        while let Some((k, _)) = cursor.next_no_dup()? {
            for kv in cursor.walk_dup(Some(k), None)? {
                let (k, v) = kv?;
                destination_cursor.append_dup(k, v)?;
            }
        }

        Ok(())
    }
}
