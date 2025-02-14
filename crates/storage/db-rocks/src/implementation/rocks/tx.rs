use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    table::{DupSort, Table},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use rocksdb::{ColumnFamilyDescriptor, ReadOptions, WriteBatch, WriteOptions, DB};
use std::marker::PhantomData;
use std::sync::Arc;

/// Generic transaction type for RocksDB
#[derive(Debug)]
pub struct RocksTransaction<const WRITE: bool> {
    /// Reference to DB
    db: Arc<DB>,
    /// Write batch for mutations (only used in write transactions)
    batch: Option<WriteBatch>,
    /// Read options
    read_opts: ReadOptions,
    /// Write options
    write_opts: WriteOptions,
    /// Marker for transaction type
    _marker: PhantomData<bool>,
}

impl<const WRITE: bool> RocksTransaction<WRITE> {
    /// Create new transaction
    pub(crate) fn new(db: Arc<DB>, _write: bool) -> Self {
        let batch = if WRITE { Some(WriteBatch::default()) } else { None };

        Self {
            db,
            batch,
            read_opts: ReadOptions::default(),
            write_opts: WriteOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Get the column family handle for a table
    fn get_cf<T: Table>(&self) -> Result<&rocksdb::ColumnFamily, DatabaseError> {
        self.db
            .cf_handle(T::NAME)
            .ok_or_else(|| DatabaseError::Other(format!("Column family not found: {}", T::NAME)))
    }
}

// Implement read-only transaction
impl<const WRITE: bool> DbTx for RocksTransaction<WRITE> {
    type Cursor<T: Table> = super::cursor::RocksCursor<T, WRITE>;
    type DupCursor<T: DupSort> = super::cursor::RocksDupCursor<T, WRITE>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let cf = self.get_cf::<T>()?;
        let key_bytes = key.encode();

        match self.db.get_cf_opt(cf, key_bytes, &self.read_opts)? {
            Some(value_bytes) => Ok(Some(T::Value::decode(&value_bytes)?)),
            None => Ok(None),
        }
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as reth_db_api::table::Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let cf = self.get_cf::<T>()?;

        match self.db.get_cf_opt(cf, key, &self.read_opts)? {
            Some(value_bytes) => Ok(Some(T::Value::decode(&value_bytes)?)),
            None => Ok(None),
        }
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        super::cursor::RocksCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        super::cursor::RocksDupCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
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
    type CursorMut<T: Table> = super::cursor::RocksCursor<T, true>;
    type DupCursorMut<T: DupSort> = super::cursor::RocksDupCursor<T, true>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let cf = self.get_cf::<T>()?;
        if let Some(batch) = &self.batch {
            batch.put_cf(cf, key.encode(), value.compress());
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
            batch.delete_cf(cf, key.encode());
        }
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        let cf = self.get_cf::<T>()?;
        // Drop and recreate column family
        self.db.drop_cf(T::NAME)?;
        self.db.create_cf(T::NAME, &Options::default())?;
        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        super::cursor::RocksCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        super::cursor::RocksDupCursor::new(self.db.clone(), self.get_cf::<T>()?.clone())
    }
}
