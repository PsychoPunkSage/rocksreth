use reth_db_api::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, Walker},
    table::{DupSort, Table},
    DatabaseError,
};
use rocksdb::{DBIterator, Direction, IteratorMode, DB};
use std::marker::PhantomData;
use std::sync::Arc;

/// RocksDB cursor implementation
pub struct RocksCursor<T: Table, const WRITE: bool> {
    /// Database reference
    db: Arc<DB>,
    /// Column family
    cf: Arc<rocksdb::ColumnFamily>,
    /// Current iterator
    iter: DBIterator<'static>,
    /// Marker for table type
    _marker: PhantomData<T>,
}

impl<T: Table, const WRITE: bool> RocksCursor<T, WRITE> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<rocksdb::ColumnFamily>) -> Result<Self, DatabaseError> {
        let iter = db.iterator_cf(&cf, IteratorMode::Start);

        Ok(Self { db, cf, iter, _marker: PhantomData })
    }
}

impl<T: Table, const WRITE: bool> DbCursorRO<T> for RocksCursor<T, WRITE> {
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.iter.set_mode(IteratorMode::Start);
        match self.iter.next() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let key_bytes = key.encode();
        self.iter.set_mode(IteratorMode::From(&key_bytes, Direction::Forward));

        match self.iter.next() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let key_bytes = key.encode();
        self.iter.set_mode(IteratorMode::From(&key_bytes, Direction::Forward));

        match self.iter.next() {
            Some(Ok((found_key, value))) if found_key == key_bytes => {
                Ok(Some((T::Key::decode(&found_key)?, T::Value::decode(&value)?)))
            }
            _ => Ok(None),
        }
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        match self.iter.next() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.iter.prev();
        match self.iter.next() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.iter.set_mode(IteratorMode::End);
        match self.iter.next() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        match self.iter.current() {
            Some(Ok((key, value))) => Ok(Some((T::Key::decode(&key)?, T::Value::decode(&value)?))),
            Some(Err(e)) => Err(DatabaseError::Backend(Box::new(e))),
            None => Ok(None),
        }
    }
}

/// RocksDB implementation of duplicate cursor
pub struct RocksDupCursor<T: DupSort, const WRITE: bool> {
    cursor: RocksCursor<T, WRITE>,
}

impl<T: DupSort, const WRITE: bool> RocksDupCursor<T, WRITE> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<rocksdb::ColumnFamily>) -> Result<Self, DatabaseError> {
        Ok(Self { cursor: RocksCursor::new(db, cf)? })
    }
}

// Implement required cursor traits for RocksDupCursor
impl<T: DupSort, const WRITE: bool> DbCursorRO<T> for RocksDupCursor<T, WRITE> {
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.first()
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.seek(key)
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.seek_exact(key)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.next()
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.prev()
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.current()
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.cursor.last()
    }
}

// For write transactions
impl<T: Table> DbCursorRW<T> for RocksCursor<T, true> {
    fn put(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode();
        let value_bytes = value.compress();
        self.db.put_cf(&self.cf, key_bytes, value_bytes)?;
        Ok(())
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some(Ok((key, _))) = self.iter.current() {
            self.db.delete_cf(&self.cf, key)?;
        }
        Ok(())
    }
}

// Implement DupSort capabilities
impl<T: DupSort> DbDupCursorRO<T> for RocksDupCursor<T, true> {
    fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Get current key
        if let Some(Ok((composite_key, value))) = self.cursor.iter.current() {
            let (current_key, _) = DupSortHelper::split_composite_key::<T>(&composite_key)?;

            // Get next entry
            if let Some(Ok((next_composite, next_value))) = self.cursor.iter.next() {
                let (next_key, _) = DupSortHelper::split_composite_key::<T>(&next_composite)?;

                // Check if we're still on same key
                if current_key == next_key {
                    return Ok(Some((next_key, T::Value::decode(&next_value)?)));
                }
            }
        }
        Ok(None)
    }

    fn next_no_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Get current key
        if let Some(Ok((composite_key, _))) = self.cursor.iter.current() {
            let (current_key, _) = DupSortHelper::split_composite_key::<T>(&composite_key)?;

            // Skip all entries with same key
            while let Some(Ok((next_composite, next_value))) = self.cursor.iter.next() {
                let (next_key, _) = DupSortHelper::split_composite_key::<T>(&next_composite)?;
                if current_key != next_key {
                    return Ok(Some((next_key, T::Value::decode(&next_value)?)));
                }
            }
        }
        Ok(None)
    }

    fn next_dup_val(&mut self) -> Result<Option<T::Value>, DatabaseError> {
        // Similar to next_dup but returns only value
        if let Some(Ok((composite_key, value))) = self.cursor.iter.current() {
            let (current_key, _) = DupSortHelper::split_composite_key::<T>(&composite_key)?;

            if let Some(Ok((next_composite, next_value))) = self.cursor.iter.next() {
                let (next_key, _) = DupSortHelper::split_composite_key::<T>(&next_composite)?;

                if current_key == next_key {
                    return Ok(Some(T::Value::decode(&next_value)?));
                }
            }
        }
        Ok(None)
    }

    fn seek_by_key_subkey(
        &mut self,
        key: T::Key,
        subkey: T::SubKey,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let composite_key = DupSortHelper::create_composite_key::<T>(&key, &subkey)?;

        self.cursor.iter.set_mode(IteratorMode::From(&composite_key, Direction::Forward));

        if let Some(Ok((found_key, value))) = self.cursor.iter.next() {
            if found_key == composite_key {
                return Ok(Some(T::Value::decode(&value)?));
            }
        }

        Ok(None)
    }
}
