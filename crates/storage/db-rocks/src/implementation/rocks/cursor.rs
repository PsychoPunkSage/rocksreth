use super::dupsort::DupSortHelper;
use crate::implementation::rocks::tx::CFPtr;
use reth_db_api::{
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Compress, Decode, Decompress, DupSort, Encode, Table},
    DatabaseError,
};
use rocksdb::{Direction, IteratorMode, ReadOptions, DB};
use std::ops::RangeBounds;
use std::result::Result::Ok;
use std::sync::{Arc, Mutex};
use std::{marker::PhantomData, ops::Bound};

/// RocksDB cursor implementation
pub struct RocksCursor<T: Table, const WRITE: bool> {
    db: Arc<DB>,
    cf: CFPtr,
    // // Store the current key-value pair
    // current_item: Option<(Box<[u8]>, Box<[u8]>)>,
    // iterator: Option<rocksdb::DBIterator<'static>>,
    // iterator: Option<rocksdb::DBIterator<'static>>,
    // Current position with cached key and value
    // current_position: Mutex<Option<(T::Key, T::Value)>>,
    current_key_bytes: Mutex<Option<Vec<u8>>>,
    current_value_bytes: Mutex<Option<Vec<u8>>>,
    // Next seek position to track where we were
    next_seek_key: Mutex<Option<Vec<u8>>>,
    // Read options
    read_opts: ReadOptions,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Table, const WRITE: bool> RocksCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone,
{
    // pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
    pub(crate) fn new(db: Arc<DB>, cf: CFPtr) -> Result<Self, DatabaseError> {
        Ok(Self {
            db,
            cf,
            next_seek_key: Mutex::new(None),
            // current_position: Mutex::new(None),
            current_key_bytes: Mutex::new(None),
            current_value_bytes: Mutex::new(None),
            read_opts: ReadOptions::default(),
            _marker: PhantomData,
        })
    }

    /// Get the column family reference safely
    #[inline]
    fn get_cf(&self) -> &rocksdb::ColumnFamily {
        // Safety: The cf_ptr is guaranteed to be valid as long as the DB is alive,
        // and we hold an Arc to the DB
        unsafe { &*self.cf }
    }

    /// Create a single-use iterator for a specific operation
    fn create_iterator(&self, mode: IteratorMode) -> rocksdb::DBIterator {
        let cf = self.get_cf();
        self.db.iterator_cf_opt(cf, ReadOptions::default(), mode)
    }

    /// Get the current key/value pair
    fn get_current(&self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Get the current key bytes
        let key_bytes = {
            let key_guard = match self.current_key_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            match &*key_guard {
                Some(bytes) => bytes.clone(),
                None => return Ok(None),
            }
        };

        // Get the current value bytes
        let value_bytes = {
            let value_guard = match self.current_value_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            match &*value_guard {
                Some(bytes) => bytes.clone(),
                None => return Ok(None),
            }
        };

        // Decode the key and value
        match T::Key::decode(&key_bytes) {
            Ok(key) => match T::Value::decompress(&value_bytes) {
                Ok(value) => Ok(Some((key, value))),
                Err(e) => Err(e),
            },
            Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
        }
    }

    /// Update the current position
    fn update_position(&self, key_bytes: Vec<u8>, value_bytes: Vec<u8>) {
        // Update the current key
        let mut key_guard = match self.current_key_bytes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        *key_guard = Some(key_bytes);

        // Update the current value
        let mut value_guard = match self.current_value_bytes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        *value_guard = Some(value_bytes);
    }

    /// Clear the current position
    fn clear_position(&self) {
        // Clear the current key
        let mut key_guard = match self.current_key_bytes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        *key_guard = None;

        // Clear the current value
        let mut value_guard = match self.current_value_bytes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        *value_guard = None;
    }

    /// Get the first key/value pair from the database
    fn get_first(&self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let cf = self.get_cf();

        // Create an iterator that starts at the beginning
        let mut iter = self.create_iterator(IteratorMode::Start);

        // Get the first item
        match iter.next() {
            Some(Ok((key_bytes, value_bytes))) => {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            }
            Some(Err(e)) => Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e))),
            None => {
                // No entries, clear the current position
                self.clear_position();
                Ok(None)
            }
        }
    }

    /// Get the last key/value pair from the database
    fn get_last(&self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let cf = self.get_cf();

        // Create an iterator that starts at the end
        let mut iter = self.create_iterator(IteratorMode::End);

        // Get the last item
        match iter.next() {
            Some(Ok((key_bytes, value_bytes))) => {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            }
            Some(Err(e)) => Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e))),
            None => {
                // No entries, clear the current position
                self.clear_position();
                Ok(None)
            }
        }
    }

    /// Seek to a specific key
    fn get_seek(&self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let cf = self.get_cf();

        // Encode the key
        let encoded_key = key.encode();

        // Create an iterator that starts at the given key
        let mut iter =
            self.create_iterator(IteratorMode::From(encoded_key.as_ref(), Direction::Forward));

        // Get the first item (the one at or after the key)
        match iter.next() {
            Some(Ok((key_bytes, value_bytes))) => {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            }
            Some(Err(e)) => Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e))),
            None => {
                // No entries after the given key, clear the current position
                self.clear_position();
                Ok(None)
            }
        }
    }

    fn get_seek_exact(&self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let cf = self.get_cf();

        // Encode the key
        let encoded_key = key.encode();

        // Create a new ReadOptions for this specific query
        let read_opts = ReadOptions::default();

        // Create an iterator that starts at the given key
        let mut iter = self.db.iterator_cf_opt(
            cf,
            read_opts,
            IteratorMode::From(encoded_key.as_ref(), Direction::Forward),
        );

        // Check the first item (should be exactly at or after the key)
        if let Some(Ok((key_bytes, value_bytes))) = iter.next() {
            // Check if this is an exact match
            if key_bytes.as_ref() == encoded_key.as_ref() {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(decoded_key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((decoded_key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            } else {
                // Not an exact match, don't update position
                Ok(None)
            }
        } else {
            // No items at or after the key
            Ok(None)
        }
    }

    /// Get the next key/value pair
    fn get_next(&self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Get the current key bytes
        let current_key_bytes = {
            let key_guard = match self.current_key_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            match &*key_guard {
                Some(bytes) => bytes.clone(),
                None => {
                    // If we don't have a current position, get the first item
                    return self.get_first();
                }
            }
        };

        // Create an iterator that starts right after the current position
        let mut iter =
            self.create_iterator(IteratorMode::From(&current_key_bytes, Direction::Forward));

        // Skip the current item (which is the one we're positioned at)
        match iter.next() {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                return Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e)))
            }
            None => {
                // No entries, clear the current position
                self.clear_position();
                return Ok(None);
            }
        }

        // Get the next item
        match iter.next() {
            Some(Ok((key_bytes, value_bytes))) => {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            }
            Some(Err(e)) => Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e))),
            None => {
                // No more entries, clear the current position
                self.clear_position();
                Ok(None)
            }
        }
    }

    /// Get the previous key/value pair
    fn get_prev(&self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Get the current key bytes
        let current_key_bytes = {
            let key_guard = match self.current_key_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            match &*key_guard {
                Some(bytes) => bytes.clone(),
                None => {
                    // If we don't have a current position, get the last item
                    return self.get_last();
                }
            }
        };

        // Create an iterator that starts right before the current position
        let mut iter =
            self.create_iterator(IteratorMode::From(&current_key_bytes, Direction::Reverse));

        // Skip the current item (which is the one we're positioned at)
        match iter.next() {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                return Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e)))
            }
            None => {
                // No entries, clear the current position
                self.clear_position();
                return Ok(None);
            }
        }

        // Get the previous item
        match iter.next() {
            Some(Ok((key_bytes, value_bytes))) => {
                // Update the current position
                self.update_position(key_bytes.to_vec(), value_bytes.to_vec());

                // Try to decode the key and value
                match T::Key::decode(&key_bytes) {
                    Ok(key) => match T::Value::decompress(&value_bytes) {
                        Ok(value) => Ok(Some((key, value))),
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DatabaseError::Other(format!("Key decode error: {}", e))),
                }
            }
            Some(Err(e)) => Err(DatabaseError::Other(format!("RocksDB iterator error: {}", e))),
            None => {
                // No more entries, clear the current position
                self.clear_position();
                Ok(None)
            }
        }
    }
}

impl<T: Table, const WRITE: bool> DbCursorRO<T> for RocksCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
{
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_first()
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_seek_exact(key)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_seek(key)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_next()
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_prev()
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_last()
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.get_current()
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

        // Convert to expected type for Walker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(Walker::new(self, iter_pair_result))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone())?,
            Bound::Excluded(key) => {
                let mut pos = self.seek(key.clone())?;
                if pos.is_some() {
                    pos = self.next()?;
                }
                pos
            }
            Bound::Unbounded => self.first()?,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Convert to expected type for RangeWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(RangeWalker::new(self, iter_pair_result, end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

        // Convert to expected type for ReverseWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(ReverseWalker::new(self, iter_pair_result))
    }
}

impl<T: Table> DbCursorRW<T> for RocksCursor<T, true>
where
    T::Key: Encode + Decode + Clone,
    T::Value: Compress + Decompress,
{
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        // Clone before encoding
        let key_clone = key.clone();

        let key_bytes = key_clone.encode();
        // let value_bytes: Vec<u8> = value.compress().into();
        let mut compressed = <<T as Table>::Value as Compress>::Compressed::default();
        value.compress_to_buf(&mut compressed);
        let value_bytes: Vec<u8> = compressed.into();

        // Clone before using to avoid borrowing self
        let db = self.db.clone();
        let cf = unsafe { &*self.cf };

        db.put_cf(cf, key_bytes, value_bytes).map_err(|e| DatabaseError::Other(e.to_string()))
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        if self.seek_exact(key.clone())?.is_some() {
            return Err(DatabaseError::Other("Key already exists".to_string()));
        }
        self.upsert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.upsert(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some((key, _)) = self.current()? {
            // Clone before using to avoid borrowing self
            let db = self.db.clone();
            let cf = unsafe { &*self.cf };

            // Clone key before encoding
            let key_clone = key.clone();
            let key_bytes = key_clone.encode();

            db.delete_cf(cf, key_bytes).map_err(|e| DatabaseError::Other(e.to_string()))?;

            // Move to next item
            let _ = self.next()?;
        }
        Ok(())
    }
}

/// RocksDB duplicate cursor implementation
pub struct RocksDupCursor<T: DupSort, const WRITE: bool> {
    inner: RocksCursor<T, WRITE>,
    current_key: Option<T::Key>,
}

impl<T: DupSort, const WRITE: bool> RocksDupCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone,
    T::SubKey: Encode + Decode + Clone,
{
    pub(crate) fn new(db: Arc<DB>, cf: CFPtr) -> Result<Self, DatabaseError> {
        Ok(Self { inner: RocksCursor::new(db, cf)?, current_key: None })
    }
}
impl<T: DupSort, const WRITE: bool> DbCursorRO<T> for RocksDupCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.first()?;
        if let Some((ref key, _)) = result {
            self.current_key = Some(key.clone());
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let key_clone = key.clone();
        let result = self.inner.seek_exact(key_clone)?;
        if result.is_some() {
            self.current_key = Some(key);
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.seek(key)?;
        if let Some((ref key, _)) = result {
            self.current_key = Some(key.clone());
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.next()?;
        if let Some((ref key, _)) = result {
            self.current_key = Some(key.clone());
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.prev()?;
        if let Some((ref key, _)) = result {
            self.current_key = Some(key.clone());
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.last()?;
        if let Some((ref key, _)) = result {
            self.current_key = Some(key.clone());
        } else {
            self.current_key = None;
        }
        Ok(result)
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.inner.current()
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

        // Convert to expected type for Walker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(Walker::new(self, iter_pair_result))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone())?,
            Bound::Excluded(key) => {
                let mut pos = self.seek(key.clone())?;
                if pos.is_some() {
                    pos = self.next()?;
                }
                pos
            }
            Bound::Unbounded => self.first()?,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Convert to expected type for RangeWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(RangeWalker::new(self, iter_pair_result, end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

        // Convert to expected type for ReverseWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(ReverseWalker::new(self, iter_pair_result))
    }
}

impl<T: DupSort, const WRITE: bool> DbDupCursorRO<T> for RocksDupCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        if let Some(ref current_key) = self.current_key {
            let current_key_clone = current_key.clone();
            let next = self.inner.next()?;
            if let Some((key, value)) = next {
                if &key == current_key {
                    self.current_key = Some(key.clone());
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn next_no_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let current_key_clone = self.current_key.clone();

        while let Some((key, _)) = self.next()? {
            if Some(&key) != current_key_clone.as_ref() {
                self.current_key = Some(key.clone());
                return self.current();
            }
        }
        Ok(None)
    }

    fn next_dup_val(&mut self) -> Result<Option<T::Value>, DatabaseError> {
        self.next_dup().map(|opt| opt.map(|(_, v)| v))
    }

    fn seek_by_key_subkey(
        &mut self,
        key: T::Key,
        subkey: T::SubKey,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let composite_key_vec = DupSortHelper::create_composite_key::<T>(&key, &subkey)?;

        // Convert the Vec<u8> to T::Key using encode_composite_key
        let encoded_key = DupSortHelper::encode_composite_key::<T>(composite_key_vec)?;

        // Now pass the properly typed key to seek_exact
        let result = self.inner.seek_exact(encoded_key)?;

        if result.is_some() {
            self.current_key = Some(key);
        }

        Ok(result.map(|(_, v)| v))
    }

    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match (key.clone(), subkey.clone()) {
            (Some(k), Some(sk)) => {
                let _ = self.seek_by_key_subkey(k.clone(), sk)?;
                self.current_key = Some(k);
                self.current().transpose()
            }
            (Some(k), None) => {
                let _ = self.seek(k.clone())?;
                self.current_key = Some(k);
                self.current().transpose()
            }
            (None, Some(_)) => {
                let _ = self.first()?;
                self.current().transpose()
            }
            (None, None) => {
                let _ = self.first()?;
                self.current().transpose()
            }
        };

        Ok(DupWalker { cursor: self, start })
    }
}

impl<T: DupSort> DbCursorRW<T> for RocksDupCursor<T, true>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Compress + Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.upsert(key, value)
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.insert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.inner.append(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        self.inner.delete_current()
    }
}

impl<T: DupSort> DbDupCursorRW<T> for RocksDupCursor<T, true>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Compress + Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        if let Some(ref current_key) = self.current_key.clone() {
            // Keep track of the current key while deleting duplicates
            let key_clone = current_key.clone();
            while let Some((cur_key, _)) = self.inner.current()? {
                if &cur_key != &key_clone {
                    break;
                }
                self.inner.delete_current()?;
                // Don't need to call next here since delete_current already moves to next
            }
        }
        Ok(())
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        // Note: append_dup takes ownership of value, but inner.append expects a reference
        self.inner.append(key, &value)
    }
}

pub struct ThreadSafeRocksCursor<T: Table, const WRITE: bool> {
    cursor: Mutex<RocksCursor<T, WRITE>>,
    // Add a phantom data to ensure proper Send/Sync implementation
    _marker: std::marker::PhantomData<*const ()>,
}

impl<T: Table, const WRITE: bool> ThreadSafeRocksCursor<T, WRITE> {
    pub fn new(cursor: RocksCursor<T, WRITE>) -> Self {
        Self { cursor: Mutex::new(cursor), _marker: std::marker::PhantomData }
    }
}

impl<T: Table, const WRITE: bool> DbCursorRO<T> for ThreadSafeRocksCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
{
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        // cursor_guard.first()
        let mut guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.first()
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.seek_exact(key)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.seek(key)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.next()
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.prev()
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.last()
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // let mut cursor_guard = self.cursor.lock().unwrap();
        let mut cursor_guard = match self.cursor.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        cursor_guard.current()
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

        // Convert to expected type for Walker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(Walker::new(self, iter_pair_result))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone())?,
            Bound::Excluded(key) => {
                let mut pos = self.seek(key.clone())?;
                if pos.is_some() {
                    pos = self.next()?;
                }
                pos
            }
            Bound::Unbounded => self.first()?,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Convert to expected type for RangeWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(RangeWalker::new(self, iter_pair_result, end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

        // Convert to expected type for ReverseWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(ReverseWalker::new(self, iter_pair_result))
    }
}

impl<T: Table> DbCursorRW<T> for ThreadSafeRocksCursor<T, true>
where
    T::Key: Encode + Decode + Clone,
    T::Value: Compress + Decompress,
{
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.upsert(key, value)
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.insert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.append(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.delete_current()
    }
}

unsafe impl<T: Table, const WRITE: bool> Send for ThreadSafeRocksCursor<T, WRITE>
where
    T::Key: Send,
    T::Value: Send,
{
}

unsafe impl<T: Table, const WRITE: bool> Sync for ThreadSafeRocksCursor<T, WRITE>
where
    T::Key: Sync,
    T::Value: Sync,
{
}

pub struct ThreadSafeRocksDupCursor<T: DupSort, const WRITE: bool> {
    cursor: Mutex<RocksDupCursor<T, WRITE>>,
    // Add a phantom data to ensure proper Send/Sync implementation
    _marker: std::marker::PhantomData<*const ()>,
}

impl<T: DupSort, const WRITE: bool> ThreadSafeRocksDupCursor<T, WRITE> {
    pub fn new(cursor: RocksDupCursor<T, WRITE>) -> Self {
        Self { cursor: Mutex::new(cursor), _marker: std::marker::PhantomData }
    }
}

impl<T: DupSort, const WRITE: bool> DbCursorRO<T> for ThreadSafeRocksDupCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.first()
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.seek_exact(key)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.seek(key)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.next()
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.prev()
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.last()
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.current()
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

        // Convert to expected type for Walker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(Walker::new(self, iter_pair_result))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(key) => self.seek(key.clone())?,
            Bound::Excluded(key) => {
                let mut pos = self.seek(key.clone())?;
                if pos.is_some() {
                    pos = self.next()?;
                }
                pos
            }
            Bound::Unbounded => self.first()?,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Convert to expected type for RangeWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(RangeWalker::new(self, iter_pair_result, end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

        // Convert to expected type for ReverseWalker::new
        let iter_pair_result = match start {
            Some(val) => Some(Ok(val)),
            None => None,
        };

        Ok(ReverseWalker::new(self, iter_pair_result))
    }
}

impl<T: DupSort, const WRITE: bool> DbDupCursorRO<T> for ThreadSafeRocksDupCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.next_dup()
    }

    fn next_no_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.next_no_dup()
    }

    fn next_dup_val(&mut self) -> Result<Option<T::Value>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.next_dup_val()
    }

    fn seek_by_key_subkey(
        &mut self,
        key: T::Key,
        subkey: T::SubKey,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.seek_by_key_subkey(key, subkey)
    }

    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match (key.clone(), subkey.clone()) {
            (Some(k), Some(sk)) => {
                let _ = self.seek_by_key_subkey(k.clone(), sk)?;
                self.current().transpose()
            }
            (Some(k), None) => {
                let _ = self.seek(k.clone())?;
                self.current().transpose()
            }
            (None, Some(_)) => {
                let _ = self.first()?;
                self.current().transpose()
            }
            (None, None) => {
                let _ = self.first()?;
                self.current().transpose()
            }
        };

        Ok(DupWalker { cursor: self, start })
    }
}

impl<T: DupSort> DbDupCursorRW<T> for ThreadSafeRocksDupCursor<T, true>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Compress + Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.delete_current_duplicates()
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.append_dup(key, value)
    }
}

impl<T: DupSort> DbCursorRW<T> for ThreadSafeRocksDupCursor<T, true>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Compress + Decompress,
    T::SubKey: Encode + Decode + Clone,
{
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.upsert(key, value)
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.insert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.append(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        let mut cursor_guard = self.cursor.lock().unwrap();
        cursor_guard.delete_current()
    }
}

unsafe impl<T: DupSort, const WRITE: bool> Send for ThreadSafeRocksDupCursor<T, WRITE>
where
    T::Key: Send,
    T::Value: Send,
    T::SubKey: Send,
{
}

unsafe impl<T: DupSort, const WRITE: bool> Sync for ThreadSafeRocksDupCursor<T, WRITE>
where
    T::Key: Sync,
    T::Value: Sync,
    T::SubKey: Sync,
{
}
