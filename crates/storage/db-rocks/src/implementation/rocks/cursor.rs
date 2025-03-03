use reth_db_api::table::{Decode, Encode};
use reth_db_api::{
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{DupSort, Table},
    DatabaseError,
};
use rocksdb::{ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
use std::ops::Bound;
use std::ops::RangeBounds;
use std::result::Result::Ok;
use std::sync::Arc;

/// RocksDB cursor implementation
pub struct RocksCursor<T: Table, const WRITE: bool> {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    // Use 'static / Box<dyn ...> to avoid external lifetime parameters
    // iter: Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + Send + Sync>,
    iter: rocksdb::DBIteratorWithThreadMode<'static, DB>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Table, const WRITE: bool> RocksCursor<T, WRITE> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);
        let iter = db.as_ref().iterator_cf_opt(&*cf, read_opts, IteratorMode::Start);
        // .map_err(|e| DatabaseError::Other(e.to_string())); // *** BIG ISSUE

        Ok(Self { db, cf, iter, _marker: std::marker::PhantomData })
    }
}

impl<T: Table, const WRITE: bool> DbCursorRO<T> for RocksCursor<T, WRITE> {
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Reinitialize the iterator to start at the beginning
        self.iter = self.db.as_ref().iterator_cf_opt(
            &*self.cf,
            ReadOptions::default(),
            IteratorMode::Start,
        );

        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            // Some(Ok((kv, vk))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            None => Ok(None),
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let key_bytes = key.encode();
        self.iter = self.db.as_ref().iterator_cf_opt(
            &*self.cf,
            ReadOptions::default(),
            IteratorMode::From(&key_bytes, Direction::Forward),
        );

        // Now check if the current item matches the key
        if let Some(Ok((k, v))) = self.iter.next() {
            if k == key_bytes {
                return Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?)));
            }
        }
        Ok(None)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let key_bytes = key.encode();

        // Reset the iterators to seek to the key
        self.iter = self.db.as_ref().iterator_cf_opt(
            &*self.cf,
            ReadOptions::default(),
            IteratorMode::From(&key_bytes, Direction::Forward),
        );

        // Get the 1st item that is >= key
        if let Some(Ok((k, v))) = self.iter.next() {
            return Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?)));
        }

        Ok(None)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            None => Ok(None),
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let current_key =
            if let Some(Ok((k, _))) = self.iter.item() { Some(k.clone()) } else { None };

        match current_key {
            Some(key) => {
                self.iter = self.db.as_ref().iterator_cf_opt(
                    &*self.cf,
                    ReadOptions::default(),
                    IteratorMode::From(&key, Direction::Reverse),
                );
            }

            None => {
                // If no current key found: Position at the end and try to get the last entry
                self.iter = self.db.as_ref().iterator_cf_opt(
                    &*self.cf,
                    ReadOptions::default(),
                    IteratorMode::End,
                );

                match self.iter.next() {
                    Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)))),
                    None => Ok(None),
                    Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
                }
            }
        }

        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            None => Ok(None),
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.iter.seek_to_last();
        match self.iter.next() {
            Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            None => Ok(None),
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        match self.iter.item() {
            Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
            None => Ok(None),
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };
        Ok(Walker::new(self, Ok(start)))
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

        Ok(RangeWalker::new(self, Ok(start), end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

        Ok(ReverseWalker::new(self, Ok(start)))
    }
}

impl<T: Table> DbCursorRW<T> for RocksCursor<T, true> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode();
        let value_bytes = value.encode();
        self.db
            .put_cf(&self.cf, key_bytes, value_bytes)
            .map_err(|e| DatabaseError::Other(e.to_string()))
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        if self.seek_exact(key.clone())?.is_some() {
            return Err(DatabaseError::KeyExists);
        }
        self.upsert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.upsert(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some((key, _)) = self.current()? {
            self.db
                .delete_cf(&self.cf, key.encode())
                .map_err(|e| DatabaseError::Other(e.to_string()))?;
        }
        Ok(())
    }
}

/// RocksDB duplicate cursor implementation
pub struct RocksDupCursor<T: DupSort, const WRITE: bool> {
    inner: RocksCursor<T, WRITE>,
    current_key: Option<T::Key>,
}

impl<T: DupSort, const WRITE: bool> RocksDupCursor<T, WRITE> {
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
        Ok(Self { inner: RocksCursor::new(db, cf)?, current_key: None })
    }
}

impl<T: DupSort, const WRITE: bool> DbCursorRO<T> for RocksDupCursor<T, WRITE> {
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.first()?;
        if let Some((key, _)) = &result {
            self.current_key = Some(key.clone());
        }
        Ok(result)
    }

    // Implement other required methods similar to RocksCursor...
    // Copy implementations from RocksCursor but maintain current_key state

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.seek_exact(key.clone())?;
        if result.is_some() {
            self.current_key = Some(key);
        }
        Ok(result)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.seek(key)?;
        if let Some((key, _)) = &result {
            self.current_key = Some(key.clone());
        }
        Ok(result)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.next()?;
        if let Some((key, _)) = &result {
            self.current_key = Some(key.clone());
        }
        Ok(result)
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.prev()?;
        if let Some((key, _)) = &result {
            self.current_key = Some(key.clone());
        }
        Ok(result)
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        let result = self.inner.last()?;
        if let Some((key, _)) = &result {
            self.current_key = Some(key.clone());
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
        Ok(Walker::new(self, Ok(start)))
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

        Ok(RangeWalker::new(self, Ok(start), end_bound))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };
        Ok(ReverseWalker::new(self, Ok(start)))
    }
}

impl<T: DupSort, const WRITE: bool> DbDupCursorRO<T> for RocksDupCursor<T, WRITE> {
    fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        if let Some(current_key) = &self.current_key {
            let next = self.inner.next()?;
            if let Some((key, value)) = next {
                if &key == current_key {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn next_no_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        while let Some((key, _)) = self.next()? {
            if Some(&key) != self.current_key.as_ref() {
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
        let composite_key = T::compose_key(&key, &subkey);
        self.inner.seek_exact(composite_key).map(|opt| opt.map(|(_, v)| v))
    }

    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        match (key, subkey) {
            (Some(k), Some(sk)) => {
                self.seek_by_key_subkey(k.clone(), sk)?;
                self.current_key = Some(k);
            }
            (Some(k), None) => {
                self.seek(k.clone())?;
                self.current_key = Some(k);
            }
            (None, Some(_)) => self.first()?,
            (None, None) => self.first()?,
        }
        Ok(DupWalker::new(self))
    }
}

impl<T: DupSort> DbCursorRW<T> for RocksDupCursor<T, true> {
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

impl<T: DupSort> DbDupCursorRW<T> for RocksDupCursor<T, true> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        if let Some(current_key) = &self.current_key {
            // Keep track of the current key while deleting duplicates
            let key = current_key.clone();
            while let Some((cur_key, _)) = self.inner.current()? {
                if &cur_key != &key {
                    break;
                }
                self.inner.delete_current()?;
                // Move to next without updating current_key
                if self.inner.next()?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.inner.append(key, value)
    }
}
