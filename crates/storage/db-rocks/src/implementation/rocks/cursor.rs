// use reth_db_api::table::{Decode, Encode};
// use reth_db_api::{
//     cursor::{
//         DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
//         ReverseWalker, Walker,
//     },
//     table::{DupSort, Table},
//     DatabaseError,
// };
// use rocksdb::{ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
// use std::ops::Bound;
// use std::ops::RangeBounds;
// use std::result::Result::Ok;
// use std::result::Result::Ok;
// use std::sync::Arc;

// /// RocksDB cursor implementation
// pub struct RocksCursor<T: Table, const WRITE: bool> {
//     db: Arc<DB>,
//     cf: Arc<ColumnFamily>,
//     // Use 'static / Box<dyn ...> to avoid external lifetime parameters
//     // iter: Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + Send + Sync>,
//     iter: rocksdb::DBIteratorWithThreadMode<'static, DB>,
//     current_item: Option<(Box<[u8]>, Box<[u8]>)>,
//     _marker: std::marker::PhantomData<T>,
// }

// impl<T: Table, const WRITE: bool> RocksCursor<T, WRITE>
// where
//     T::Key: Encode + Decode,
//     T::Value: Encode + Decode,
// {
//     pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
//         let mut read_opts = ReadOptions::default();
//         read_opts.set_verify_checksums(false);
//         let iter = db.clone().as_ref().iterator_cf_opt(&*cf, read_opts, IteratorMode::Start);
//         // .map_err(|e| DatabaseError::Other(e.to_string())); // *** BIG ISSUE

//         Ok(Self { db, cf, iter, current_item: None, _marker: std::marker::PhantomData })
//     }

//     // Helper to update current_item
//     fn update_current(&mut self, item: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>) {
//         self.current_item = match item {
//             Some(Ok((k, v))) => Some((k, v)),
//             _ => None,
//         }
//     }
// }

// impl<T: Table, const WRITE: bool> DbCursorRO<T> for RocksCursor<T, WRITE>
// where
//     T::Key: Encode + Decode,
//     T::Value: Encode + Decode,
// {
//     fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         // Reinitialize the iterator to start at the beginning
//         self.iter = self.db.as_ref().iterator_cf_opt(
//             &*self.cf,
//             ReadOptions::default(),
//             IteratorMode::Start,
//         );

//         let next_item = self.iter.next();
//         self.update_current(next_item.clone());

//         match next_item {
//             Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//             // Some(Ok((kv, vk))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//             None => Ok(None),
//             Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//         }
//     }

//     fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let key_bytes = key.encode();
//         self.iter = self.db.as_ref().iterator_cf_opt(
//             &*self.cf,
//             ReadOptions::default(),
//             IteratorMode::From(key_bytes.as_ref(), Direction::Forward),
//         );

//         let next_item = self.iter.next();
//         self.update_current(next_item.clone());

//         // Now check if the current item matches the key
//         match next_item {
//             Some(Ok((k, v))) => {
//                 if k.as_ref() == key_bytes.as_ref() {
//                     Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?)))
//                 } else {
//                     Ok(None)
//                 }
//             }
//             _ => Ok(None),
//         }
//     }

//     fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let key_bytes = key.encode();

//         // Reset the iterators to seek to the key
//         self.iter = self.db.as_ref().iterator_cf_opt(
//             &*self.cf,
//             ReadOptions::default(),
//             IteratorMode::From(&key_bytes, Direction::Forward),
//         );

//         // Get the 1st item that is >= key
//         let next_item = self.iter.next();
//         self.update_current(next_item.clone());

//         match next_item {
//             Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//             None => Ok(None),
//             Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//         }
//     }

//     fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let next_item = self.iter.next();
//         self.update_current(next_item.clone());

//         match next_item {
//             Some(Ok((k, v))) => Ok(Some((
//                 T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
//                 T::Value::decode(&v).map_err(|e| DatabaseError::Other(e.to_string()))?,
//             ))),
//             None => Ok(None),
//             Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//         }
//     }

//     fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let current_key = match &self.current_item {
//             Some((k, _)) => Some(k.clone()),
//             None => None,
//         };

//         match current_key {
//             Some(key) => {
//                 self.iter = self.db.as_ref().iterator_cf_opt(
//                     &*self.cf,
//                     ReadOptions::default(),
//                     IteratorMode::From(&key, Direction::Reverse),
//                 );

//                 // Skip the current item as it would be the same key
//                 let _ = self.iter.next();
//             }

//             None => {
//                 // If no current key found: Position at the end and try to get the last entry
//                 self.iter = self.db.as_ref().iterator_cf_opt(
//                     &*self.cf,
//                     ReadOptions::default(),
//                     IteratorMode::End,
//                 );
//             }
//         }

//         let next_item = self.iter.next();
//         self.update_current(next_item.clone());

//         match next_item {
//             Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//             None => Ok(None),
//             Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//         }
//     }

//     // fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//     //     self.iter =
//     //         self.db.iterator_cf_opt(self.cf.as_ref(), ReadOptions::default(), IteratorMode::End);

//     //     let next_item = self.iter.next();
//     //     self.update_current(next_item.clone());

//     //     match next_item {
//     //         Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//     //         None => Ok(None),
//     //         Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//     //     }
//     // }

//     fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         // Clone the Arc references to avoid borrowing self directly
//         let db = self.db.clone();
//         let cf = self.cf.clone();

//         // Create the iterator using the cloned references
//         let mut temp_iter =
//             db.iterator_cf_opt(cf.as_ref(), ReadOptions::default(), IteratorMode::End);

//         // Get the next item from this temporary iterator
//         let next_item = temp_iter.next();

//         // Store the current item
//         if let Some(Ok((k, v))) = &next_item {
//             self.current_item = Some((k.clone(), v.clone()));
//         } else {
//             self.current_item = None;
//         }

//         // Replace the iterator
//         self.iter = temp_iter;

//         // Return the result
//         match next_item {
//             Some(Ok((k, v))) => Ok(Some((T::Key::decode(&k)?, T::Value::decode(&v)?))),
//             None => Ok(None),
//             Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
//         }
//     }

//     fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         match &self.current_item {
//             Some((k, v)) => Ok(Some((T::Key::decode(k)?, T::Value::decode(v)?))),
//             None => Ok(None),
//         }
//     }

//     fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

//         // Convert to expected type for Walker::new
//         let iter_pair_result = match start {
//             Some(val) => Some(Ok(val)),
//             None => None,
//         };

//         Ok(Walker::new(self, iter_pair_result))
//     }

//     fn walk_range(
//         &mut self,
//         range: impl RangeBounds<T::Key>,
//     ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = match range.start_bound() {
//             Bound::Included(key) => self.seek(key.clone())?,
//             Bound::Excluded(key) => {
//                 let mut pos = self.seek(key.clone())?;
//                 if pos.is_some() {
//                     pos = self.next()?;
//                 }
//                 pos
//             }
//             Bound::Unbounded => self.first()?,
//         };

//         let end_bound = match range.end_bound() {
//             Bound::Included(key) => Bound::Included(key.clone()),
//             Bound::Excluded(key) => Bound::Excluded(key.clone()),
//             Bound::Unbounded => Bound::Unbounded,
//         };

//         // Convert to expected type for RangeWalker::new
//         let iter_pair_result = match start {
//             Some(val) => Some(Ok(val)),
//             None => None,
//         };

//         Ok(RangeWalker::new(self, iter_pair_result, end_bound))
//     }

//     fn walk_back(
//         &mut self,
//         start_key: Option<T::Key>,
//     ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };

//         // Convert to expected type for ReverseWalker::new
//         let iter_pair_result = match start {
//             Some(val) => Some(Ok(val)),
//             None => None,
//         };

//         Ok(ReverseWalker::new(self, iter_pair_result))
//     }
// }

// impl<T: Table> DbCursorRW<T> for RocksCursor<T, true>
// where
//     T::Key: Encode + Decode,
//     T::Value: Encode + Decode,
// {
//     fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         let key_bytes = key.encode();
//         let value_bytes = value.encode();
//         self.db
//             .put_cf(&self.cf, key_bytes, value_bytes)
//             .map_err(|e| DatabaseError::Other(e.to_string()))
//     }

//     fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         if self.seek_exact(key.clone())?.is_some() {
//             return Err(DatabaseError::Other("Key already exists".to_string()));
//         }
//         self.upsert(key, value)
//     }

//     fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         self.upsert(key, value)
//     }

//     fn delete_current(&mut self) -> Result<(), DatabaseError> {
//         if let Some((key, _)) = self.current()? {
//             self.db
//                 .delete_cf(&self.cf, key.encode())
//                 .map_err(|e| DatabaseError::Other(e.to_string()))?;
//         }
//         Ok(())
//     }
// }

// /// RocksDB duplicate cursor implementation
// pub struct RocksDupCursor<T: DupSort, const WRITE: bool> {
//     inner: RocksCursor<T, WRITE>,
//     current_key: Option<T::Key>,
// }

// impl<T: DupSort, const WRITE: bool> RocksDupCursor<T, WRITE>
// where
//     T::Key: Encode + Decode,
//     T::Value: Encode + Decode,
//     T::SubKey: Encode + Decode,
// {
//     pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
//         Ok(Self { inner: RocksCursor::new(db, cf)?, current_key: None })
//     }
// }

// impl<T: DupSort, const WRITE: bool> DbCursorRO<T> for RocksDupCursor<T, WRITE>
// where
//     T::Key: Encode + Decode + Clone,
//     T::Value: Encode + Decode,
//     T::SubKey: Encode + Decode,
// {
//     fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.first()?;
//         if let Some((key, _)) = &result {
//             self.current_key = Some(key);
//         }
//         Ok(result)
//     }

//     fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.seek_exact(key.clone())?;
//         if result.is_some() {
//             self.current_key = Some(key);
//         }
//         Ok(result)
//     }

//     fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.seek(key)?;
//         if let Some((key, _)) = &result {
//             self.current_key = Some(key.clone());
//         }
//         Ok(result)
//     }

//     fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.next()?;
//         if let Some((key, _)) = &result {
//             self.current_key = Some(key.clone());
//         }
//         Ok(result)
//     }

//     fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.prev()?;
//         if let Some((key, _)) = &result {
//             self.current_key = Some(key.clone());
//         }
//         Ok(result)
//     }

//     fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let result = self.inner.last()?;
//         if let Some((key, _)) = &result {
//             self.current_key = Some(key.clone());
//         }
//         Ok(result)
//     }

//     fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         self.inner.current()
//     }

//     fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = if let Some(key) = start_key { self.seek(key)? } else { self.first()? };

//         // Convert to expected type for Walker::new
//         let iter_pair_result = match start {
//             Some(val) => Some(Ok(val)),
//             None => None,
//         };

//         Ok(Walker::new(self, iter_pair_result))
//     }

//     fn walk_range(
//         &mut self,
//         range: impl RangeBounds<T::Key>,
//     ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = match range.start_bound() {
//             Bound::Included(key) => self.seek(key.clone())?,
//             Bound::Excluded(key) => {
//                 let mut pos = self.seek(key.clone())?;
//                 if pos.is_some() {
//                     pos = self.next()?;
//                 }
//                 pos
//             }
//             Bound::Unbounded => self.first()?,
//         };

//         let end_bound = match range.end_bound() {
//             Bound::Included(key) => Bound::Included(key.clone()),
//             Bound::Excluded(key) => Bound::Excluded(key.clone()),
//             Bound::Unbounded => Bound::Unbounded,
//         };

//         // Convert to expected type for RangeWalker::new
//         let iter_pair_result = match start {
//             Some(val) => Some(Ok(val)),
//             None => None,
//         };

//         Ok(RangeWalker::new(self, iter_pair_result, end_bound))
//     }

//     fn walk_back(
//         &mut self,
//         start_key: Option<T::Key>,
//     ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         let start = if let Some(key) = start_key { self.seek(key)? } else { self.last()? };
//         Ok(ReverseWalker::new(self, Ok(start)))
//     }
// }

// impl<T: DupSort, const WRITE: bool> DbDupCursorRO<T> for RocksDupCursor<T, WRITE>
// where
//     T::Key: Encode + Decode + Clone + PartialEq,
//     T::Value: Encode + Decode,
//     T::SubKey: Encode + Decode,
// {
//     fn next_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         if let Some(current_key) = &self.current_key {
//             let next = self.inner.next()?;
//             if let Some((key, value)) = next {
//                 if &key == current_key {
//                     return Ok(Some((key, value)));
//                 }
//             }
//         }
//         Ok(None)
//     }

//     fn next_no_dup(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
//         let mut current_key_clone = self.current_key.clone();

//         while let Some((key, _)) = self.next()? {
//             if Some(&key) != current_key_clone.as_ref() {
//                 self.current_key = Some(key.clone());
//                 return self.current();
//             }
//             current_key_clone = Some(key);
//         }
//         Ok(None)
//     }

//     fn next_dup_val(&mut self) -> Result<Option<T::Value>, DatabaseError> {
//         self.next_dup().map(|opt| opt.map(|(_, v)| v))
//     }

//     fn seek_by_key_subkey(
//         &mut self,
//         key: T::Key,
//         subkey: T::SubKey,
//     ) -> Result<Option<T::Value>, DatabaseError> {
//         let composite_key = T::Key::from_key_subkey(&key, &subkey); // *** DOUBT
//         self.inner.seek_exact(composite_key).map(|opt| opt.map(|(_, v)| v))
//     }

//     fn walk_dup(
//         &mut self,
//         key: Option<T::Key>,
//         subkey: Option<T::SubKey>,
//     ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
//     where
//         Self: Sized,
//     {
//         match (key, subkey) {
//             (Some(k), Some(sk)) => {
//                 self.seek_by_key_subkey(k.clone(), sk)?;
//                 self.current_key = Some(k);
//             }
//             (Some(k), None) => {
//                 self.seek(k.clone())?;
//                 self.current_key = Some(k);
//             }
//             (None, Some(_)) => self.first()?,
//             (None, None) => self.first()?,
//         }
//         Ok(DupWalker::new(self))
//     }
// }

// impl<T: DupSort> DbCursorRW<T> for RocksDupCursor<T, true>
// where
//     T::Key: Encode + Decode + Clone,
//     T::Value: Encode + Decode,
//     T::SubKey: Encode + Decode,
// {
//     fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         self.inner.upsert(key, value)
//     }

//     fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         self.inner.insert(key, value)
//     }

//     fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
//         self.inner.append(key, value)
//     }

//     fn delete_current(&mut self) -> Result<(), DatabaseError> {
//         self.inner.delete_current()
//     }
// }

// impl<T: DupSort> DbDupCursorRW<T> for RocksDupCursor<T, true> {
//     fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
//         if let Some(current_key) = &self.current_key {
//             // Keep track of the current key while deleting duplicates
//             let key = current_key.clone();
//             while let Some((cur_key, _)) = self.inner.current()? {
//                 if &cur_key != &key {
//                     break;
//                 }
//                 self.inner.delete_current()?;
//                 // Move to next without updating current_key
//                 if self.inner.next()?.is_none() {
//                     break;
//                 }
//             }
//         }
//         Ok(())
//     }

//     fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
//         self.inner.append(key, value)
//     }
// }

use reth_db_api::{
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    table::{Compress, Decode, Decompress, DupSort, Encode, Table},
    DatabaseError,
};
use rocksdb::{AsColumnFamilyRef, ColumnFamily, Direction, IteratorMode, ReadOptions, DB};
use std::ops::Bound;
use std::ops::RangeBounds;
use std::result::Result::Ok;
use std::sync::Arc;

use super::dupsort::DupSortHelper;

/// RocksDB cursor implementation
pub struct RocksCursor<T: Table, const WRITE: bool> {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    // Store the current key-value pair
    current_item: Option<(Box<[u8]>, Box<[u8]>)>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Table, const WRITE: bool> RocksCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone,
{
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
        let mut cursor = Self { db, cf, current_item: None, _marker: std::marker::PhantomData };

        // Position at the first item
        let _ = cursor.reset_to_first()?;

        Ok(cursor)
    }

    // Reset cursor position to the first key
    fn reset_to_first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError>
    where
        T::Value: Decompress,
    {
        // Clone the Arc references to avoid borrowing issues
        let db_clone = self.db.clone();
        let cf_clone = self.cf.clone();

        // Create a new iterator
        let mut iter = db_clone.iterator_cf_opt(
            cf_clone.as_ref(),
            ReadOptions::default(),
            IteratorMode::Start,
        );

        // Get the first item
        let next_item = iter.next();

        // Process the result
        match next_item {
            Some(Ok((k, v))) => {
                // Store the current key-value pair
                self.current_item = Some((k.clone(), v.clone()));

                // Decode and return
                Ok(Some((
                    T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                    T::Value::decompress(&v).map_err(|e| DatabaseError::Other(e.to_string()))?,
                )))
            }
            None => {
                self.current_item = None;
                Ok(None)
            }
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    // Reset cursor position to a specific key
    fn reset_to_key(
        &mut self,
        key_bytes: &[u8],
        direction: Direction,
    ) -> Result<Option<(T::Key, T::Value)>, DatabaseError>
    where
        T::Value: Decompress,
    {
        // Clone the Arc references to avoid borrowing issues
        let db_clone = self.db.clone();
        let cf_clone = self.cf.clone();

        // Create a new iterator
        let mut iter = db_clone.iterator_cf_opt(
            cf_clone.as_ref(),
            ReadOptions::default(),
            IteratorMode::From(key_bytes, direction),
        );

        // Get the next item
        let next_item = iter.next();

        // Process the result
        match next_item {
            Some(Ok((k, v))) => {
                // Store the current key-value pair
                self.current_item = Some((k.clone(), v.clone()));

                // Decode and return
                Ok(Some((
                    T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                    T::Value::decompress(&v).map_err(|e| DatabaseError::Other(e.to_string()))?,
                )))
            }
            None => {
                self.current_item = None;
                Ok(None)
            }
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    // Reset cursor position to the end
    fn reset_to_last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError>
    where
        T::Value: Decompress,
    {
        // Clone the Arc references to avoid borrowing issues
        let db_clone = self.db.clone();
        let cf_clone = self.cf.clone();

        // Create a new iterator
        let mut iter =
            db_clone.iterator_cf_opt(cf_clone.as_ref(), ReadOptions::default(), IteratorMode::End);

        // Get the next item
        let next_item = iter.next();

        // Process the result
        match next_item {
            Some(Ok((k, v))) => {
                // Store the current key-value pair
                self.current_item = Some((k.clone(), v.clone()));

                // Decode and return
                Ok(Some((
                    T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                    T::Value::decompress(&v).map_err(|e| DatabaseError::Other(e.to_string()))?,
                )))
            }
            None => {
                self.current_item = None;
                Ok(None)
            }
            Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
        }
    }

    // Move to next item after current position
    fn move_to_next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError>
    where
        T::Value: Decompress,
    {
        if let Some((current_key, _)) = &self.current_item {
            // Clone the Arc references and the current key to avoid borrowing issues
            let db_clone = self.db.clone();
            let cf_clone = self.cf.clone();
            let key_clone = current_key.clone();

            // Create a new iterator positioned at the current key
            let mut iter = db_clone.iterator_cf_opt(
                cf_clone.as_ref(),
                ReadOptions::default(),
                IteratorMode::From(&key_clone, Direction::Forward),
            );

            // Skip the current key
            let _ = iter.next();

            // Get the next item
            let next_item = iter.next();

            // Process the result
            match next_item {
                Some(Ok((k, v))) => {
                    // Store the current key-value pair
                    self.current_item = Some((k.clone(), v.clone()));

                    // Decode and return
                    Ok(Some((
                        T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                        T::Value::decompress(&v)
                            .map_err(|e| DatabaseError::Other(e.to_string()))?,
                    )))
                }
                None => {
                    self.current_item = None;
                    Ok(None)
                }
                Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
            }
        } else {
            // If no current key, start from the beginning
            self.reset_to_first()
        }
    }

    // Move to previous item before current position
    fn move_to_prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError>
    where
        T::Value: Decompress,
    {
        if let Some((current_key, _)) = &self.current_item {
            // Clone the Arc references and the current key to avoid borrowing issues
            let db_clone = self.db.clone();
            let cf_clone = self.cf.clone();
            let key_clone = current_key.clone();

            // Create a new iterator positioned at the current key in reverse direction
            let mut iter = db_clone.iterator_cf_opt(
                cf_clone.as_ref(),
                ReadOptions::default(),
                IteratorMode::From(&key_clone, Direction::Reverse),
            );

            // Skip the current key
            let _ = iter.next();

            // Get the previous item
            let next_item = iter.next();

            // Process the result
            match next_item {
                Some(Ok((k, v))) => {
                    // Store the current key-value pair
                    self.current_item = Some((k.clone(), v.clone()));

                    // Decode and return
                    Ok(Some((
                        T::Key::decode(&k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                        T::Value::decompress(&v)
                            .map_err(|e| DatabaseError::Other(e.to_string()))?,
                    )))
                }
                None => {
                    self.current_item = None;
                    Ok(None)
                }
                Some(Err(e)) => Err(DatabaseError::Other(e.to_string())),
            }
        } else {
            // If no current key, start from the end
            self.reset_to_last()
        }
    }
}

impl<T: Table, const WRITE: bool> DbCursorRO<T> for RocksCursor<T, WRITE>
where
    T::Key: Encode + Decode + Clone + PartialEq,
    T::Value: Decompress,
{
    fn first(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.reset_to_first()
    }

    fn seek_exact(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Clone the key before encoding it
        let key_clone = key.clone();
        let key_bytes = key_clone.encode();

        let result = self.reset_to_key(key_bytes.as_ref(), Direction::Forward)?;

        // Check if the found key matches exactly
        if let Some((found_key, value)) = result {
            if found_key == key {
                return Ok(Some((found_key, value)));
            }
        }

        // No exact match found
        Ok(None)
    }

    fn seek(&mut self, key: T::Key) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        // Clone the key before encoding it
        let key_clone = key.clone();
        let key_bytes = key_clone.encode();

        self.reset_to_key(key_bytes.as_ref(), Direction::Forward)
    }

    fn next(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.move_to_next()
    }

    fn prev(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.move_to_prev()
    }

    fn last(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        self.reset_to_last()
    }

    fn current(&mut self) -> Result<Option<(T::Key, T::Value)>, DatabaseError> {
        match &self.current_item {
            Some((k, v)) => Ok(Some((
                T::Key::decode(k).map_err(|e| DatabaseError::Other(e.to_string()))?,
                T::Value::decompress(v).map_err(|e| DatabaseError::Other(e.to_string()))?,
            ))),
            None => Ok(None),
        }
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
        let cf = self.cf.clone();

        db.put_cf(cf.as_ref(), key_bytes, value_bytes)
            .map_err(|e| DatabaseError::Other(e.to_string()))
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
            let cf = self.cf.clone();

            // Clone key before encoding
            let key_clone = key.clone();
            let key_bytes = key_clone.encode();

            db.delete_cf(cf.as_ref(), key_bytes)
                .map_err(|e| DatabaseError::Other(e.to_string()))?;

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
    pub(crate) fn new(db: Arc<DB>, cf: Arc<ColumnFamily>) -> Result<Self, DatabaseError> {
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
        // Clone before operating on them
        let key_clone = key.clone();
        let subkey_clone = subkey.clone();

        // Assuming T has a static method compose_key
        let composite_key_vec = DupSortHelper::create_composite_key(&key_clone, &subkey_clone);
        let encoded_key = DupSortHelper::encode_composite_key(composite_key_vec?);

        // Use the inner cursor to seek to the exact key
        let result = self.inner.seek_exact(encoded_key?)?;

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
        // match (key.clone(), subkey.clone()) {
        //     (Some(k), Some(sk)) => {
        //         let _ = self.seek_by_key_subkey(k.clone(), sk)?;
        //         self.current_key = Some(k);
        //     }
        //     (Some(k), None) => {
        //         let _ = self.seek(k.clone())?;
        //         self.current_key = Some(k);
        //     }
        //     (None, Some(_)) => {
        //         let _ = self.first()?;
        //     }
        //     (None, None) => {
        //         let _ = self.first()?;
        //     }
        // }
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
