use crate::{
    tables::trie::{AccountTrieTable, StorageTrieTable, TrieNibbles, TrieNodeValue, TrieTable},
    RocksTransaction,
};
use alloy_primitives::{keccak256, Address, B256};
use reth_db::transaction::{DbTx, DbTxMut};
use reth_db_api::cursor::{DbCursorRO, DbDupCursorRO, DbDupCursorRW};
use reth_db_api::table::Table;
use reth_trie::{BranchNodeCompact, Nibbles, StoredNibbles, TrieMask};
use rocksdb::{Options, DB};
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_db() -> (Arc<DB>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    // create options
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Define column families
    let cf_names = vec![TrieTable::NAME, AccountTrieTable::NAME, StorageTrieTable::NAME];

    // create column family descriptor
    let cf_descriptors = cf_names
        .iter()
        .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect::<Vec<_>>();

    // Open the Database with column families
    let db = DB::open_cf_descriptors(&opts, path, cf_descriptors).unwrap();

    (Arc::new(db), temp_dir)
}

fn create_trie_node_value(nibbles_str: &str, node_hash: B256) -> TrieNodeValue {
    let nibbles = Nibbles::from_nibbles(
        &nibbles_str.chars().map(|c| c.to_digit(16).unwrap() as u8).collect::<Vec<_>>(),
    );

    TrieNodeValue { nibbles: StoredNibbles(nibbles), node: node_hash }
}

fn create_test_branch_node() -> BranchNodeCompact {
    let state_mask = TrieMask::new(0);
    let tree_mask = TrieMask::new(0);
    let hash_mask = TrieMask::new(0);
    let hashes = Vec::new();
    let root_hash = Some(B256::from([1; 32]));

    BranchNodeCompact::new(state_mask, tree_mask, hash_mask, hashes, root_hash)
}

#[test]
fn test_put_get_account_trie_node() {
    let (db, _temp_dir) = create_test_db();

    // Creating a Writable txn <WRITE: true>
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Creating dummy nibbles (key)
    let nibbles = Nibbles::from_nibbles(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let key = TrieNibbles(nibbles);

    // Creating dummy value
    let value = create_test_branch_node();

    // Putting k-v pair into the db
    tx.put::<AccountTrieTable>(key.clone(), value.clone()).unwrap();

    // Committing the transaction
    tx.commit().unwrap();

    // Creating a Read txn <WRITE: false>
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);

    // Getting the value from the db
    let stored_val = read_tx.get::<AccountTrieTable>(key.clone()).unwrap();

    // Verifying the value
    assert!(stored_val.is_some());
    assert_eq!(value, stored_val.unwrap());
}

#[test]
fn test_put_get_storage_trie_node() {
    let (db, _temp_dir) = create_test_db();

    // Create a writable txn
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Creating test account and hash it
    let address = Address::from([1; 20]);
    let address_hash = keccak256(address);

    // Create a test storage key (nibbles)
    let storage_nibbles = Nibbles::from_nibbles(&[5, 6, 7, 8, 9]);
    let storage_key = StoredNibbles(storage_nibbles.clone());

    // Create s test node hash
    let node_hash = B256::from([1; 32]);

    // Creating a test val
    let val = TrieNodeValue { nibbles: storage_key.clone(), node: node_hash };

    // Put the key-value pair into the database
    let mut cursor = tx.cursor_dup_write::<StorageTrieTable>().unwrap();
    cursor.seek_exact(address_hash).unwrap();
    cursor.append_dup(address_hash, val.clone()).unwrap();

    // Commit the transaction
    drop(cursor);
    tx.commit().unwrap();

    // Create a read transaction
    let read_tx = RocksTransaction::<false>::new(db, false);

    // Try to get the value back
    let mut read_cursor = read_tx.cursor_dup_read::<StorageTrieTable>().unwrap();
    let result = read_cursor.seek_by_key_subkey(address_hash, storage_key).unwrap();

    // Verify that the retrieved value matches the original
    assert!(result.is_some());

    let retrieved_value = result.unwrap();
    assert_eq!(retrieved_value.node, node_hash);
    assert_eq!(retrieved_value.nibbles.0, storage_nibbles);
}

#[test]
fn test_cursor_navigation() {
    let (db, _temp_dir) = create_test_db();

    // Creating a Writable txn <WRITE: true>
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Insert multiple account trie nodes
    let mut keys = Vec::new();
    let mut values = Vec::new();

    for i in 0..5 {
        let nibbles = Nibbles::from_nibbles(&[i, i + 1, i + 2, i + 3, i + 4]);
        let key = TrieNibbles(nibbles);
        keys.push(key.clone());

        let value = create_test_branch_node();
        values.push(value.clone());

        tx.put::<AccountTrieTable>(key, value).unwrap();
    }

    // Commit the txn
    tx.commit().unwrap();

    // Creating a read txn
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);

    // Test cursor navigation
    let mut cursor = read_tx.cursor_read::<AccountTrieTable>().unwrap();

    // Test first()
    let first = cursor.first().unwrap();
    assert!(first.is_some());
    assert_eq!(keys[0], first.as_ref().unwrap().0);

    // Test next()
    let mut next = cursor.next().unwrap();
    assert!(next.is_some());
    assert_eq!(keys[1], next.as_ref().unwrap().0);

    // Test seek()
    let seek = cursor.seek(keys[3].clone()).unwrap();
    assert!(seek.is_some());
    assert_eq!(keys[3], seek.as_ref().unwrap().0);

    // Test seek_exact()
    let seek_exact = cursor.seek_exact(keys[4].clone()).unwrap();
    assert!(seek_exact.is_some());
    assert_eq!(seek_exact.as_ref().unwrap().0, keys[4]);

    // Test last()
    let last = cursor.last().unwrap();
    assert!(last.is_some());
    assert_eq!(last.as_ref().unwrap().0, keys[4]);
}

#[test]
fn test_delete_account_trie_node() {
    let (db, _temp_dir) = create_test_db();

    // Create writable txn
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Creating test key and vals
    let nibbles = Nibbles::from_nibbles(&[1, 2, 3, 4]);
    let key = TrieNibbles(nibbles);
    let val = create_test_branch_node();

    // Insert k-v pair
    tx.put::<AccountTrieTable>(key.clone(), val.clone()).unwrap();
    tx.commit().unwrap();

    // Verify if it is there
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);
    assert!(read_tx.get::<AccountTrieTable>(key.clone()).unwrap().is_some());
    assert_eq!(read_tx.get::<AccountTrieTable>(key.clone()).unwrap().unwrap(), val);

    // Delete the k-v pair
    let delete_tx = RocksTransaction::<true>::new(db.clone(), true);
    delete_tx.delete::<AccountTrieTable>(key.clone(), None).unwrap();
    delete_tx.commit().unwrap();

    // Verify if it's gone
    let verify_tx = RocksTransaction::<false>::new(db.clone(), false);
    assert!(verify_tx.get::<AccountTrieTable>(key).unwrap().is_none());
}

#[test]
fn test_empty_values() {
    let (db, _temp_dir) = create_test_db();

    // Create writable tx
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Create a key
    let nibbles = Nibbles::from_nibbles(&[1, 2, 3, 4, 5, 6]);
    let key = TrieNibbles(nibbles);
    let empty_val = BranchNodeCompact::new(
        TrieMask::new(0),
        TrieMask::new(0),
        TrieMask::new(0),
        Vec::new(),
        None,
    );

    // Insert an empty value for the account
    tx.put::<AccountTrieTable>(key.clone(), empty_val.clone()).unwrap();
    tx.commit().unwrap();

    // Verify we can retrieve it
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);
    let result = read_tx.get::<AccountTrieTable>(key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), empty_val);
}

#[test]
fn test_transaction_abort() {
    let (db, _temp_dir) = create_test_db();

    // Create a writable transaction
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Create test key and value
    let nibbles = Nibbles::from_nibbles(&[9, 8, 7, 6, 5]);
    let key = TrieNibbles(nibbles);
    let value = create_test_branch_node();

    // Insert the key-value pair
    tx.put::<AccountTrieTable>(key.clone(), value.clone()).unwrap();

    // Abort the transaction instead of committing
    tx.abort();

    // Verify the data was not persisted
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);
    assert!(read_tx.get::<AccountTrieTable>(key.clone()).unwrap().is_none());
}

#[test]
fn test_large_keys_and_values() {
    let (db, _temp_dir) = create_test_db();

    // Create a writable transaction
    let tx = RocksTransaction::<true>::new(db.clone(), true);

    // Create a large key (many nibbles)
    let mut nibble_vec = Vec::new();
    for i in 0..100 {
        nibble_vec.push(i % 16);
    }
    let large_nibbles = Nibbles::from_nibbles(&nibble_vec);
    let large_key = TrieNibbles(large_nibbles);

    // Create a value with many hash entries
    let state_mask = TrieMask::new(0xFFFF); // All bits set
    let tree_mask = TrieMask::new(0xFFFF);
    let hash_mask = TrieMask::new(0xFFFF);

    // Generate lots of hashes
    let mut hashes = Vec::new();
    for i in 0..16 {
        hashes.push(B256::from([i as u8; 32]));
    }

    let large_node = BranchNodeCompact::new(
        state_mask,
        tree_mask,
        hash_mask,
        hashes,
        Some(B256::from([255; 32])),
    );

    // Insert the large key-value pair
    tx.put::<AccountTrieTable>(large_key.clone(), large_node.clone()).unwrap();
    tx.commit().unwrap();

    // Verify we can retrieve it correctly
    let read_tx = RocksTransaction::<false>::new(db.clone(), false);
    let result = read_tx.get::<AccountTrieTable>(large_key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), large_node);
}

// fn test_dupsort_cursor_navigation() {
//     let (db, _temp_dir) = create_test_db();

//     // Create a writable transaction
//     let tx = RocksTransaction::<true>::new(db.clone(), true);

//     // Create a test account hash
//     let account_hash = B256::from([1; 32]);

//     // Insert multiple storage trie nodes for the same account
//     let mut subkeys = Vec::new();
//     let mut values = Vec::new();

//     for i in 0..5 {
//         let nibbles = Nibbles::from_nibbles(&[i, i + 1, i + 2]);
//         let subkey = StoredNibbles(nibbles);
//         subkeys.push(subkey.clone());

//         let node_hash = B256::from([1; 32]);
//         let value = TrieNodeValue { nibbles: subkey.clone(), node: node_hash };
//         values.push(value.clone());

//         let mut cursor = tx.cursor_dup_write::<StorageTrieTable>().unwrap();
//         cursor.append_dup(account_hash, value).unwrap();
//     }

//     // Commit the transaction
//     tx.commit().unwrap();

//     // Create a read transaction
//     let read_tx = RocksTransaction::<false>::new(db, false);

//     // Test dupsort cursor navigation
//     let mut cursor = read_tx.cursor_dup_read::<StorageTrieTable>().unwrap();

//     // Seek to the account hash
//     let seek_result = cursor.seek(account_hash).unwrap();
//     assert!(seek_result.is_some());

//     // Test next_dup() to iterate through all values for this key
//     let mut count = 0;
//     while cursor.next_dup().unwrap().is_some() {
//         count += 1;
//     }

//     // We should have found (n-1) more entries (n total minus the one we already got with seek)
//     assert_eq!(count, 4);

//     // Test seek_by_key_subkey()
//     for (i, subkey) in subkeys.iter().enumerate() {
//         let result = cursor.seek_by_key_subkey(account_hash, subkey.clone()).unwrap();
//         assert!(result.is_some());

//         let retrieved_value = result.unwrap();
//         assert_eq!(retrieved_value.nibbles.0, subkey.0);
//         assert_eq!(retrieved_value.node, values[i].node);
//     }
// }
