#[cfg(test)]
mod rocks_cursor_test {
    use crate::test::utils::create_test_db; // Replace with the correct module path where `create_test_db` is defined
    use crate::{implementation::rocks::trie::RocksHashedCursorFactory, Account, RocksTransaction};
    use alloy_primitives::{keccak256, Address, B256, U256};
    use reth_db::{
        cursor::DbCursorRO,
        transaction::{DbTx, DbTxMut},
        HashedAccounts,
    };
    use reth_trie::hashed_cursor::{HashedCursor, HashedCursorFactory};
    use std::collections::BTreeMap;

    #[test]
    fn test_rocks_cursor_basic() {
        let (db, _temp_dir) = create_test_db();

        // Create a write transaction and insert some test data
        let write_tx = RocksTransaction::<true>::new(db.clone(), true);

        // Create test keys and values
        let key1 = B256::from([1; 32]);
        let key2 = B256::from([2; 32]);

        let value1 = Account {
            nonce: 1,
            balance: U256::from(1000),
            bytecode_hash: Some(B256::from([1; 32])),
        };

        let value2 = Account {
            nonce: 2,
            balance: U256::from(2000),
            bytecode_hash: Some(B256::from([2; 32])),
        };

        // Insert data
        write_tx.put::<HashedAccounts>(key1, value1.clone()).unwrap();
        write_tx.put::<HashedAccounts>(key2, value2.clone()).unwrap();

        // Commit transaction
        write_tx.commit().unwrap();

        // Test with a read transaction
        let read_tx = RocksTransaction::<false>::new(db.clone(), false);

        // Get a cursor directly
        let mut cursor = read_tx.cursor_read::<HashedAccounts>().unwrap();

        // Test first()
        let first = cursor.first().unwrap();
        println!("First result: {:?}", first);
        assert!(first.is_some(), "Failed to get first item");

        // Test next()
        let next = cursor.next().unwrap();
        println!("Next result: {:?}", next);
        assert!(next.is_some(), "Failed to get next item");
    }

    #[test]
    fn test_rocks_cursor_comprehensive() {
        let (db, _temp_dir) = create_test_db();

        // Create a write transaction
        let write_tx = RocksTransaction::<true>::new(db.clone(), true);

        // Create multiple test keys and values
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut data_map = BTreeMap::new();

        // Create 10 entries with sequential keys for predictable ordering
        for i in 1..=10 {
            let key = B256::from([i as u8; 32]);
            let value = Account {
                nonce: i,
                balance: U256::from(i * 1000),
                bytecode_hash: Some(B256::from([i as u8; 32])),
            };

            keys.push(key);
            values.push(value.clone());
            data_map.insert(key, value.clone());

            // Insert into database
            write_tx.put::<HashedAccounts>(key, value).unwrap();
        }

        // Commit transaction
        write_tx.commit().unwrap();

        // Test with a read transaction
        let read_tx = RocksTransaction::<false>::new(db.clone(), false);

        // Get a cursor
        let mut cursor = read_tx.cursor_read::<HashedAccounts>().unwrap();

        // Test first()
        let first = cursor.first().unwrap();
        assert!(first.is_some(), "Failed to get first item");
        let (first_key, first_value) = first.unwrap();
        assert_eq!(first_key, keys[0], "First key doesn't match expected value");
        assert_eq!(first_value.nonce, values[0].nonce, "First value doesn't match expected value");

        // Test current() after first()
        let current = cursor.current().unwrap();
        assert!(current.is_some(), "Failed to get current item after first()");
        let (current_key, current_value) = current.unwrap();
        assert_eq!(current_key, keys[0], "Current key after first() doesn't match");
        assert_eq!(
            current_value.nonce, values[0].nonce,
            "Current value after first() doesn't match"
        );

        // Test next() multiple times
        for i in 1..10 {
            let next = cursor.next().unwrap();
            assert!(next.is_some(), "Failed to get next item at index {}", i);
            let (next_key, next_value) = next.unwrap();
            assert_eq!(next_key, keys[i], "Next key at index {} doesn't match", i);
            assert_eq!(
                next_value.nonce, values[i].nonce,
                "Next value at index {} doesn't match",
                i
            );
        }

        // Test next() at the end should return None
        let beyond_end = cursor.next().unwrap();
        assert!(beyond_end.is_none(), "Next() should return None when beyond the end");

        // Test last()
        let last = cursor.last().unwrap();
        assert!(last.is_some(), "Failed to get last item");
        let (last_key, last_value) = last.unwrap();
        assert_eq!(last_key, keys[9], "Last key doesn't match expected value");
        assert_eq!(last_value.nonce, values[9].nonce, "Last value doesn't match expected value");

        // Test current() after last()
        let current = cursor.current().unwrap();
        assert!(current.is_some(), "Failed to get current item after last()");
        let (current_key, current_value) = current.unwrap();
        assert_eq!(current_key, keys[9], "Current key after last() doesn't match");
        assert_eq!(
            current_value.nonce, values[9].nonce,
            "Current value after last() doesn't match"
        );

        // Test prev() multiple times from the end
        for i in (0..9).rev() {
            let prev = cursor.prev().unwrap();
            assert!(prev.is_some(), "Failed to get prev item at index {}", i);
            let (prev_key, prev_value) = prev.unwrap();
            assert_eq!(prev_key, keys[i], "Prev key at index {} doesn't match", i);
            assert_eq!(
                prev_value.nonce, values[i].nonce,
                "Prev value at index {} doesn't match",
                i
            );
        }

        // Test prev() at the beginning should return None
        let before_start = cursor.prev().unwrap();
        assert!(before_start.is_none(), "Prev() should return None when before the start");

        // Test seek_exact() for existing keys
        for i in 0..10 {
            let seek_result = cursor.seek_exact(keys[i]).unwrap();
            assert!(seek_result.is_some(), "Failed to seek_exact to key at index {}", i);
            let (seek_key, seek_value) = seek_result.unwrap();
            assert_eq!(seek_key, keys[i], "Seek_exact key at index {} doesn't match", i);
            assert_eq!(
                seek_value.nonce, values[i].nonce,
                "Seek_exact value at index {} doesn't match",
                i
            );
        }

        // Test seek_exact() for non-existent key
        let non_existent_key = B256::from([42u8; 32]);
        let seek_result = cursor.seek_exact(non_existent_key).unwrap();
        assert!(seek_result.is_none(), "Seek_exact should return None for non-existent key");

        // Test seek() for existing keys
        for i in 0..10 {
            let seek_result = cursor.seek(keys[i]).unwrap();
            assert!(seek_result.is_some(), "Failed to seek to key at index {}", i);
            let (seek_key, seek_value) = seek_result.unwrap();
            assert_eq!(seek_key, keys[i], "Seek key at index {} doesn't match", i);
            assert_eq!(
                seek_value.nonce, values[i].nonce,
                "Seek value at index {} doesn't match",
                i
            );
        }

        // Test seek() for a key that should place us at the start of a range
        let before_all = B256::from([0u8; 32]);
        let seek_result = cursor.seek(before_all).unwrap();
        assert!(seek_result.is_some(), "Failed to seek to key before all");
        let (seek_key, seek_value) = seek_result.unwrap();
        assert_eq!(seek_key, keys[0], "Seek key for 'before all' test doesn't match first key");
        assert_eq!(
            seek_value.nonce, values[0].nonce,
            "Seek value for 'before all' test doesn't match first value"
        );

        // Test seek() for a key that should place us in the middle of the range
        let mid_key = B256::from([5u8; 32]);
        let seek_result = cursor.seek(mid_key).unwrap();
        assert!(seek_result.is_some(), "Failed to seek to middle key");
        let (seek_key, seek_value) = seek_result.unwrap();
        assert_eq!(seek_key, keys[4], "Seek key for 'middle' test doesn't match expected key");
        assert_eq!(
            seek_value.nonce, values[4].nonce,
            "Seek value for 'middle' test doesn't match expected value"
        );

        // Test seek() for a key that should place us at the end of the range
        let after_all = B256::from([11u8; 32]);
        let seek_result = cursor.seek(after_all).unwrap();
        assert!(seek_result.is_none(), "Seek should return None for key beyond all");

        // Test navigation after seek
        cursor.seek(keys[5]).unwrap();

        // Test next() after seek
        let next_after_seek = cursor.next().unwrap();
        assert!(next_after_seek.is_some(), "Failed to get next after seek");
        let (next_key, next_value) = next_after_seek.unwrap();
        assert_eq!(next_key, keys[6], "Next key after seek doesn't match");
        assert_eq!(next_value.nonce, values[6].nonce, "Next value after seek doesn't match");

        // Test prev() after seek and next
        let prev_after_next = cursor.prev().unwrap();
        assert!(prev_after_next.is_some(), "Failed to get prev after next");
        let (prev_key, prev_value) = prev_after_next.unwrap();
        assert_eq!(prev_key, keys[5], "Prev key after next doesn't match");
        assert_eq!(prev_value.nonce, values[5].nonce, "Prev value after next doesn't match");

        // Test that cursor position is properly maintained through operations
        cursor.first().unwrap();
        cursor.next().unwrap();
        cursor.next().unwrap();
        let current = cursor.current().unwrap().unwrap();
        assert_eq!(current.0, keys[2], "Current key doesn't match after navigation sequence");
    }

    // Test cursor behavior with empty database
    #[test]
    fn test_rocks_cursor_empty_db() {
        let (db, _temp_dir) = create_test_db();
        let read_tx = RocksTransaction::<false>::new(db.clone(), false);
        let mut cursor = read_tx.cursor_read::<HashedAccounts>().unwrap();

        // Test first() on empty database
        let first = cursor.first().unwrap();
        assert!(first.is_none(), "First() should return None on empty database");

        // Test last() on empty database
        let last = cursor.last().unwrap();
        assert!(last.is_none(), "Last() should return None on empty database");

        // Test current() on empty database
        let current = cursor.current().unwrap();
        assert!(current.is_none(), "Current() should return None on empty database");

        // Test seek() on empty database
        let key = B256::from([1u8; 32]);
        let seek_result = cursor.seek(key).unwrap();
        assert!(seek_result.is_none(), "Seek() should return None on empty database");

        // Test seek_exact() on empty database
        let seek_exact_result = cursor.seek_exact(key).unwrap();
        assert!(seek_exact_result.is_none(), "Seek_exact() should return None on empty database");
    }

    // Test cursor with a database containing a single entry
    #[test]
    fn test_rocks_cursor_single_entry() {
        let (db, _temp_dir) = create_test_db();

        // Create a write transaction and insert one test entry
        let write_tx = RocksTransaction::<true>::new(db.clone(), true);
        let key = B256::from([1u8; 32]);
        let value = Account {
            nonce: 1,
            balance: U256::from(1000),
            bytecode_hash: Some(B256::from([1u8; 32])),
        };
        write_tx.put::<HashedAccounts>(key, value.clone()).unwrap();
        write_tx.commit().unwrap();

        // Test with a read transaction
        let read_tx = RocksTransaction::<false>::new(db.clone(), false);
        let mut cursor = read_tx.cursor_read::<HashedAccounts>().unwrap();

        // Test first() with single entry
        let first = cursor.first().unwrap();
        assert!(first.is_some(), "Failed to get first item on single-entry database");
        let (first_key, first_value) = first.unwrap();
        assert_eq!(
            first_key, key,
            "First key doesn't match expected value on single-entry database"
        );
        assert_eq!(
            first_value.nonce, value.nonce,
            "First value doesn't match expected value on single-entry database"
        );

        // Test last() with single entry
        let last = cursor.last().unwrap();
        assert!(last.is_some(), "Failed to get last item on single-entry database");
        let (last_key, last_value) = last.unwrap();
        assert_eq!(last_key, key, "Last key doesn't match expected value on single-entry database");
        assert_eq!(
            last_value.nonce, value.nonce,
            "Last value doesn't match expected value on single-entry database"
        );

        // Test next() after first() should return None
        cursor.first().unwrap();
        let next = cursor.next().unwrap();
        assert!(next.is_none(), "Next() after first() should return None on single-entry database");

        // Test prev() after last() should return None
        cursor.last().unwrap();
        let prev = cursor.prev().unwrap();
        assert!(prev.is_none(), "Prev() after last() should return None on single-entry database");
    }

    #[test]
    fn test_rocks_hashed_account_cursor() {
        let (db, _temp_dir) = create_test_db();

        // Create a write transaction and insert some test accounts
        let write_tx = RocksTransaction::<true>::new(db.clone(), true);

        // Create test accounts
        let addr1 = keccak256(Address::from([1; 20]));
        let addr2 = keccak256(Address::from([2; 20]));
        let addr3 = keccak256(Address::from([3; 20]));

        println!("Test account addresses: {:?}, {:?}", addr1, addr2);

        let account1 = Account {
            nonce: 1,
            balance: U256::from(1000),
            bytecode_hash: Some(B256::from([1; 32])),
        };
        let account2 = Account {
            nonce: 2,
            balance: U256::from(2000),
            bytecode_hash: Some(B256::from([2; 32])),
        };
        let account3 = Account {
            nonce: 3,
            balance: U256::from(3000),
            bytecode_hash: Some(B256::from([3; 32])),
        };

        println!("Inserting test accounts");

        // Insert accounts into HashedAccounts table
        write_tx.put::<HashedAccounts>(addr1, account1.clone()).unwrap();
        write_tx.put::<HashedAccounts>(addr2, account2.clone()).unwrap();
        write_tx.put::<HashedAccounts>(addr3, account3.clone()).unwrap();

        // Commit transaction
        write_tx.commit().unwrap();

        println!("Transaction committed");

        // Verify accounts were stored
        let verify_tx = RocksTransaction::<false>::new(db.clone(), false);

        let acct1 = verify_tx.get::<HashedAccounts>(addr1).unwrap();
        let acct2 = verify_tx.get::<HashedAccounts>(addr2).unwrap();
        let acct3 = verify_tx.get::<HashedAccounts>(addr3).unwrap();

        println!(
            "Verification: \n>Account1: \n  -{:?}, \n>Account2: \n  -{:?} \n>Account3: \n  -{:?}",
            acct1, acct2, acct3
        );

        // Create a read transaction to test the cursor
        let read_tx = RocksTransaction::<false>::new(db.clone(), false);

        // Create and test hashed account cursor
        let hashed_factory = RocksHashedCursorFactory::new(&read_tx);
        let mut account_cursor = hashed_factory.hashed_account_cursor().unwrap();

        // Test seek
        println!("\nTesting seek()...");

        let result = account_cursor.seek(addr1).unwrap();
        println!("Seek result(acct1): \n  -{:?}", result);
        assert!(result.is_some(), "Failed to seek account");

        let result = account_cursor.seek(addr2).unwrap();
        println!("Seek result(acct2): \n  -{:?}", result);
        assert!(result.is_some(), "Failed to seek account");

        let result = account_cursor.seek(addr3).unwrap();
        println!("Seek result(acct3): \n  -{:?}", result);
        assert!(result.is_some(), "Failed to seek account");

        let (found_addr, found_account) = result.unwrap();
        assert_eq!(found_addr, addr3, "Found wrong account address");
        assert_eq!(found_account.nonce, account3.nonce, "Account nonce mismatch");

        // Test next
        println!("\nTesting next()...");

        let next_result = account_cursor.next().unwrap();

        println!("Next result: \n  -{:?}", next_result);
        assert!(next_result.is_some(), "Failed to get next account");

        let (next_addr, next_account) = next_result.unwrap();

        assert_eq!(next_addr, addr2, "Found wrong next account address");
        assert_eq!(next_account.nonce, account2.nonce, "Next account nonce mismatch");

        let next_result = account_cursor.next().unwrap();

        println!("Next result: \n  -{:?}", next_result);
        assert!(next_result.is_some(), "Failed to get next account");

        let (next_addr, next_account) = next_result.unwrap();

        assert_eq!(next_addr, addr1, "Found wrong next account address");
        assert_eq!(next_account.nonce, account1.nonce, "Next account nonce mismatch");

        let next_result = account_cursor.next().unwrap();

        println!("Next result: \n  -{:?}", next_result);
        assert!(next_result.is_none(), "Failed to get next account");
    }
}
