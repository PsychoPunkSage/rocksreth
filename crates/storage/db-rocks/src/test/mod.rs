#[cfg(test)]
mod tests {
    use super::*;
    use reth_db_api::{
        cursor::{DbCursorRO, DbCursorRW},
        database::Database,
        table::Table,
    };
    use reth_primitives::{Address, B256, U256};
    use tempfile::TempDir;

    // Define a simple test table
    #[derive(Debug)]
    struct TestTable;

    impl Table for TestTable {
        const NAME: &'static str = "test_table";
        const DUPSORT: bool = false;
        type Key = Address;
        type Value = U256;
    }

    #[test]
    fn test_basic_operations() {
        // Create temporary directory for RocksDB
        let temp_dir = TempDir::new().unwrap();

        // Initialize database
        let config = RocksDBConfig {
            path: temp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let db = DatabaseEnv::open(temp_dir.path(), config).unwrap();

        // Test data
        let key = Address::random();
        let value = U256::from(1234);

        // Write test
        db.update(|tx| {
            tx.put::<TestTable>(key, value).unwrap();
            println!("Written value: {:?}", value);
        })
        .unwrap();

        // Read test
        let read_value = db.view(|tx| tx.get::<TestTable>(key).unwrap()).unwrap();

        assert_eq!(read_value, Some(value));
        println!("Read value: {:?}", read_value);

        // Test cursor operations
        db.update(|tx| {
            let mut cursor = tx.cursor_write::<TestTable>().unwrap();

            // Write multiple values
            for i in 0..5 {
                let key = Address::random();
                let value = U256::from(i);
                cursor.put(key, &value).unwrap();
            }
        })
        .unwrap();

        // Read through cursor
        db.view(|tx| {
            let mut cursor = tx.cursor_read::<TestTable>().unwrap();
            let mut count = 0;

            while let Some(Ok(_)) = cursor.next() {
                count += 1;
            }
            assert_eq!(count, 6); // 5 new + 1 original
            println!("Total entries: {}", count);
        })
        .unwrap();
    }
}
