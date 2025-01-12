pub mod helpers;
pub mod traits;

use openmls_traits::storage::*;
use std::path::Path;
use std::sync::Arc;

pub struct LmdbStorage {
    env: Arc<heed::Env>,
}
/// Errors thrown by the key store.
#[derive(thiserror::Error, Debug)]
pub enum LmdbStorageError {
    #[error("LMDB error: {0}")]
    LmdbError(#[from] heed::Error),
    #[error("Serialization error")]
    SerializationError,
    #[error("Value does not exist.")]
    None,
}

impl From<serde_json::Error> for LmdbStorageError {
    fn from(_: serde_json::Error) -> Self {
        Self::SerializationError
    }
}

impl LmdbStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, LmdbStorageError> {
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024) // 100MB
                .max_dbs(1)
                .open(path)?
        };
        Ok(Self { env: Arc::new(env) })
    }

    pub fn write_txn(&self) -> Result<heed::RwTxn, LmdbStorageError> {
        Ok(self.env.write_txn()?)
    }

    pub fn read_txn(&self) -> Result<heed::RoTxn, LmdbStorageError> {
        Ok(self.env.read_txn()?)
    }

    pub fn readable_db(
        &self,
    ) -> Result<
        (
            heed::RoTxn,
            heed::Database<heed::types::Bytes, heed::types::Bytes>,
        ),
        LmdbStorageError,
    > {
        let rtxn = self.read_txn()?;
        let db = self.env.open_database(&rtxn, None)?;
        if db.is_none() {
            return Err(LmdbStorageError::None);
        }
        Ok((rtxn, db.unwrap()))
    }

    pub fn writable_db(
        &self,
    ) -> Result<
        (
            heed::RwTxn,
            heed::Database<heed::types::Bytes, heed::types::Bytes>,
        ),
        LmdbStorageError,
    > {
        let mut wtxn = self.write_txn()?;
        let db = self.env.create_database(&mut wtxn, None)?;
        Ok((wtxn, db))
    }

    pub fn delete_all_data(&self) -> Result<(), LmdbStorageError> {
        let (mut wtxn, db) = self.writable_db()?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Writes a value to the storage with the given prefix, key, and value.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be stored.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a LmdbStorageError.
    #[inline(always)]
    fn write<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (mut wtxn, db) = self.writable_db()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        db.put(&mut wtxn, &compound_key, &value)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Appends a value to a list stored at the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be appended.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a LmdbStorageError.
    fn append<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (mut wtxn, db) = self.writable_db()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());

        let list_bytes = db.get(&wtxn, &compound_key)?;
        let mut list: Vec<Vec<u8>> = Vec::new();
        if let Some(list_bytes) = list_bytes {
            list = serde_json::from_slice(list_bytes)?;
        }

        list.push(value);
        let updated_list_bytes = serde_json::to_vec(&list)?;
        db.put(&mut wtxn, &compound_key, &updated_list_bytes)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Reads a value from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with the value (if found) or a `LmdbStorageError`.
    #[inline(always)]
    fn read<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<Option<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (rtxn, db) = self.readable_db()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        match db.get(&rtxn, &compound_key) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => {
                // Deserialize directly to V, without the intermediate Vec<u8>
                Ok(Some(serde_json::from_slice(value)?))
            }
            Err(e) => Err(LmdbStorageError::LmdbError(e)),
        }
    }

    /// Reads a list of entities from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of entities or a LmdbStorageError.
    #[inline(always)]
    fn read_list<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<Vec<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (rtxn, db) = self.readable_db()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let value: Vec<Vec<u8>> = match db.get(&rtxn, &compound_key) {
            Ok(Some(list_bytes)) => serde_json::from_slice(list_bytes)?,
            Ok(None) => vec![],
            Err(e) => return Err(LmdbStorageError::LmdbError(e)),
        };

        value
            .iter()
            .map(|value_bytes| serde_json::from_slice(value_bytes))
            .collect::<Result<Vec<V>, _>>()
            .map_err(|_| LmdbStorageError::SerializationError)
    }

    /// Removes a specific item from a list stored at the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be removed.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a LmdbStorageError.
    fn remove_item<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (mut wtxn, db) = self.writable_db()?;
        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        let list = match db.get(&wtxn, &compound_key) {
            Ok(Some(list)) => list,
            Ok(None) => return Ok(()),
            Err(e) => return Err(LmdbStorageError::LmdbError(e)),
        };

        let mut parsed_list: Vec<Vec<u8>> = serde_json::from_slice(list)?;
        if let Some(pos) = parsed_list
            .iter()
            .position(|stored_item| stored_item == &value)
        {
            parsed_list.remove(pos);
        }

        let updated_list_bytes = serde_json::to_vec(&parsed_list)?;
        db.put(&mut wtxn, &compound_key, &updated_list_bytes)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Deletes an entry from the storage with the given prefix and key.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the storage entry.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a LmdbStorageError.
    #[inline(always)]
    fn delete<const VERSION: u16>(
        &self,
        prefix: &[u8],
        key: &[u8],
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let (mut wtxn, db) = self.writable_db()?;

        let compound_key = helpers::build_key_from_vec::<VERSION>(prefix, key.to_vec());
        db.delete(&mut wtxn, &compound_key)?;
        wtxn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const CURRENT_VERSION: u16 = 1; // Assuming CURRENT_VERSION is 1, adjust if needed

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        data: String,
    }

    impl Entity<CURRENT_VERSION> for TestEntity {}

    fn setup_storage() -> LmdbStorage {
        let dir = tempdir().unwrap();
        LmdbStorage::new(dir.path()).unwrap()
    }

    #[test]
    fn test_new() {
        let dir = tempdir().unwrap();
        let storage = LmdbStorage::new(dir.path()).unwrap();
        let (txn, db) = storage.readable_db().unwrap();
        assert!(db.stat(&txn).unwrap().entries == 0);
    }

    #[test]
    fn test_write_and_read() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        let serialized = serde_json::to_vec(&value).unwrap();
        println!(
            "Serialized value: {:?}",
            String::from_utf8_lossy(&serialized)
        );

        let write_result = storage.write::<CURRENT_VERSION>(prefix, key, serialized);
        assert!(write_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);

        // Debug the actual value we got back
        match &read_result {
            Ok(Some(v)) => println!("Read value: {:?}", v),
            Ok(None) => println!("Got None!"),
            Err(e) => println!("Got error: {:?}", e),
        }

        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), Some(value));
    }

    #[test]
    fn test_append_and_read_list() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            let append_result =
                storage.append::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(value).unwrap());
            assert!(append_result.is_ok());
        }

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), values);
    }

    #[test]
    fn test_remove_item() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            storage
                .append::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(value).unwrap())
                .unwrap();
        }

        let remove_result = storage.remove_item::<CURRENT_VERSION>(
            prefix,
            key,
            serde_json::to_vec(&values[0]).unwrap(),
        );
        assert!(remove_result.is_ok());

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![values[1].clone()]);
    }

    #[test]
    fn test_delete() {
        let storage = setup_storage();
        let prefix = b"test_prefix";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        storage
            .write::<CURRENT_VERSION>(prefix, key, serde_json::to_vec(&value).unwrap())
            .unwrap();

        let delete_result = storage.delete::<CURRENT_VERSION>(prefix, key);
        assert!(delete_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(prefix, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), None);
    }
}
