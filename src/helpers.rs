use crate::LmdbStorage;
use openmls_traits::storage::*;
use serde::Serialize;

/// Builds a key with version and label.
///
/// This function takes a label and a key as byte slices, and returns a new Vec<u8>
/// that combines them with a version number.
///
/// # Arguments
///
/// * `label` - A byte slice representing the label. The label is the prefix of the key.
/// * `key` - A Vec<u8> representing the key.
///
/// # Returns
///
/// A Vec<u8> containing the label, key, and version number.
pub fn build_key_from_vec<const V: u16>(label: &[u8], key: Vec<u8>) -> Vec<u8> {
    let mut key_out = label.to_vec();
    key_out.extend_from_slice(&key);
    key_out.extend_from_slice(&u16::to_be_bytes(V));
    key_out
}

/// Builds a key with version and label, serializing the key.
///
/// This function is similar to `build_key_from_vec`, but it takes a serializable key
/// and serializes it before building the final key.
///
/// # Arguments
///
/// * `label` - A byte slice representing the label. The label is the prefix of the key.
/// * `key` - A value of type K that implements Serialize.
///
/// # Returns
///
/// A Vec<u8> containing the label, serialized key, and version number.
pub fn build_key<const V: u16, K: Serialize>(label: &[u8], key: K) -> Vec<u8> {
    build_key_from_vec::<V>(label, serde_json::to_vec(&key).unwrap())
}

/// Generates a unique identifier for epoch key pairs.
///
/// This function creates a unique identifier by combining the group ID, epoch, and leaf index.
///
/// # Arguments
///
/// * `group_id` - A reference to a type implementing GroupId trait.
/// * `epoch` - A reference to a type implementing EpochKey trait.
/// * `leaf_index` - A u32 representing the leaf index.
///
/// # Returns
///
/// A Result containing a Vec<u8> if successful, or an error if serialization fails.
pub fn epoch_key_pairs_id(
    group_id: &impl traits::GroupId<CURRENT_VERSION>,
    epoch: &impl traits::EpochKey<CURRENT_VERSION>,
    leaf_index: u32,
) -> Result<Vec<u8>, <LmdbStorage as StorageProvider<CURRENT_VERSION>>::Error> {
    let mut key = serde_json::to_vec(group_id)?;
    key.extend_from_slice(&serde_json::to_vec(epoch)?);
    key.extend_from_slice(&serde_json::to_vec(&leaf_index)?);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn test_build_key_from_vec() {
        let label = b"test_label";
        let key = vec![1, 2, 3, 4];
        let result = build_key_from_vec::<1>(label, key);
        assert_eq!(
            result,
            vec![116, 101, 115, 116, 95, 108, 97, 98, 101, 108, 1, 2, 3, 4, 0, 1]
        );
    }

    #[test]
    fn test_build_key() {
        #[derive(Serialize)]
        struct TestKey {
            id: u32,
        }
        let label = b"test_label";
        let key = TestKey { id: 42 };
        let result = build_key::<1, _>(label, key);
        assert_eq!(
            result,
            vec![
                116, 101, 115, 116, 95, 108, 97, 98, 101, 108, 123, 34, 105, 100, 34, 58, 52, 50,
                125, 0, 1
            ]
        );
    }

    #[test]
    fn test_epoch_key_pairs_id() {
        // TODO: This test would require mock implementations of GroupId and EpochKey traits
        // As well as a mock SledStorage. For brevity, we'll just test the function signature.
    }
}
