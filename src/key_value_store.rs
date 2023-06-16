use crate::crypto::Key;

type Value = String;

pub trait KVStore {
    fn get(&self, key: &Key) -> Option<Value>;
    fn put(&self, key: &Key, value: &Value) -> bool;
    fn range(&self, key: &Key) -> Vec<Key>;
}
