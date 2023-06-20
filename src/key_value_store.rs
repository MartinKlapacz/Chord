use std::collections::HashMap;
use std::iter::Take;
use std::path::Iter;

use crate::crypto::Key;

type Value = String;

pub trait KVStore {
    fn get(&self, key: &Key) -> Option<&Value>;
    fn put(&mut self, key: &Key, value: &Value) -> bool;
    fn iter(&self, limit: Key) -> Take<Iter>;
}


#[derive(Default)]
pub struct HashMapStore {
    map: HashMap<Key, Value>,
}


impl KVStore for HashMapStore {
    fn get(&self, key: &Key) -> Option<&Value> {
        self.map.get(key)
    }

    fn put(&mut self, key: &Key, value: &Value) -> bool {
        let exists = self.map.contains_key(key);
        self.map.insert(key.clone(), value.clone());
        exists
    }

    fn iter(&self, limit: Key) -> Take<Iter> {
        todo!()
    }
}
