use std::collections::HashMap;
use std::iter::Take;
use std::path::Iter;

use crate::kv::kv_store::{KVStore, Value};
use crate::threads::chord::is_between;
use crate::utils::crypto::Key;

#[derive(Default, Debug)]
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

    fn iter(&self, lower: Key, upper: Key) -> Box<dyn Iterator<Item=(&Key, &Value)>> {
            // .filter(|(&key, &value)| is_between(key, lower, upper, true, false));
        Box::new(self.map.iter())
    }


    fn size(&self) -> usize {
        self.map.keys().len()
    }
}
