use std::collections::{BTreeMap, HashMap};
use std::iter::Take;
use std::path::Iter;

use crate::kv::kv_store::{KVStore, Value};
use crate::utils::crypto::{Key, is_between};

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

    fn delete(&mut self, key: &Key) -> bool {
        self.map.remove(key).is_some()
    }


    fn iter(&self, lower: Key, upper: Key, left_open: bool, right_open: bool) -> Box<dyn Iterator<Item=(&Key, &Value)> + '_> {
        let keys_in_range = self.map.iter()
            .filter(move |(key, _)| is_between(**key, lower, upper, left_open, right_open))
            .into_iter();
        Box::new(keys_in_range)
    }


    fn size(&self) -> usize {
        self.map.keys().len()
    }
}
