use std::collections::{BTreeMap, HashMap};
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

    fn iter(&mut self, lower: Key, upper: Key) -> Box<dyn Iterator<Item=(Key, Value)>> {

        let keys_in_range = self.map.iter()
            // .filter(|(k, _v)| is_between(**k, lower, upper, true, false))
            .map(|(k, _v)| k.clone())
            .collect::<Vec<Key>>();

        let values_in_range = keys_in_range.iter().map(|k| {
            self.map.remove(k).expect("Key not found")
        }).collect::<Vec<Value>>();

        Box::new(keys_in_range.into_iter().zip(values_in_range))
    }


    fn size(&self) -> usize {
        self.map.keys().len()
    }
}
