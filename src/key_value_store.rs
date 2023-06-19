use std::collections::HashMap;
use std::iter::Take;

use crate::crypto::Key;

type Value = String;

pub struct HandOverIterator<I: Iterator> {
    iter: Take<I>,
    limit: Key,
}

impl<I: Iterator> Iterator for HandOverIterator<I> {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some((key, val)) => {
                    if key < self.limit {
                        return Some(val)
                    } else {
                        None
                    }
                }
                None => return None
            }
        }
    }
}

pub trait KVStore {
    fn get(&self, key: &Key) -> Option<Value>;
    fn put(&mut self, key: &Key, value: &Value) -> bool;
    fn iter(&self, limit: Key) -> HandOverIterator<I> { HandOverIterator { iter: (), limit } }
    fn get_base_iter(&self, ) -> dyn Iterator<Item=()>;
}


pub struct HashMapStore {
    map: HashMap<Key, Value>,
}


impl KVStore for HashMapStore {
    fn get(&self, key: &Key) -> Option<Value> {
        match self.map.get(key) {
            Some(&value) => Some(value),
            None => None
        }
    }

    fn put(&mut self, key: &Key, value: &Value) -> bool {
        let exists = self.map.contains_key(key);
        self.map.insert(key.clone(), value.clone());
        exists
    }
}
