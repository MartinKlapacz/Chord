use std::collections::HashMap;
use std::iter::Take;
use std::path::Iter;

use crate::utils::crypto::Key;

pub type Value = String;

pub trait KVStore {
    fn get(&self, key: &Key) -> Option<&Value>;
    fn put(&mut self, key: &Key, value: &Value) -> bool;
    fn iter(&self, limit: Key) -> Take<Iter>;
    fn size(&self) -> usize;
}

