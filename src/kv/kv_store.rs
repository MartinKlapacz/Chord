use std::fmt::Debug;

use crate::utils::crypto::{HashPos, Key};

pub type Value = String;

pub trait KVStore : Debug {
    fn get(&self, key: &Key) -> Option<&Value>;
    fn put(&mut self, key: &Key, value: &Value) -> bool;
    fn delete(&mut self, key: &Key) -> bool;
    fn iter(&self, lower: HashPos, upper: HashPos, left_open: bool, right_open: bool) -> Box<dyn Iterator<Item=(&Key, &Value)> + '_>;
    fn size(&self) -> usize;
}

