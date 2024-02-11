use std::collections::HashMap;

// todo: change to u128
pub type HashPos = u64;

pub type ExpirationDate = u64;

pub type Key = [u8; 32];
pub type Value = String;

pub type Address = String;
pub type KvStore = HashMap<Key, (Value, u64)>;

