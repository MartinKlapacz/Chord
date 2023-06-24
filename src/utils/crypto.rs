use std::hash::Hash;
use std::mem;
use std::mem::size_of;

use blake3::Hasher;

pub type Key = u8;


pub trait HashRingKey {
    fn size() -> usize;
    fn one() -> Key;
    fn two() -> Key;
    fn finger_count() -> usize;
}

impl HashRingKey for Key {
    fn size() -> usize {
        mem::size_of::<Key>()
    }

    fn one() -> Key {
        Key::default() + 1
    }

    fn two() -> Key {
        Key::default() + 2
    }

    fn finger_count() -> usize {
        Key::size() * 8
    }
}

pub fn hash(input: &[u8]) -> Key {
    let mut hasher = Hasher::new();
    hasher.update(input);
    let hash = hasher.finalize();
    let bytes = *hash.as_bytes();
    Key::from_le_bytes(bytes[0..Key::size()].try_into().unwrap())
}
