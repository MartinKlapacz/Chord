use std::mem::size_of;

use blake3::Hasher;

pub type HashPos = u8;
pub type Key = [u8; 32];


pub trait HashRingKey {
    fn size() -> usize;
    fn one() -> HashPos;
    fn two() -> HashPos;
    fn finger_count() -> usize;
}

impl HashRingKey for HashPos {
    fn size() -> usize {
        size_of::<HashPos>()
    }

    fn one() -> HashPos {
        HashPos::default() + 1
    }

    fn two() -> HashPos {
        HashPos::default() + 2
    }

    fn finger_count() -> usize {
        HashPos::size() * 8
    }
}

pub fn hash(input: &[u8]) -> HashPos {
    let mut hasher = Hasher::new();
    hasher.update(input);
    let hash = hasher.finalize();
    let bytes = *hash.as_bytes();
    HashPos::from_le_bytes(bytes[0..HashPos::size()].try_into().unwrap())
}

pub fn is_between(pos: HashPos, lower: HashPos, upper: HashPos, left_open: bool, right_open: bool) -> bool {
    if lower < upper {
        if left_open && right_open {
            return lower < pos && pos < upper;
        } else if left_open {
            return lower < pos && pos <= upper;
        } else if right_open {
            return lower <= pos && pos < upper;
        } else {
            return lower <= pos && pos <= upper;
        }
    } else if lower > upper {
        if left_open && right_open {
            return lower < pos || pos < upper;
        } else if left_open {
            return lower < pos || pos <= upper;
        } else if right_open {
            return lower <= pos || pos < upper;
        } else {
            return lower <= pos || pos <= upper;
        }
    } else {
        return !left_open && pos == lower;
    }
}

