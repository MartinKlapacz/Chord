use blake3::Hasher;

pub type Key = u128;

pub fn hash(input: String) -> Key {
    let mut hasher = Hasher::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    let bytes = *hash.as_bytes();
    u128::from_le_bytes(bytes[0..16].try_into().unwrap())
}
