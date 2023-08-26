use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn now() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn has_expired(expiration_date: &u64) -> bool {
    now().as_secs() > expiration_date.clone()
}

