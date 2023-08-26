use std::fmt;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use log::{debug, info};
use crate::utils::constants::{POW_THREAD_NUM, POW_TOKEN_LIVE_TIME};
use crate::utils::time::{has_expired, now};
use crate::utils::constants::POW_DIFFICULTY;
use crate::utils::crypto::hash;

extern crate rayon;
use rayon::prelude::*;

#[derive(Default, Clone)]
pub struct PowToken {
    pub timestamp: u64,
    pub nonce: u64
}

impl fmt::Display for PowToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = hash(self.serialize().as_slice()).to_be_bytes();
        write!(f, "PowToken {{ timestamp: {}, nonce: {}, {:?} }}", self.timestamp, self.nonce, bytes)
    }
}


impl PowToken {

    fn serialize(&self, ) -> Vec<u8> {
        let mut bytes = self.timestamp.to_be_bytes().to_vec();
        bytes.extend_from_slice(&self.nonce.to_be_bytes());
        bytes
    }

    fn check_trailing_zeros(&self) -> bool {
        hash(self.serialize().as_slice()).to_be_bytes().iter().take(POW_DIFFICULTY).all(|&x| x == 0)
    }

    fn has_expired(&self, ) -> bool {
        let expiration_time = self.timestamp + POW_TOKEN_LIVE_TIME;
        has_expired(&expiration_time)
    }

    pub fn validate(&self, ) -> (bool, bool) {
        (self.has_expired(), self.check_trailing_zeros())
    }

    pub fn new() -> Self {
        let timestamp = now().as_secs();
        let token = Arc::new(Mutex::new(PowToken { timestamp, nonce: 0 }));
        let found = Arc::new(AtomicBool::new(false));

        let start = now().as_millis();
        rayon::scope(|s| {
            for i in 0..POW_THREAD_NUM {
                let token_clone = Arc::clone(&token);
                let found_clone = Arc::clone(&found);

                s.spawn(move |_| {
                    let mut local_token = PowToken { timestamp, nonce: i as u64 };

                    while !found_clone.load(Ordering::Relaxed) {
                        if local_token.check_trailing_zeros() {
                            let mut shared_token = token_clone.lock().unwrap();
                            *shared_token = local_token.clone();
                            found_clone.store(true, Ordering::Relaxed);
                            break;
                        }
                        local_token.nonce += POW_THREAD_NUM as u64;

                        if local_token.has_expired() {
                            break;
                        }
                    }
                });
            }
        });

        debug!("Found pow_token withing {} milliseconds", now().as_millis() - start);

        let final_token = token.lock().unwrap();
        final_token.clone()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let token = PowToken::new();
        println!("{}", token);
    }
}

