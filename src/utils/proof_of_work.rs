use std::fmt;
use crate::utils::constants::POW_TOKEN_LIVE_TIME;
use crate::utils::time::{has_expired, now};
use crate::utils::constants::DIFFICULTY;
use crate::utils::crypto::hash;

#[derive(Default)]
struct PowToken {
    timestamp: u64,
    nonce: u64
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
        hash(self.serialize().as_slice()).to_be_bytes().iter().take(DIFFICULTY).all(|&x| x == 0)
    }

    fn has_expired(&self, ) -> bool {
        let expiration_time = self.timestamp + POW_TOKEN_LIVE_TIME;
        has_expired(&expiration_time)
    }

    pub fn validate(&self, ) -> bool {
        !self.has_expired() || self.check_trailing_zeros()
    }

    pub fn new() -> Self {
        let mut token =  PowToken {
            timestamp: now().as_secs(),
            nonce: 0,
        };
        while !token.check_trailing_zeros() {
            token.nonce = token.nonce.overflowing_add(1).0;
            if token.has_expired() {

                token = PowToken::new()
            }
        }
        token
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let token = PowToken::new();
        token.validate();
    }
}

