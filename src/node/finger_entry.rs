use std::fmt::{Debug, Pointer};
use std::fmt;

use crate::chord::Address;
use crate::chord::chord_proto::{FingerEntryDebugMsg, FingerEntryMsg, FingerTableMsg, KeyMsg};
use crate::utils::crypto::Key;


#[derive(Clone)]
pub struct FingerEntry {
    pub(crate) key: Key,
    pub(crate) address: Address,
}

impl Debug for FingerEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("")
            .field("key", &self.key)
            .field("address", &self.address)
            .finish()
    }
}

impl FingerEntry {
    pub fn new(key: &Key, address: &Address) -> Self {
        FingerEntry {
            address: address.clone(),
            key: key.clone(),
        }
    }

    pub fn get_key(&self) -> &Key {
        &self.key
    }

    pub fn get_address(&self) -> &Address {
        &self.address
    }

    pub fn get_address_mut(&mut self) -> &mut Address {
        &mut self.address
    }
}
