use std::fmt::{Debug, Pointer};
use std::fmt;

use crate::threads::chord::Address;
use crate::threads::chord::chord_proto::{FingerEntryDebugMsg, FingerEntryMsg, FingerTableMsg, HashPosMsg};
use crate::utils::crypto::HashPos;


#[derive(Clone, Default)]
pub struct FingerEntry {
    pub(crate) key: HashPos,
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
    pub fn new(key: &HashPos, address: &Address) -> Self {
        FingerEntry {
            address: address.clone(),
            key: key.clone(),
        }
    }

    pub fn get_key(&self) -> &HashPos {
        &self.key
    }

    pub fn get_address(&self) -> &Address {
        &self.address
    }

    pub fn get_address_mut(&mut self) -> &mut Address {
        &mut self.address
    }
}
