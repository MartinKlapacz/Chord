use std::fmt::Debug;

use crate::threads::chord::chord_proto::FingerTableMsg;
use crate::utils::crypto::{HashRingKey};
use crate::node::finger_entry::FingerEntry;
use crate::utils::types::{Address, HashPos};

#[derive(Debug, Clone)]
pub struct FingerTable {
    pub fingers: Vec<FingerEntry>,
}

impl FingerTable {
    pub fn new(key: &HashPos) -> FingerTable {
        let mut fingers = Vec::new();
        for i in 0..HashPos::finger_count() {
            fingers.push(FingerEntry {
                // key: (key + 2u128.pow(i as u32)) % 2u128.pow(finger_count as u32),
                key: key.overflowing_add(HashPos::one().overflowing_shl(i as u32).0).0,
                address: Address::default(),
            });
        };
        FingerTable { fingers }
    }

    pub fn set_finger(&mut self, index: usize, address: Address) -> () {
        self.fingers[index].address = address;
    }

    pub fn set_all_fingers(&mut self, address: &Address) -> () {
        for mut finger in &mut self.fingers {
            finger.address = address.clone();
        }
    }

    pub fn get_successor_address(&self) -> Address {
        self.fingers[0].address.clone()
    }
}

impl Into<FingerTableMsg> for FingerTable {
    fn into(self) -> FingerTableMsg {
        let mut fingers = Vec::new();
        for finger in self.fingers {
            fingers.push(finger.into());
        }
        FingerTableMsg { fingers }
    }
}



