use std::fmt::Debug;

use crate::utils::crypto::{HashRingKey};
use crate::node::finger_entry::FingerEntry;
use crate::utils::types::{Address, HashPos};


/// The data structure that contains the routing information used for efficient node look up

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

}


