use std::fmt::{Debug, Formatter};
use std::io::Read;
use std::mem;

use crate::chord::chord_proto::{FingerEntryMsg, FingerInfoMsg, FingerTableMsg};
use crate::chord::NodeUrl;
use crate::crypto;
use crate::crypto::{HashRingKey, Key};

#[derive(Debug, Clone)]
pub struct FingerTable {
    pub fingers: Vec<FingerEntry>,
}

#[derive(Debug, Clone)]
pub struct FingerEntry {
    pub(crate) key: Key,
    pub(crate) url: NodeUrl,
}

impl FingerEntry {
    pub fn new(key: &Key, url: &NodeUrl) -> Self {
        FingerEntry {
            url: url.clone(),
            key: key.clone(),
        }
    }
}

// impl From<&NodeUrl> for FingerEntry {
//     fn from(url: &NodeUrl) -> Self {
//         FingerEntry {
//             url: url.clone(),
//             key: crypto::hash(url)
//         }
//     }
// }

impl From<(&NodeUrl, &Key)> for FingerEntry {
    fn from((url, key): (&NodeUrl, &Key)) -> Self {
        FingerEntry {
            url: url.clone(),
            key: key.clone(),
        }
    }
}

impl Into<FingerInfoMsg> for FingerEntry {
    fn into(self) -> FingerInfoMsg {
        let id = self.key.to_be_bytes().iter()
            .map(|byte| byte.to_string())
            .collect::<Vec<String>>()
            .join(" ");
        FingerInfoMsg { id, url: self.url }
    }
}


// impl From<&FingerEntryMsg> for FingerEntry {
//     fn from(finger_entry: &FingerEntryMsg) -> Self {
//         let mut bytes = [0u8; 16];
//         bytes.copy_from_slice(finger_entry.key.clone().as_slice());
//         FingerEntry {
//             key: u128::from_le_bytes(bytes),
//             url: finger_entry.url.clone()
//         }
//     }
// }

impl Into<FingerEntry> for FingerEntryMsg {
    fn into(self) -> FingerEntry {
        let mut bytes = [0u8; mem::size_of::<Key>()];
        bytes.copy_from_slice(self.key.clone().as_slice());
        FingerEntry {
            key: Key::from_le_bytes(bytes),
            url: self.url.clone(),
        }
    }
}

impl Into<FingerEntryMsg> for FingerEntry {
    fn into(self) -> FingerEntryMsg {
        FingerEntryMsg {
            url: self.url.clone(),
            key: self.key.to_be_bytes().to_vec(),
        }
    }
}

impl FingerTable {
    pub fn new(key: &Key) -> FingerTable {
        let mut fingers = Vec::new();
        for i in 0..Key::finger_count() {
            fingers.push(FingerEntry {
                // key: (key + 2u128.pow(i as u32)) % 2u128.pow(finger_count as u32),
                key: key.overflowing_add(Key::one().overflowing_shl(i as u32).0).0,
                url: NodeUrl::default(),
            });
        };
        FingerTable { fingers }
    }

    pub fn set_finger(&mut self, index: usize, url: NodeUrl) -> () {
        self.fingers[index].url = url;
    }

    pub fn set_all_fingers(&mut self, url: &NodeUrl) -> () {
        for mut finger in &mut self.fingers {
            finger.url = url.clone();
        }
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



