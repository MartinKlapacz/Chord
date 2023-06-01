use std::fmt::{Debug, Formatter};
use std::io::Read;
use std::mem;

use crate::chord::chord_proto::{FingerInfoMsg, FingerTableMsg, NodeInfo, NodeMsg};
use crate::chord::{ChordService, NodeUrl};
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

impl Into<FingerEntry> for NodeMsg {
    fn into(self) -> FingerEntry {
        FingerEntry {
            key: crypto::hash(&self.url),
            url: self.url,
        }
    }
}

impl Into<FingerEntry> for &NodeMsg {
    fn into(self) -> FingerEntry {
        self.clone().into()
    }
}

impl Into<NodeMsg> for NodeUrl {
    fn into(self) -> NodeMsg {
        NodeMsg {
            url: self
        }
    }
}

impl Into<NodeMsg> for &NodeUrl {
    fn into(self) -> NodeMsg {
        self.clone().into()
    }
}



impl Into<FingerEntry> for String {
    fn into(self) -> FingerEntry {
        FingerEntry {
            key: crypto::hash(&self),
            url: self,
        }
    }
}


impl Into<FingerEntry> for &String {
    fn into(self) -> FingerEntry {
        self.clone().into()
    }
}

impl Into<NodeMsg> for FingerEntry {
    fn into(self) -> NodeMsg {
        NodeMsg { url: self.url }
    }
}


impl Into<NodeMsg> for &FingerEntry {
    fn into(self) -> NodeMsg {
        self.clone().into()
    }
}


impl Into<Key> for NodeMsg {
    fn into(self) -> Key {
        crypto::hash(&self.url)
    }
}

impl Into<Key> for &NodeMsg {
    fn into(self) -> Key {
        self.clone().into()
    }
}

impl Into<FingerInfoMsg> for FingerEntry {
    fn into(self) -> FingerInfoMsg {
        FingerInfoMsg {
            id: self.key.to_string(),
            url: self.url,
        }
    }
}



impl From<(&NodeUrl, &Key)> for FingerEntry {
    fn from((url, key): (&NodeUrl, &Key)) -> Self {
        FingerEntry {
            url: url.clone(),
            key: key.clone(),
        }
    }
}


impl FingerTable {
    pub fn new(key: &Key, url: &NodeUrl) -> FingerTable {
        let mut fingers = Vec::new();
        for i in 0..Key::finger_count() {
            fingers.push(FingerEntry {
                // key: (key + 2u128.pow(i as u32)) % 2u128.pow(finger_count as u32),
                key: key.overflowing_add(Key::one().overflowing_shl(i as u32).0).0,
                url: url.clone(),
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



