use crate::chord::NodeUrl;
use crate::crypto::Key;

static FINGER_COUNT: u32 = 32;

#[derive(Debug)]
pub struct FingerTable {
    pub fingers: Vec<FingerEntry>,
}

impl FingerTable {
    pub fn new(key: &Key) -> FingerTable {
        let mut fingers = Vec::new();
        for i in 0..FINGER_COUNT {
            fingers.push(FingerEntry {
                // key: (key + 2u128.pow(i as u32)) % 2u128.pow(finger_count as u32),
                key: key.overflowing_add(1u128.overflowing_shl(i as u32).0).0,
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


#[derive(Debug)]
pub struct FingerEntry {
    pub(crate) key: Key,
    pub(crate) url: NodeUrl,
}
