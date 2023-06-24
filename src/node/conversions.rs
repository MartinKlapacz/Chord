use crate::threads::chord::Address;
use crate::threads::chord::chord_proto::{AddressMsg, FingerEntryDebugMsg, FingerEntryMsg, KeyMsg};
use crate::node::finger_entry::FingerEntry;

use crate::utils::crypto;
use crate::utils::crypto::Key;

impl Into<FingerEntryMsg> for AddressMsg {
    fn into(self) -> FingerEntryMsg {
        FingerEntryMsg {
            id: crypto::hash(&self.address.as_bytes()).to_be_bytes().to_vec(),
            address: self.address,
        }
    }
}

impl Into<FingerEntryMsg> for &AddressMsg {
    fn into(self) -> FingerEntryMsg {
        self.clone().into()
    }
}

impl Into<AddressMsg> for FingerEntryMsg {
    fn into(self) -> AddressMsg {
        AddressMsg {
            address: self.address,
        }
    }
}

impl Into<AddressMsg> for &FingerEntryMsg {
    fn into(self) -> AddressMsg {
        self.clone().into()
    }
}

impl Into<FingerEntryMsg> for FingerEntry {
    fn into(self) -> FingerEntryMsg {
        FingerEntryMsg {
            id: self.key.to_be_bytes().to_vec(),
            address: self.address,
        }
    }
}

impl Into<FingerEntryMsg> for &FingerEntry {
    fn into(self) -> FingerEntryMsg {
        self.clone().into()
    }
}


impl Into<FingerEntry> for FingerEntryMsg {
    fn into(self) -> FingerEntry {
        FingerEntry {
            key: Key::from_be_bytes(self.id.try_into().unwrap()),
            address: self.address,
        }
    }
}

impl Into<FingerEntry> for &FingerEntryMsg {
    fn into(self) -> FingerEntry {
        self.clone().into()
    }
}


impl Into<AddressMsg> for FingerEntry {
    fn into(self) -> AddressMsg {
        AddressMsg {
            address: self.address,
        }
    }
}

impl Into<AddressMsg> for &FingerEntry {
    fn into(self) -> AddressMsg {
        self.clone().into()
    }
}


impl Into<FingerEntry> for AddressMsg {
    fn into(self) -> FingerEntry {
        FingerEntry {
            key: crypto::hash(&self.address.as_bytes()),
            address: self.address,
        }
    }
}

impl Into<FingerEntry> for &AddressMsg {
    fn into(self) -> FingerEntry {
        self.clone().into()
    }
}

impl Into<AddressMsg> for Address {
    fn into(self) -> AddressMsg {
        AddressMsg {
            address: self,
        }
    }
}

impl Into<AddressMsg> for &Address {
    fn into(self) -> AddressMsg {
        self.clone().into()
    }
}

impl Into<Address> for AddressMsg {
    fn into(self) -> Address {
        self.address
    }
}

impl Into<Address> for &AddressMsg {
    fn into(self) -> Address {
        self.clone().into()
    }
}

impl Into<KeyMsg> for AddressMsg {
    fn into(self) -> KeyMsg {
        KeyMsg {
            key: crypto::hash(&self.address.as_bytes()).to_be_bytes().to_vec()
        }
    }
}

impl Into<KeyMsg> for &AddressMsg {
    fn into(self) -> KeyMsg {
        self.clone().into()
    }
}

impl Into<KeyMsg> for Key {
    fn into(self) -> KeyMsg {
        KeyMsg {
            key: self.to_be_bytes().to_vec()
        }
    }
}

impl Into<KeyMsg> for &Key {
    fn into(self) -> KeyMsg {
        self.clone().into()
    }
}


impl Into<Key> for KeyMsg {
    fn into(self) -> Key {
        Key::from_be_bytes(self.key.try_into().unwrap())
    }
}

impl Into<Key> for &KeyMsg {
    fn into(self) -> Key {
        self.clone().into()
    }
}

impl Into<KeyMsg> for &mut FingerEntry {
    fn into(self) -> KeyMsg {
        KeyMsg {
            key: self.key.to_be_bytes().to_vec(),
        }
    }
}

impl Into<FingerEntry> for Address {
    fn into(self) -> FingerEntry {
        FingerEntry {
            key: crypto::hash(&self.as_bytes()),
            address: self,
        }
    }
}

impl Into<FingerEntry> for &Address {
    fn into(self) -> FingerEntry {
        self.clone().into()
    }
}

impl Into<Key> for FingerEntry {
    fn into(self) -> Key {
        Key::from_be_bytes(self.key.to_be_bytes())
    }
}

impl Into<Key> for &FingerEntry {
    fn into(self) -> Key {
        self.clone().into()
    }
}

impl Into<FingerEntryDebugMsg> for FingerEntry {
    fn into(self) -> FingerEntryDebugMsg {
        FingerEntryDebugMsg {
            id: self.key.to_string(),
            address: self.address,
        }
    }
}
impl Into<FingerEntryDebugMsg> for &FingerEntry {
    fn into(self) -> FingerEntryDebugMsg {
        self.clone().into()
    }
}
