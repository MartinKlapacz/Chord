use crate::node::finger_entry::FingerEntry;
use crate::node::successor_list::SuccessorList;
use crate::threads::chord::chord_proto::{AddressMsg, FingerEntryDebugMsg, FingerEntryMsg, HashPosMsg, PowTokenMsg, SuccessorListMsg};
use crate::utils::crypto;
use crate::utils::proof_of_work::PowToken;
use crate::utils::types::{Address, HashPos};

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
            key: HashPos::from_be_bytes(self.id.try_into().unwrap()),
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

impl Into<HashPosMsg> for AddressMsg {
    fn into(self) -> HashPosMsg {
        HashPosMsg {
            key: crypto::hash(&self.address.as_bytes()).to_be_bytes().to_vec()
        }
    }
}

impl Into<HashPosMsg> for &AddressMsg {
    fn into(self) -> HashPosMsg {
        self.clone().into()
    }
}

impl Into<HashPosMsg> for HashPos {
    fn into(self) -> HashPosMsg {
        HashPosMsg {
            key: self.to_be_bytes().to_vec()
        }
    }
}

impl Into<HashPosMsg> for &HashPos {
    fn into(self) -> HashPosMsg {
        self.clone().into()
    }
}


impl Into<HashPos> for HashPosMsg {
    fn into(self) -> HashPos {
        HashPos::from_be_bytes(self.key.try_into().unwrap())
    }
}

impl Into<HashPos> for &HashPosMsg {
    fn into(self) -> HashPos {
        self.clone().into()
    }
}

impl Into<HashPosMsg> for &mut FingerEntry {
    fn into(self) -> HashPosMsg {
        HashPosMsg {
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

impl Into<HashPos> for FingerEntry {
    fn into(self) -> HashPos {
        HashPos::from_be_bytes(self.key.to_be_bytes())
    }
}

impl Into<HashPos> for &FingerEntry {
    fn into(self) -> HashPos {
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

impl Into<SuccessorListMsg> for SuccessorList {
    fn into(self) -> SuccessorListMsg {
        SuccessorListMsg {
            own_address: Some(self.own_address.into()),
            successors: self.successors.iter().map(|succ| succ.into()).collect(),
        }
    }
}

impl Into<SuccessorList> for SuccessorListMsg {
    fn into(self) -> SuccessorList {
        SuccessorList {
            own_address: self.own_address.unwrap().into(),
            successors: self.successors.iter().map(|succ| succ.into()).collect(),
        }
    }
}

impl Into<PowTokenMsg> for PowToken {
    fn into(self) -> PowTokenMsg {
        PowTokenMsg {
            timestamp: self.timestamp,
            nonce: self.nonce,
            pow_difficulty: self.pow_difficulty as u32
        }
    }
}

impl Into<PowToken> for PowTokenMsg {
    fn into(self) -> PowToken {
        PowToken {
            timestamp: self.timestamp,
            nonce: self.nonce,
            pow_difficulty: self.pow_difficulty as usize
        }
    }
}
