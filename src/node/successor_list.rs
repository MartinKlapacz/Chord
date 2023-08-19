use crate::utils::types::Address;

pub const SUCCESSOR_LIST_SIZE: usize = 3;

#[derive(Default, Debug, Clone)]
pub struct SuccessorList {
    pub own_address: Address,
    pub successors: Vec<Address>,
}

impl SuccessorList {
    pub fn new(own_address: &Address, direct_successor: &Address) -> Self {
        SuccessorList {
            own_address: own_address.clone(),
            successors: vec![direct_successor.clone()],
        }
    }

    pub fn update_with_other_succ_list(&mut self, other_list: SuccessorList) -> () {
        self.successors = other_list.successors.clone();
        self.successors.insert(0, other_list.own_address.clone());
        if self.successors.len() > SUCCESSOR_LIST_SIZE {
            self.successors.pop();
        }
    }
}
