use tonic::transport::Channel;
use crate::threads::chord::{Address, connect_with_retry};
use crate::threads::chord::chord_proto::chord_client::ChordClient;

pub const SUCCESSOR_LIST_SIZE: usize = 3;

#[derive(Default, Debug, Clone)]
pub struct SuccessorList {
    pub own_address: Address,
    pub successors: Vec<Address>
}

impl SuccessorList {

    pub fn new(own_address: &Address, direct_successor: &Address) -> Self {
        SuccessorList {
            own_address: own_address.clone(),
            successors: vec![direct_successor.clone()]
        }
    }

    pub fn update_with_other_succ_list(&mut self, other_list: SuccessorList) -> () {
        self.successors = vec![other_list.own_address.clone()];
        if other_list.successors.len() == 1 {
            return;
        }
        for (i, successor) in other_list.successors.iter().enumerate() {
            if self.successors.len() > SUCCESSOR_LIST_SIZE {
                break
            }
            self.successors.push(successor.clone());
            if self.own_address.eq(successor) {
                break;
            }
        }
    }
    

}

