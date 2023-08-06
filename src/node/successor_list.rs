use crate::threads::chord::Address;

const SUCCESSOR_LIST_SIZE: usize = 3;

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



#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test1() {
        let mut l1 = SuccessorList::new(&"foo".to_string(), &"ba1".to_string());
        l1.successors.push("bar2".to_string());
        l1.successors.push("bar3".to_string());

        let mut l2 = SuccessorList::new(&"foo2".to_string(), &"ba1".to_string());
        l2.successors.push("bar2".to_string());
        l2.successors.push("bar3".to_string());

        l1.update_with_other_succ_list(l2);

        print!("{:?}", l1)
    }

}
