use std::fmt::Debug;



// pub trait KVStore: Debug {
//     fn get(&self, key: &Key) -> Option<&Value>;
//     fn put(&mut self, key: &Key, value: &Value) -> bool;
//     fn delete(&mut self, key: &Key) -> bool;
//     fn iter(&self, lower: HashPos, upper: HashPos) -> Box<dyn Iterator<Item=(&Key, &Value)> + '_>;
//     fn iter_full(&self) -> Box<dyn Iterator<Item=(&Key, &Value)> + '_> {
//         self.iter(HashPos::MIN + 1, HashPos::MAX)
//     }
//
//     fn size(&self) -> usize;
// }
//
