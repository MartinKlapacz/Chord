use std::error::Error;
use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::{AddressMsg, Data, Empty, FingerEntryMsg, FingerTableMsg, KeyMsg, NodeSummaryMsg, UpdateFingerTableEntryRequest};
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::crypto;
use crate::crypto::Key;
use crate::finger_table::{FingerEntry, FingerTable};

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type Address = String;

#[derive(Debug)]
pub struct ChordService {
    address: String,
    pos: Key,
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor: Arc<Mutex<FingerEntry>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<(FingerTable, FingerEntry)>, url: &String) -> ChordService {
        let (finger_table, predecessor) = rx.await.unwrap();
        ChordService {
            address: url.clone(),
            pos: crypto::hash(&url),
            finger_table: Arc::new(Mutex::new(finger_table)),
            predecessor: Arc::new(Mutex::new(predecessor)),
        }
    }

    pub fn is_successor_of_key(&self, key: Key) -> bool {
        let predecessor_position = self.predecessor.lock().unwrap().key.clone();
        let own_position = self.pos.clone();

        if predecessor_position < own_position {
            return predecessor_position < key && key <= own_position;
        } else if predecessor_position > own_position {
            return predecessor_position < key || key <= own_position;
        } else {
            return true;
        }
    }
}

pub fn is_between(key: Key, lower: Key, upper: Key, left_open: bool, right_open: bool) -> bool {
    if lower < upper {
        if left_open && right_open {
            return lower < key && key < upper;
        } else if left_open {
            return lower < key && key <= upper;
        } else if right_open {
            return lower <= key && key < upper;
        } else {
            return lower <= key && key <= upper;
        }
    } else if lower > upper {
        if left_open && right_open {
            return lower < key || key < upper;
        } else if left_open {
            return lower < key || key <= upper;
        } else if right_open {
            return lower <= key || key < upper;
        } else {
            return lower <= key || key <= upper;
        }
    } else {
        return !left_open && key == lower;
    }
}


#[tonic::async_trait]
impl chord_proto::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord_proto::KeyMsg>,
    ) -> Result<Response<chord_proto::AddressMsg>, Status> {
        let key_msg: &KeyMsg = request.get_ref();

        let successor_finger_entry: FingerEntry = match self.is_successor_of_key(key_msg.into()) {
            true => self.address.clone().into(),
            false => {
                let find_predecessor_response = self.find_predecessor(Request::new(request.get_ref().clone()))
                    .await
                    .unwrap();
                let mut predecessor_client = ChordClient::connect(format!("http://{}", find_predecessor_response.get_ref().address))
                    .await
                    .unwrap();
                let successor_of_key_predecessor_response = predecessor_client.get_direct_successor(Request::new(Empty {}))
                    .await
                    .unwrap();
                successor_of_key_predecessor_response.get_ref().address.clone().into()
            }
        };
        info!("Received find_successor call for {:?}, successor is {:?}", key_msg, successor_finger_entry);
        Ok(Response::new(successor_finger_entry.into()))
    }


    async fn find_predecessor(&self, request: Request<KeyMsg>) -> Result<Response<AddressMsg>, Status> {
        let look_up_key = Key::from_be_bytes(request.get_ref().key.clone().try_into().unwrap());
        let mut current_address_finger_entry: FingerEntry = self.address.clone().into();
        let mut current_successor_finger_entry: FingerEntry = {
            let finger_table_guard = self.finger_table.lock().unwrap();
            finger_table_guard.fingers[0].clone()
        };

        let mut current_key: Key = current_address_finger_entry.clone().into();
        let mut current_successor_key: Key = crypto::hash(&current_successor_finger_entry.clone().address);

        while !is_between(look_up_key, current_key, current_successor_key, true, false) {
            let mut current_client = ChordClient::connect(format!("http://{}", current_successor_finger_entry.address))
                .await
                .unwrap();
            let response = current_client.find_closest_preceding_finger(Request::new(KeyMsg {
                key: look_up_key.to_be_bytes().to_vec(),
            })).await.unwrap();
            current_address_finger_entry = response.get_ref().clone().into();
            current_key = current_address_finger_entry.key;

            current_client = ChordClient::connect(format!("http://{}", current_address_finger_entry.address))
                .await
                .unwrap();
            let response = current_client.get_direct_successor(Request::new(Empty {}))
                .await
                .unwrap();
            current_successor_finger_entry = response.get_ref().into();
            current_successor_key = current_successor_finger_entry.key;
        }

        Ok(Response::new(current_address_finger_entry.into()))
    }

    async fn get_predecessor(&self, _request: Request<Empty>) -> Result<Response<AddressMsg>, Status> {
        let finger_entry = self.predecessor.lock().unwrap();
        info!("Received get predecessor call, predecessor is {:?}", finger_entry);
        Ok(Response::new(finger_entry.clone().into()))
    }

    async fn set_predecessor(&self, request: Request<AddressMsg>) -> Result<Response<Data>, Status> {
        let new_predecessor: FingerEntry = request.get_ref().into();

        info!("Setting predecessor to {:?}", new_predecessor);
        let mut predecessor = self.predecessor.lock().unwrap();
        *predecessor = new_predecessor;
        Ok(Response::new(Data {}))
    }

    async fn get_direct_successor(&self, _: Request<Empty>) -> Result<Response<AddressMsg>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap().fingers[0].clone();
        Ok(Response::new(finger_table_guard.into()))
    }

    async fn update_finger_table_entry(&self, request: Request<UpdateFingerTableEntryRequest>) -> Result<Response<Empty>, Status> {
        let index_to_update = request.get_ref().index as usize;
        let finger_entry_update: &FingerEntry = &(request.get_ref().clone().finger_entry.unwrap().into());
        let finger_entry_update_key: Key = finger_entry_update.into();

        let predecessor_address_str = {
            let predecessor_guard = self.predecessor.lock().unwrap();
            predecessor_guard.address.clone()
        };
        let upper_finger = {
            let finger_table_guard = self.finger_table.lock().unwrap();
            finger_table_guard.fingers[index_to_update].clone()
        };


        let upper_key = crypto::hash(&upper_finger.address);
        if is_between(finger_entry_update_key, self.pos, upper_key, false, true)
            || self.pos == upper_key {
            info!("Updating finger entry {} with {:?}", index_to_update, finger_entry_update);
            {
                let mut finger_table_guard = self.finger_table.lock().unwrap();
                finger_table_guard.fingers[index_to_update].address = finger_entry_update.address.clone();
            }

            if predecessor_address_str.ne(self.address.as_str()) {
                let mut predecessor_to_update_client = ChordClient::connect(format!("http://{}", predecessor_address_str))
                    .await
                    .unwrap();
                let _ = predecessor_to_update_client.update_finger_table_entry(Request::new(UpdateFingerTableEntryRequest {
                    index: index_to_update as u32,
                    finger_entry: Some(finger_entry_update.into()),
                })).await.unwrap();
            }
        }

        Ok(Response::new(Empty {}))
    }

    async fn find_closest_preceding_finger(&self, request: Request<KeyMsg>) -> Result<Response<FingerEntryMsg>, Status> {
        let key = Key::from_be_bytes(request.get_ref().clone().key.try_into().unwrap());
        for finger in self.finger_table.lock().unwrap().fingers.iter().rev() {
            if is_between(finger.key, self.pos, key, false, false) {
                return Ok(Response::new(FingerEntryMsg {
                    id: finger.key.to_be_bytes().to_vec(),
                    address: finger.clone().address.into(),
                }));
            }
        }
        Ok(Response::new(FingerEntryMsg {
            id: self.pos.to_be_bytes().to_vec(),
            address: self.address.clone().into(),
        }))
    }


    async fn get_node_summary(&self, _: Request<Empty>) -> Result<Response<NodeSummaryMsg>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap();
        let predecessor = self.predecessor.lock().unwrap().clone();

        Ok(Response::new(NodeSummaryMsg {
            url: self.address.clone(),
            id: self.pos.to_be_bytes().iter().map(|byte| byte.to_string()).collect::<Vec<String>>().join(" "),
            predecessor: Some(predecessor.into()),
            finger_entries: finger_table_guard.fingers.iter()
                .map(|finger| finger.clone())
                .map(|finger| finger.into())
                .collect(),
        }))
    }
}
