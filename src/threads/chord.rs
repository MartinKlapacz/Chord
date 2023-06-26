use std::error::Error;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Receiver;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::kv::kv_store::KVStore;
use crate::node::finger_entry::FingerEntry;
use crate::node::finger_table::FingerTable;
use crate::threads::chord::chord_proto::{AddressMsg, Data, Empty, FingerEntryMsg, FingerTableMsg, GetKvStoreSizeResponse, GetResponse, GetStatus, KeyMsg, KvPairMsg, NodeSummaryMsg, PutRequest, UpdateFingerTableEntryRequest};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::utils::crypto::{HashRingKey, Key};
use crate::utils::crypto;

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type Address = String;

pub struct ChordService {
    address: String,
    pos: Key,
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor: Arc<Mutex<FingerEntry>>,
    kv_store_arc: Arc<Mutex<dyn KVStore + Send>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<(Arc<Mutex<FingerTable>>, FingerEntry, Arc<Mutex<dyn KVStore + Send>>)>, url: &String) -> ChordService {
        let (finger_table, predecessor, kv_store) = rx.await.unwrap();
        ChordService {
            address: url.clone(),
            pos: crypto::hash(&url.as_bytes()),
            finger_table,
            predecessor: Arc::new(Mutex::new(predecessor)),
            kv_store_arc: kv_store,
        }
    }

    pub fn is_successor_of_key(&self, key: Key) -> bool {
        let predecessor_position = self.predecessor.lock().unwrap().get_key().clone();
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

        // current
        let mut current_address: Address = self.address.clone();
        let mut current_key: Key = crypto::hash(&current_address.as_bytes());

        // successor
        let mut current_successor_address: Address = {
            let finger_table_guard = self.finger_table.lock().unwrap();
            finger_table_guard.fingers[0].get_address().clone()
        };
        let mut current_successor_key: Key = crypto::hash(&current_successor_address.as_bytes());


        while (!is_between(look_up_key, current_key, current_successor_key, true, false)) && current_key != current_successor_key {
            let mut current_client = ChordClient::connect(format!("http://{}", &current_successor_address))
                .await
                .unwrap();
            let response = current_client.find_closest_preceding_finger(Request::new(KeyMsg {
                key: look_up_key.to_be_bytes().to_vec(),
            })).await.unwrap();

            // update current
            current_address = response.get_ref().address.clone();
            current_key = Key::from_be_bytes(response.get_ref().id.clone().try_into().unwrap());

            current_client = ChordClient::connect(format!("http://{}", current_address))
                .await
                .unwrap();
            let response = current_client.get_direct_successor(Request::new(Empty {}))
                .await
                .unwrap();

            // update successor
            current_successor_address = response.get_ref().into();
            current_successor_key = crypto::hash(&current_successor_address.as_bytes());
        }

        Ok(Response::new(current_address.into()))
    }

    async fn get_predecessor(&self, _request: Request<Empty>) -> Result<Response<AddressMsg>, Status> {
        let finger_entry = self.predecessor.lock().unwrap();
        info!("Received get predecessor call, predecessor is {:?}", finger_entry);
        Ok(Response::new(finger_entry.clone().into()))
    }

    type SetPredecessorStream = Pin<Box<dyn Stream<Item=Result<KvPairMsg, Status>> + Send>>;

    async fn set_predecessor(&self, request: Request<AddressMsg>) -> Result<Response<Self::SetPredecessorStream>, Status> {
        let new_predecessor: FingerEntry = request.get_ref().into();
        let upper = new_predecessor.key;

        info!("Received set_predecessor call, new predecessor is {:?}", new_predecessor);

        let limit: Key = {
            let mut predecessor = self.predecessor.lock().unwrap();
            *predecessor = new_predecessor;
            predecessor.key
        };


        let (tx, rx) = mpsc::unbounded_channel();
        let kv_store_arc = self.kv_store_arc.clone();

        let lower = {
            self.predecessor.lock().unwrap().key
        };

        tokio::spawn(async move {
            let kv_store_guard = kv_store_arc.lock().unwrap();
            let kv_store_iter = kv_store_guard.iter(lower, upper);
            for (key, value) in kv_store_iter {
                println!("handing over pair ({}, {})", key, value);

                if let Err(err) = tx.send(Ok(KvPairMsg {
                    key: key.to_be_bytes().to_vec(),
                    value: value.clone(),
                })) {
                    println!("ERROR: failed to update stream client: {:?}", err);
                };
            }
        });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::SetPredecessorStream))
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
            predecessor_guard.get_address().clone()
        };
        let upper_finger = {
            let finger_table_guard = self.finger_table.lock().unwrap();
            finger_table_guard.fingers[index_to_update].clone()
        };


        let upper_key = crypto::hash(&upper_finger.get_address().as_bytes());
        let lower_key = self.pos.overflowing_add(Key::two().overflowing_pow(index_to_update as u32).0).0 as Key;
        // let lower_key = self.pos;
        if is_between(finger_entry_update_key, lower_key, upper_key, false, true) {
            info!("Updating finger table entry {} with {:?}", index_to_update, finger_entry_update);
            {
                let mut finger_table_guard = self.finger_table.lock().unwrap();
                *finger_table_guard.fingers[index_to_update].get_address_mut() = finger_entry_update.get_address().clone();
            }

            let mut predecessor_to_update_client = ChordClient::connect(format!("http://{}", predecessor_address_str))
                .await
                .unwrap();
            let _ = predecessor_to_update_client.update_finger_table_entry(Request::new(UpdateFingerTableEntryRequest {
                index: index_to_update as u32,
                finger_entry: Some(finger_entry_update.into()),
            })).await.unwrap();
        }

        Ok(Response::new(Empty {}))
    }

    async fn find_closest_preceding_finger(&self, request: Request<KeyMsg>) -> Result<Response<FingerEntryMsg>, Status> {
        let key = Key::from_be_bytes(request.get_ref().clone().key.try_into().unwrap());
        for finger in self.finger_table.lock().unwrap().fingers.iter().rev() {
            let node_pos = crypto::hash(finger.get_address().as_bytes());
            if is_between(node_pos, self.pos, key, true, true) {
                return Ok(Response::new(FingerEntryMsg {
                    id: node_pos.to_be_bytes().to_vec(),
                    address: finger.clone().get_address().into(),
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

    async fn get_kv_store_size(&self, _: Request<Empty>) -> Result<Response<GetKvStoreSizeResponse>, Status> {
        Ok(Response::new(GetKvStoreSizeResponse {
            size: self.kv_store_arc.lock().unwrap().size() as u32
        }))
    }

    async fn get(&self, request: Request<KeyMsg>) -> Result<Response<GetResponse>, Status> {
        let key = Key::from_be_bytes(request.get_ref().key.clone().try_into().unwrap());
        let predecessor_address = {
            self.predecessor.lock().unwrap().get_address().clone()
        };
        let predecessor_key = crypto::hash(predecessor_address.as_bytes());
        if is_between(key, predecessor_key, self.pos, true, false) || predecessor_key == self.pos {
            match self.kv_store_arc.lock().unwrap().get(&key) {
                Some(value) => {
                    info!("Get request for key {}, value = {}", key, value);
                    Ok(Response::new(GetResponse {
                        value: value.clone(),
                        status: GetStatus::Ok.into(),
                    }))
                }
                None => {
                    Ok(Response::new(GetResponse {
                        value: String::default(),
                        status: GetStatus::NotFound.into(),
                    }))
                }
            }
        } else {
            error!("Invalid key {}!", key);
            error!("This node is responsible for interval ({}, {}] !", predecessor_key, self.pos);
            let msg = format!("Node ({}, {}) is responsible for range ({}, {})", self.address, self.pos, predecessor_address, predecessor_key);
            Err(Status::internal(msg))
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<Empty>, Status> {
        let key = Key::from_be_bytes(request.get_ref().key.clone().unwrap().key.try_into().unwrap());
        let ttl = request.get_ref().ttl;
        let replication = request.get_ref().replication;
        let value = &request.get_ref().value;

        // todo: handle ttl
        // todo: handle replication
        let is_update = self.kv_store_arc.lock().unwrap().put(&key, value);
        info!("Received PUT request ({}, {}) with ttl {} and replication {}", key, value, ttl, replication);
        Ok(Response::new(Empty {}))
    }
}
