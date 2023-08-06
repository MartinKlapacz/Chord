use std::future::IntoFuture;
use std::ops::Add;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{debug, warn};
use tokio::sync::mpsc;
use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tonic::codegen::Body;
use tonic::transport::Channel;

use crate::kv::kv_store::KVStore;
use crate::node::finger_entry::FingerEntry;
use crate::node::finger_table::FingerTable;
use crate::node::successor_list::SuccessorList;
use crate::threads::chord::chord_proto::{AddressMsg, Empty, FingerEntryMsg, GetKvStoreDataResponse, GetKvStoreSizeResponse, GetPredecessorResponse, GetRequest, GetResponse, HashPosMsg, KvPairDebugMsg, KvPairMsg, NodeSummaryMsg, PutRequest, SuccessorListMsg};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::utils::crypto::{hash, HashPos, HashRingKey, is_between};


pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type Address = String;

pub struct ChordService {
    address: String,
    pos: HashPos,
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor_option: Arc<Mutex<Option<FingerEntry>>>,
    kv_store: Arc<Mutex<dyn KVStore + Send>>,
    fix_finger_index: Arc<Mutex<usize>>,
    successor_list: Arc<Mutex<SuccessorList>>
}

const MAX_RETRIES: u64 = 30;
const CONNECTION_RETRY_SLEEP: u64 = 100;

pub(crate) async fn connect_with_retry(current_address: &Address) -> Result<ChordClient<Channel>, Status> {
    let mut retries = 0;

    loop {
        match ChordClient::connect(format!("http://{}", current_address)).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                retries += 1;
                if retries > MAX_RETRIES {
                    // You can return an error or handle it differently
                    return Err(Status::unavailable("Reached maximum number of connection retries"));
                }
                // Log error or do something with it
                eprintln!("Failed to connect: {}. Retrying...", e);
                sleep(Duration::from_millis(CONNECTION_RETRY_SLEEP)).await; // Wait 100 ms before retrying
            }
        }
    }
}


impl ChordService {
    pub async fn new(rx: Receiver<(Arc<Mutex<FingerTable>>, Arc<Mutex<Option<FingerEntry>>>, Arc<Mutex<dyn KVStore + Send>>, Arc<Mutex<SuccessorList>>)>, url: &String) -> ChordService {
        let (finger_table_arc, predecessor_option_arc, kv_store_arc, successor_list_arc) = rx.await.unwrap();
        ChordService {
            address: url.clone(),
            pos: hash(&url.as_bytes()),
            finger_table: finger_table_arc,
            predecessor_option: predecessor_option_arc,
            kv_store: kv_store_arc,
            fix_finger_index: Arc::new(Mutex::new(0)),
            successor_list: successor_list_arc
        }
    }

    pub async fn get_successor_address(&self, ) -> Address {
        self.successor_list.lock().unwrap().successors[0].clone()
    }

}


#[tonic::async_trait]
impl chord_proto::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord_proto::HashPosMsg>,
    ) -> Result<Response<chord_proto::AddressMsg>, Status> {
        let key: HashPos = request.into_inner().into();

        let direct_successor_address = self.get_successor_address().await;
        let successor_pos: HashPos = hash(direct_successor_address.as_bytes());

        let successor_address_msg: AddressMsg = if is_between(key, self.pos + 1, successor_pos, false, false) {
            direct_successor_address.into()
        } else {
            let closest_preceding_node_address = self.find_closest_preceding_finger(Request::new(HashPosMsg {
                key: key.to_be_bytes().to_vec()
            })).await.unwrap().into_inner().address;

            let mut closest_preceding_node_client = connect_with_retry(&closest_preceding_node_address)
                .await?;

            closest_preceding_node_client.find_successor(Request::new(key.into()))
                .await?
                .into_inner()
        };

        debug!("Received find_successor call for {:?}, successor is {:?}", key, successor_address_msg);
        Ok(Response::new(successor_address_msg))
    }


    async fn get_predecessor(&self, _request: Request<Empty>) -> Result<Response<GetPredecessorResponse>, Status> {
        let predecessor = match *self.predecessor_option.lock().unwrap() {
            Some(ref predecessor) => {
                debug!("Received get predecessor call, predecessor is {:?}", predecessor.address);
                predecessor.address.clone()
            }
            None => {
                debug!("Received get predecessor call, predecessor is Nil");
                Address::default()
            }
        };
        Ok(Response::new(GetPredecessorResponse { address_optional: Some(predecessor.into()) }))
    }

    async fn get_successor_list(&self, _: Request<Empty>) -> Result<Response<SuccessorListMsg>, Status> {
        Ok(Response::new(self.successor_list.lock().unwrap().clone().into()))
    }


    async fn find_closest_preceding_finger(&self, request: Request<HashPosMsg>) -> Result<Response<FingerEntryMsg>, Status> {
        let key = HashPos::from_be_bytes(request.get_ref().clone().key.try_into().unwrap());
        for finger in self.finger_table.lock().unwrap().fingers.iter().rev() {
            if finger.address.eq(&Address::default()) {
                // ignore yet uninitialized entries
                continue;
            }
            let node_pos = hash(finger.get_address().as_bytes());
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
        let predecessor_option = self.predecessor_option.lock().unwrap();
        let successor_list = self.successor_list.lock().unwrap();

        Ok(Response::new(NodeSummaryMsg {
            url: self.address.clone(),
            pos: self.pos.to_be_bytes().iter()
                .map(|byte| byte.to_string())
                .collect::<Vec<String>>().join(" "),
            predecessor: match predecessor_option.clone() {
                Some(predecessor) => Some(predecessor.into()),
                None => None
            },
            finger_entries: finger_table_guard.fingers.iter()
                .map(|finger| finger.clone())
                .map(|finger| finger.into())
                .collect(),
            successor_list: Some(successor_list.clone().into())
        }))
    }

    async fn get_kv_store_size(&self, _: Request<Empty>) -> Result<Response<GetKvStoreSizeResponse>, Status> {
        Ok(Response::new(GetKvStoreSizeResponse {
            size: self.kv_store.lock().unwrap().size() as u32
        }))
    }

    async fn get_kv_store_data(&self, _: Request<Empty>) -> Result<Response<GetKvStoreDataResponse>, Status> {
        let kv_pairs = {
            self.kv_store.lock().unwrap().iter(HashPos::one() + 1, HashPos::one(), false, false)
                .map(|(key, value)| KvPairDebugMsg {
                    key: key.map(|b| b.to_string()).join(" "),
                    value: value.clone(),
                }).collect()
        };
        Ok(Response::new(GetKvStoreDataResponse { kv_pairs }))
    }


    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        // let key: Key = request.into_inner().key.try_into().unwrap();
        // let predecessor_address = {
        //     self.predecessor_option.lock().unwrap().get_address().clone()
        // };
        // let predecessor_key = hash(predecessor_address.as_bytes());
        // if is_between(hash(&key), predecessor_key, self.pos, true, false) || predecessor_key == self.pos {
        //     match self.kv_store.lock().unwrap().get(&key) {
        //         Some(value) => {
        //             // info!("Get request for key {}, value = {}", key, value);
        //             Ok(Response::new(GetResponse {
        //                 value: value.clone(),
        //                 status: GetStatus::Ok.into(),
        //             }))
        //         }
        //         None => {
        //             Ok(Response::new(GetResponse {
        //                 value: String::default(),
        //                 status: GetStatus::NotFound.into(),
        //             }))
        //         }
        //     }
        // } else {
        //     // error!("Invalid key {}!", key);
        //     error!("This node is responsible for interval ({}, {}] !", predecessor_key, self.pos);
        //     let msg = format!("Node ({}, {}) is responsible for range ({}, {})", self.address, self.pos, predecessor_address, predecessor_key);
        //     Err(Status::internal(msg))
        // }
        Err(Status::internal(""))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<Empty>, Status> {
        let key = request.get_ref().key.clone().try_into().unwrap();
        let ttl = request.get_ref().ttl;
        let replication = request.get_ref().replication;
        let value = &request.get_ref().value;

        // todo: handle ttl
        // todo: handle replication
        let is_update = self.kv_store.lock().unwrap().put(&key, value);
        // info!("Received PUT request ({}, {}) with ttl {} and replication {}", key, value, ttl, replication);
        Ok(Response::new(Empty {}))
    }

    async fn fix_fingers(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let index = (*self.fix_finger_index.lock().unwrap() + 1) % HashPos::finger_count();
        debug!("Fixing finger entry {}", index);
        let lookup_position = self.pos.overflowing_add(HashPos::one().overflowing_shl(index as u32).0).0;

        let responsible_node_for_lookup_pos_response_result = self.find_successor(Request::new(HashPosMsg {
            key: lookup_position.to_be_bytes().to_vec(),
        })).await;

        match responsible_node_for_lookup_pos_response_result {
            Ok(responsible_node_for_lookup_pos_response) => {
                *self.fix_finger_index.lock().unwrap() = index;
                self.finger_table.lock().unwrap().fingers[index].address = responsible_node_for_lookup_pos_response.into_inner().into();
            }
            Err(e) => warn!("An error occurred during fix_fingers: {}", e)
        }
        Ok(Response::new(Empty {}))
    }

    async fn stabilize(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        debug!("Stabilizing...");
        let successor_address = self.get_successor_address().await;

        let current_successors_predecessor_address_optional: Option<Address> = {
            match ChordClient::connect(format!("http://{}", successor_address).clone()).await {
                Ok(mut successor_client) => {
                    successor_client.get_predecessor(Request::new(Empty {}))
                        .await
                        .unwrap().into_inner().address_optional.map(|address| address.into())
                }
                Err(_) => {
                    // todo: handle successor unreachable
                    let mut i = 1;
                    let mut finger_table_guard = self.finger_table.lock().unwrap();
                    let successor_address = { self.successor_list.lock().unwrap().successors[0].clone() };

                    while i < finger_table_guard.fingers.len() && finger_table_guard.fingers[i].address.eq(&successor_address) {
                        i += 1;
                    }

                    let new_successor_address = if i == finger_table_guard.fingers.len() {
                        match *self.predecessor_option.lock().unwrap() {
                            Some(ref predecessor) => predecessor.address.clone(),
                            None => self.address.clone()
                        }
                    } else {
                        finger_table_guard.fingers[i].get_address().clone()
                    };

                    for index in 0..i {
                        *finger_table_guard.fingers[index].get_address_mut() = new_successor_address.clone();
                    }
                    return Ok(Response::new(Empty {}));
                }
            }
        };


        Ok(Response::new(Empty {}))
    }


    type NotifyStream = Pin<Box<dyn Stream<Item=Result<KvPairMsg, Status>> + Send>>;

    async fn notify(&self, request: Request<AddressMsg>) -> Result<Response<Self::NotifyStream>, Status> {
        let (tx, rx) = mpsc::unbounded_channel();

        let caller_address: Address = request.into_inner().into();
        let caller_pos = hash(caller_address.as_bytes());

        let mut predecessor_option_guard = self.predecessor_option.lock().unwrap();

        let (update_predecessor_to_caller, lower, upper) = match *predecessor_option_guard {
            Some(ref predecessor) => {
                let lower = hash(predecessor.address.as_bytes());
                let upper = self.pos;
                if is_between(caller_pos, lower + 1, upper, false, true) {
                    (true, lower, upper)
                } else {
                    (false, HashPos::default(), HashPos::default())
                }
            }
            None => {
                (true, HashPos::default(), caller_pos)
            }
        };

        if update_predecessor_to_caller {
            *predecessor_option_guard = Some(FingerEntry {
                key: caller_pos,
                address: caller_address,
            });
            debug!("Updated predecessor due to notify-call");
        }

        // let kv_store_arc = self.kv_store.clone();
        // tokio::spawn(async move {
        //     if caller_is_new_predecessor {
        //         let mut transferred_keys: Vec<Key> = vec![];
        //         {
        //             info!("Handing over data from {} to {}", lower, upper);
        //             let kv_store_guard = kv_store_arc.lock().unwrap();
        //             let mut pair_count = 0;
        //             for (key, value) in kv_store_guard.iter(lower, upper, true, false) {
        //                 transferred_keys.push(key.clone());
        //                 debug!("Handing over KV pair ({:?}, {})", key, value);
        //                 if let Err(err) = tx.send(Ok(KvPairMsg {
        //                     key: key.to_vec(),
        //                     value: value.clone(),
        //                 })) {
        //                     error!("ERROR: failed to update stream client: {:?}", err);
        //                 };
        //                 pair_count += 1;
        //             }
        //             info!("Data handoff finished, transferred {} pairs", pair_count)
        //         }
        //
        //         let mut kv_store_guard = kv_store_arc.lock().unwrap();
        //         for key in &transferred_keys {
        //             kv_store_guard.delete(key);
        //         }
        //     }
        // });

        let stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::NotifyStream))
    }

    async fn health(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }



}
