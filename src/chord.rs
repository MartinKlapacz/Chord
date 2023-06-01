use std::error::Error;
use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::{Empty, FindPredecessorRequest, FindPredecessorResponse, FindSuccessorRequest, FindSuccessorResponse, FingerEntryMsg, FingerInfoMsg, FingerTableMsg, GetPredecessorResponse, NodeInfoResponse, SetPredecessorRequest};
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::crypto;
use crate::crypto::Key;
use crate::finger_table::{FingerEntry, FingerTable};

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type NodeUrl = String;

#[derive(Debug)]
pub struct ChordService {
    url: String,
    key: Key,
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor: Arc<Mutex<FingerEntry>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<(FingerTable, FingerEntry)>, url: &String) -> ChordService {
        let (finger_table, predecessor) = rx.await.unwrap();
        ChordService {
            url: url.clone(),
            key: crypto::hash(&url),
            finger_table: Arc::new(Mutex::new(finger_table)),
            predecessor: Arc::new(Mutex::new(predecessor)),
        }
    }

    pub fn is_successor_of_key(&self, key: Key) -> bool {
        let predecessor_position = self.predecessor.lock().unwrap().key.clone();
        let own_position = self.key.clone();

        if predecessor_position < own_position {
            return predecessor_position < key && key <= own_position;
        } else if predecessor_position > own_position {
            return predecessor_position < key || key <= own_position;
        } else {
            return true
        }
    }

    async fn find_successor_helper(&self, id: Key) -> Result<(Vec<u8>, String), Box<dyn Error>> {
        let all_fingers = &self.finger_table.lock().unwrap().fingers;
        let successor_fingers: Vec<FingerEntry> = all_fingers.iter()
            .filter(|finger| id <= finger.key)
            .map(|finger| finger.clone())
            .collect();
        let closest_successor = match successor_fingers.is_empty() {
            true => all_fingers.first().unwrap().clone(),
            false => successor_fingers.first().unwrap().clone()
        };

        Ok((closest_successor.key.to_be_bytes().to_vec(), closest_successor.url))
    }
}

#[tonic::async_trait]
impl chord_proto::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord_proto::FindSuccessorRequest>,
    ) -> Result<Response<chord_proto::FindSuccessorResponse>, Status> {
        let key = request.get_ref().id.clone();
        info!("Received find successor call for {:?}", key);
        // todo: get closest successor for key

        let key = Key::from_be_bytes(key.clone().try_into().unwrap());

        if self.is_successor_of_key(key) {
            let finger_entry_msg = FingerEntryMsg {
                key: self.key.to_be_bytes().to_vec(),
                url: self.url.clone(),
            };
            Ok(Response::new(FindSuccessorResponse {
                successor: Some(finger_entry_msg)
            }))
        } else {
            let (key, url) = self.find_successor_helper(key).await.unwrap();

            let successor = FingerEntryMsg { key, url };
            Ok(Response::new(FindSuccessorResponse { successor: Some(successor) }))
        }
    }


    async fn get_predecessor(&self, _request: Request<Empty>) -> Result<Response<GetPredecessorResponse>, Status> {
        info!("Received get predecessor call");
        let finger_entry = self.predecessor.lock().unwrap();
        Ok(Response::new(GetPredecessorResponse {
            predecessor: Some(finger_entry.clone().into())
        }))
    }

    async fn set_predecessor(&self, request: Request<SetPredecessorRequest>) -> Result<Response<Empty>, Status> {
        let new_predecessor_msg = request.get_ref().clone().predecessor.unwrap();
        let new_predecessor: FingerEntry = new_predecessor_msg.into();

        info!("Setting predecessor to {}", new_predecessor.url);
        let mut predecessor = self.predecessor.lock().unwrap();
        *predecessor = new_predecessor;
        Ok(Response::new(Empty {}))
    }

    async fn find_predecessor(&self, request: Request<FindPredecessorRequest>) -> Result<Response<FindPredecessorResponse>, Status> {
        let id = request.get_ref().id.clone();
        let id = Key::from_be_bytes(id.try_into().unwrap());

        let (_, url) = self.find_successor_helper(id).await.unwrap();

        let mut client = ChordClient::connect(format!("http://{}", url))
            .await
            .unwrap();

        let predecessor = client.get_predecessor(Request::new(
            Empty {}
        )).await.unwrap().get_ref().clone().predecessor.unwrap();

        Ok(Response::new(FindPredecessorResponse {
            predecessor: Some(predecessor)
        }))
    }


    async fn notify(
        &self,
        request: Request<chord_proto::NotifyRequest>,
    ) -> Result<Response<chord_proto::Empty>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }

    async fn get_node_info(&self, _: Request<Empty>) -> Result<Response<NodeInfoResponse>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap();
        let predecessor = self.predecessor.lock().unwrap().clone();

        Ok(Response::new(NodeInfoResponse {
            url: self.url.clone(),
            id: self.key.to_be_bytes().iter().map(|byte| byte.to_string()).collect::<Vec<String>>().join(" "),
            predecessor: Some(predecessor.into()),
            finger_infos: finger_table_guard.fingers.iter()
                .map(|finger| finger.clone())
                .map(|finger| finger.into())
                .collect(),
        }))
    }
}
