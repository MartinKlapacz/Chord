use std::error::Error;
use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::{AddressMsg, Data, Empty, FingerEntryMsg, FingerTableMsg, KeyMsg, NodeSummaryMsg};
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
    key: Key,
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor: Arc<Mutex<FingerEntry>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<(FingerTable, FingerEntry)>, url: &String) -> ChordService {
        let (finger_table, predecessor) = rx.await.unwrap();
        ChordService {
            address: url.clone(),
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
            return true;
        }
    }

    async fn find_successor_helper(&self, id: Key) -> Result<FingerEntry, Box<dyn Error>> {
        let all_fingers = &self.finger_table.lock().unwrap().fingers;
        let successor_fingers: Vec<FingerEntry> = all_fingers.iter()
            .filter(|finger| id <= finger.key)
            .map(|finger| finger.clone())
            .collect();
        let closest_successor_finger = match successor_fingers.is_empty() {
            true => all_fingers.first(),
            false => successor_fingers.first()
        }.unwrap().clone();

        Ok(closest_successor_finger)
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
            false => self.find_successor_helper(key_msg.into()).await.unwrap()
        };
        info!("Received find_successor call for {:?}, successor is {:?}", key_msg, successor_finger_entry);
        Ok(Response::new(successor_finger_entry.into()))
    }


    async fn find_predecessor(&self, request: Request<KeyMsg>) -> Result<Response<AddressMsg>, Status> {
        let successor_finger_entry = self.find_successor_helper(request.get_ref().into()).await.unwrap();
        let mut client = ChordClient::connect(format!("http://{}", successor_finger_entry.address))
            .await
            .unwrap();

        let predecessor = client.get_predecessor(Request::new(
            Empty {}
        )).await.unwrap().get_ref().clone();

        Ok(Response::new(predecessor))
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


    async fn get_node_summary(&self, _: Request<Empty>) -> Result<Response<NodeSummaryMsg>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap();
        let predecessor = self.predecessor.lock().unwrap().clone();

        Ok(Response::new(NodeSummaryMsg {
            url: self.address.clone(),
            id: self.key.to_be_bytes().iter().map(|byte| byte.to_string()).collect::<Vec<String>>().join(" "),
            predecessor: Some(predecessor.into()),
            finger_entries: finger_table_guard.fingers.iter()
                .map(|finger| finger.clone())
                .map(|finger| finger.into())
                .collect(),
        }))
    }
}
