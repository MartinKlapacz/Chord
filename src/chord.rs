use std::error::Error;
use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::{Empty, FingerInfoMsg, FingerTableMsg, NodeInfo, NodeMsg};
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
        request: Request<chord_proto::NodeMsg>,
    ) -> Result<Response<chord_proto::NodeMsg>, Status> {
        let finger_entry: FingerEntry = request.get_ref().into();
        info!("Received find successor call for {:?}", finger_entry);

        if self.is_successor_of_key(finger_entry.key) {
            Ok(Response::new(self.url.clone().into()))
        } else {
            let looked_up_finger_entry = self.find_successor_helper(finger_entry.key).await.unwrap();
            Ok(Response::new(looked_up_finger_entry.into()))
        }
    }


    async fn get_predecessor(&self, _request: Request<Empty>) -> Result<Response<NodeMsg>, Status> {
        info!("Received get predecessor call");
        let finger_entry = self.predecessor.lock().unwrap();
        Ok(Response::new(finger_entry.clone().into()))
    }

    async fn set_predecessor(&self, request: Request<NodeMsg>) -> Result<Response<Empty>, Status> {
        let new_predecessor: FingerEntry = request.get_ref().into();

        info!("Setting predecessor to {:?}", new_predecessor);
        let mut predecessor = self.predecessor.lock().unwrap();
        *predecessor = new_predecessor;
        Ok(Response::new(Empty {}))
    }

    async fn find_predecessor(&self, request: Request<NodeMsg>) -> Result<Response<NodeMsg>, Status> {
        let successor_finger_entry = self.find_successor_helper(request.get_ref().into()).await.unwrap();
        let mut client = ChordClient::connect(format!("http://{}", successor_finger_entry.url))
            .await
            .unwrap();

        let predecessor = client.get_predecessor(Request::new(
            Empty {}
        )).await.unwrap().get_ref().clone();

        Ok(Response::new(predecessor))
    }


    async fn get_node_info(&self, _: Request<Empty>) -> Result<Response<NodeInfo>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap();
        let predecessor = self.predecessor.lock().unwrap().clone();

        Ok(Response::new(NodeInfo {
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
