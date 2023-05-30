use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::{Empty, FindSuccessorResponse, FingerEntryMsg, FingerTableMsg, GetPredecessorResponse, SetPredecessorRequest};
use crate::finger_table::{FingerEntry, FingerTable};

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type NodeUrl = String;

#[derive(Debug)]
pub struct ChordService {
    finger_table: Arc<Mutex<FingerTable>>,
    predecessor: Arc<Mutex<FingerEntry>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<(FingerTable, FingerEntry)>) -> ChordService {
        let (finger_table, predecessor) = rx.await.unwrap();
        ChordService {
            finger_table: Arc::new(Mutex::new(finger_table)),
            predecessor: Arc::new(Mutex::new(predecessor)),
        }
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

        Ok(Response::new(FindSuccessorResponse {
            address: format!("{}", self.finger_table.lock().unwrap().fingers.len()),
        }))
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


    async fn notify(
        &self,
        request: Request<chord_proto::NotifyRequest>,
    ) -> Result<Response<chord_proto::Empty>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }

    async fn get_finger_table(&self, request: Request<Empty>) -> Result<Response<FingerTableMsg>, Status> {
        let finger_table_guard = self.finger_table.lock().unwrap();
        let finger_table_msg: FingerTableMsg = finger_table_guard.clone().into();
        Ok(Response::new(finger_table_msg))
    }
}
