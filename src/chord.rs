use std::sync::{Arc, Mutex};
use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};
use crate::chord::chord_proto::FindSuccessorResponse;

use crate::crypto;
use crate::crypto::Key;
use crate::finger_table::{FingerEntry, FingerTable};

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type NodeUrl = String;

#[derive(Debug)]
pub struct ChordService {
    finger_table: Arc<Mutex<FingerTable>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<FingerTable>) -> ChordService {
        let finger_table = rx.await.unwrap();
        ChordService { finger_table: Arc::new(Mutex::new(finger_table)) }
    }
}

#[tonic::async_trait]
impl chord_proto::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord_proto::FindSuccessorRequest>,
    ) -> Result<Response<chord_proto::FindSuccessorResponse>, Status> {
        info!("Received FindSuccessorRequest");
        Ok(Response::new(FindSuccessorResponse{
            address: format!("{}", self.finger_table.lock().unwrap().fingers.len()),
        }))
    }

    async fn notify(
        &self,
        request: Request<chord_proto::NotifyRequest>,
    ) -> Result<Response<chord_proto::NotifyResponse>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }
}
