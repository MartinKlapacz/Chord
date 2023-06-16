use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Receiver;
use tonic::{Request, Response, Status};

use crate::chord::chord_proto::FindSuccessorResponse;
use crate::finger_table::FingerTable;
use crate::key_value_store::KVStore;

pub mod chord_proto {
    tonic::include_proto!("chord");
}

pub type NodeUrl = String;

pub struct ChordService {
    finger_table: Arc<Mutex<FingerTable>>,
    kv_store: Option<Arc<Mutex<dyn KVStore + Send>>>,
}


impl ChordService {
    pub async fn new(rx: Receiver<FingerTable>) -> ChordService {
        let finger_table = rx.await.unwrap();
        ChordService {
            finger_table: Arc::new(Mutex::new(finger_table)),
            kv_store: None
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

    async fn notify(
        &self,
        request: Request<chord_proto::NotifyRequest>,
    ) -> Result<Response<chord_proto::NotifyResponse>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }
}
