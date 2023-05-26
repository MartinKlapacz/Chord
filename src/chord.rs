use tonic::{Request, Response, Status};

use crate::crypto;
use crate::crypto::Key;

pub mod chord_proto {
    tonic::include_proto!("chord");
}

type NodeUrl = String;

#[derive(Default)]
pub struct ChordService {
    finger_table: Vec<FingerEntry>,
}

pub struct FingerEntry {
    key: Key,
    url: NodeUrl,
}


impl ChordService {
    pub fn new(address: &String, m: usize) -> ChordService {
        let id = crypto::hash(address);

        let mut finger_table = Vec::with_capacity(m);
        for i in 0..m {
            let start = (id + 2u128.pow(i as u32)) % 2u128.pow(m as u32);
            finger_table.push(FingerEntry {
                key: start,
                url: NodeUrl::default(),
            });
        };

        ChordService { finger_table }
    }
}

#[tonic::async_trait]
impl chord_proto::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord_proto::FindSuccessorRequest>,
    ) -> Result<Response<chord_proto::FindSuccessorResponse>, Status> {
        // Implement the find_successor method here.
        Err(Status::unimplemented("todo"))
    }

    async fn notify(
        &self,
        request: Request<chord_proto::NotifyRequest>,
    ) -> Result<Response<chord_proto::NotifyResponse>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }
}
