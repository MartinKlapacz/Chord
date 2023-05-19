use tonic::{Request, Response, Status};
use crate::chord;

pub struct ChordService {
    // Add necessary fields here.
}

#[tonic::async_trait]
impl chord::chord_server::Chord for ChordService {
    async fn find_successor(
        &self,
        request: Request<chord::FindSuccessorRequest>,
    ) -> Result<Response<chord::FindSuccessorResponse>, Status> {
        // Implement the find_successor method here.
        Err(Status::unimplemented("todo"))
    }

    async fn notify(
        &self,
        request: Request<chord::NotifyRequest>,
    ) -> Result<Response<chord::NotifyResponse>, Status> {
        // Implement the notify method here.
        Err(Status::unimplemented("todo"))
    }
}
