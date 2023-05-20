use tonic::{Request, Response, Status};

pub mod chord_proto {
    tonic::include_proto!("chord");
}

#[derive(Default)]
pub struct ChordService {}

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
