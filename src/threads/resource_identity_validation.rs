use std::future::Future;
use std::pin::Pin;
use tonic::Request;
use tonic::transport::Channel;
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::PuzzleMsg;

pub async fn validate_predecessor_resources(mut predecessor_client: ChordClient<Channel>)  {
    predecessor_client.request_proof_of_work(Request::new(PuzzleMsg {

    }));
    println!("df")
}
