use std::error::Error;
use log::info;
use tokio::sync::oneshot::Sender;
use tonic::Request;
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::chord::chord_proto::{Empty, FindSuccessorRequest, SetPredecessorRequest};
use crate::crypto;
use crate::finger_table::FingerTable;

pub async fn process_node_join(peer_address_option: Option<String>, own_grpc_address: String, tx: Sender<(FingerTable, String)>) -> Result<(), Box<dyn Error>>{
    let id = crypto::hash(&own_grpc_address);

    let mut finger_table = FingerTable::new(&id);
    let mut predecessor_url = own_grpc_address.clone();

    match peer_address_option {
        Some(peer_address) => {
            info!("Joining existing cluster");
            let mut client = ChordClient::connect(format!("http://{}", peer_address))
                .await
                .unwrap();

            for finger in &mut finger_table.fingers {
                let bytes = finger.key.to_be_bytes().to_vec();
                let response = client.find_successor(Request::new(FindSuccessorRequest {
                    id: bytes,
                })).await.unwrap();
                finger.url = response.get_ref().address.clone();
            }
            info!("Initialized finger table from peer");

            let response = client.get_predecessor(Request::new(Empty{})).await.unwrap();
            predecessor_url = response.get_ref().url.clone();
            info!("Received predecessor from peer");

            let _empty = client.set_predecessor(Request::new(SetPredecessorRequest {
                url: own_grpc_address.clone()
            })).await.unwrap();
            info!("Updated predecessor of peer to this")
        }
        None => {
            info!("Starting up a new cluster");
            finger_table.set_all_fingers(&own_grpc_address);
        }
    };
    tx.send((finger_table, predecessor_url)).unwrap();
    Ok(())
}
