use std::error::Error;
use log::info;
use tokio::sync::oneshot::Sender;
use tonic::Request;
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::chord::chord_proto::{Empty, NodeMsg};
use crate::chord::NodeUrl;
use crate::crypto;
use crate::crypto::Key;
use crate::finger_table::{FingerEntry, FingerTable};

pub async fn process_node_join(peer_address_option: Option<String>, own_grpc_address: String, tx: Sender<(FingerTable, FingerEntry)>) -> Result<(), Box<dyn Error>>{
    let id = crypto::hash(&own_grpc_address);

    let mut finger_table = FingerTable::new(&id, &own_grpc_address);
    let mut predecessor: FingerEntry = own_grpc_address.clone().into();

    match peer_address_option {
        Some(peer_address) => {
            info!("Joining existing cluster");
            let mut join_peer_client = ChordClient::connect(format!("http://{}", peer_address))
                .await
                .unwrap();

            for finger in &mut finger_table.fingers {
                let response = join_peer_client.find_successor(Request::new(finger.clone().into()))
                    .await
                    .unwrap();
                finger.url = response.get_ref().clone().url;
            }
            info!("Initialized finger table from peer");

            let direct_successor_url = finger_table.fingers.first().unwrap().url.clone();
            let mut direct_successor_client = ChordClient::connect(format!("http://{}", direct_successor_url))
                .await
                .unwrap();
            let get_predecessor_response = direct_successor_client.get_predecessor(Request::new(Empty{})).await.unwrap();
            predecessor = get_predecessor_response.get_ref().clone().into();
            info!("Received predecessor from peer");

            let _empty = direct_successor_client.set_predecessor(Request::new(predecessor.clone().url.into()))
                .await
                .unwrap();
            info!("Updated predecessor of {} to this", peer_address)
        }
        None => {
            info!("Starting up a new cluster");
            finger_table.set_all_fingers(&own_grpc_address);
        }
    };

    tx.send((finger_table, predecessor)).unwrap();
    Ok(())
}
