use std::convert::TryInto;
use std::error::Error;
use std::ops::Add;

use log::info;
use tokio::sync::oneshot::Sender;
use tonic::Request;

use crate::chord::Address;
use crate::chord::chord_proto::{AddressMsg, Empty, UpdateFingerTableEntryRequest};
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::crypto;
use crate::crypto::{HashRingKey, Key};
use crate::finger_table::{FingerEntry, FingerTable};

pub async fn process_node_join(peer_address_option: Option<Address>, own_grpc_address_str: &String, tx: Sender<(FingerTable, FingerEntry)>) -> Result<(), Box<dyn Error>> {
    let own_id = crypto::hash(own_grpc_address_str);

    let mut finger_table = FingerTable::new(&own_id, own_grpc_address_str);
    let mut predecessor: AddressMsg = own_grpc_address_str.clone().into();

    match peer_address_option {
        Some(peer_address_str) => {
            info!("Joining existing cluster");
            let mut join_peer_client = ChordClient::connect(format!("http://{}", peer_address_str))
                .await
                .unwrap();

            for finger in &mut finger_table.fingers {
                let response = join_peer_client.find_successor(Request::new(finger.into()))
                    .await
                    .unwrap();
                finger.address = response.get_ref().clone().into();
            }
            info!("Initialized finger table from peer");

            let direct_successor_url = finger_table.fingers.first().unwrap().address.clone();
            let mut direct_successor_client = ChordClient::connect(format!("http://{}", direct_successor_url))
                .await
                .unwrap();
            let get_predecessor_response = direct_successor_client.get_predecessor(Request::new(Empty {})).await.unwrap();
            predecessor = get_predecessor_response.get_ref().clone().into();
            info!("Received predecessor from peer");

            let data = direct_successor_client.set_predecessor(Request::new(own_grpc_address_str.into()))
                .await
                .unwrap();

            // todo: store data and make available to grpc service
            let finger_entry_peer: FingerEntry = peer_address_str.into();
            let finger_entry_this: FingerEntry = own_grpc_address_str.into();
            info!("Updated predecessor of {:?} to {:?}", &finger_entry_peer, &finger_entry_this);

            // finger table is constructed, send it to grpc thread so it can start up its service
            tx.send((finger_table.clone(), predecessor.into())).unwrap();

            info!("Updating others...");
            for index in 0..finger_table.fingers.len() {
                let key_to_find_predecessor_for: Key = own_id.overflowing_sub(Key::two().overflowing_pow(index as u32).0).0;
                info!("Searching predecessor for key: {} ", key_to_find_predecessor_for);
                let response = join_peer_client.find_predecessor(Request::new(key_to_find_predecessor_for.into()))
                    .await
                    .unwrap();
                let predecessor_to_update_address = response.get_ref().address.clone();

                let mut predecessor_to_update_client = ChordClient::connect(format!("http://{}", predecessor_to_update_address))
                    .await
                    .unwrap();
                let _ = predecessor_to_update_client.update_finger_table_entry(Request::new(UpdateFingerTableEntryRequest {
                    index: index as u32,
                    finger_entry: Some(finger_entry_this.clone().into()),
                })).await.unwrap();
            }
            info!("Updated other nodes")
        }
        None => {
            info!("Starting up a new cluster");
            finger_table.set_all_fingers(&own_grpc_address_str);
            tx.send((finger_table, predecessor.into())).unwrap();
        }
    };

    Ok(())
}
