use std::error::Error;
use std::sync::{Arc, Mutex};

use log::{debug, info};
use tokio::sync::oneshot::Sender;
use tokio_stream::StreamExt;
use tonic::Request;
use chord::utils::crypto::Key;

use crate::kv::hash_map_store::HashMapStore;
use crate::kv::kv_store::KVStore;
use crate::node::finger_entry::FingerEntry;
use crate::node::finger_table::FingerTable;
use crate::threads::chord::Address;
use crate::threads::chord::chord_proto::{AddressMsg, Empty, UpdateFingerTableEntryRequest};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::utils::crypto::{hash, HashRingKey, HashPos};

pub async fn process_node_join(peer_address_option: Option<Address>, own_grpc_address_str: &String,
                               tx1: Sender<(Arc<Mutex<FingerTable>>, FingerEntry, Arc<Mutex<dyn KVStore + Send>>)>,
                               tx2: Sender<(Arc<Mutex<FingerTable>>, Arc<Mutex<dyn KVStore + Send>>)>) -> Result<(), Box<dyn Error>> {
    let own_id = hash(own_grpc_address_str.as_bytes());

    let mut finger_table = FingerTable::new(&own_id, own_grpc_address_str);
    let mut predecessor: AddressMsg = own_grpc_address_str.clone().into();
    let kv_store_arc = Arc::new(Mutex::new(HashMapStore::default()));

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
                *finger.get_address_mut() = response.get_ref().clone().into();
            }
            info!("Initialized finger table from peer");

            let direct_successor_url = finger_table.fingers.first().unwrap().get_address().clone();
            let mut direct_successor_client = ChordClient::connect(format!("http://{}", direct_successor_url))
                .await
                .unwrap();
            let get_predecessor_response = direct_successor_client.get_predecessor(Request::new(Empty {})).await.unwrap();
            predecessor = get_predecessor_response.get_ref().clone().into();
            info!("Received predecessor from peer");


            let mut kv_pair_stream = direct_successor_client.set_predecessor(Request::new(own_grpc_address_str.into()))
                .await
                .unwrap().into_inner();
            info!("Receiving data through data handoff...");
            while let Some(kv_pair) = kv_pair_stream.next().await {
                match kv_pair {
                    Ok(item) => {
                        let key: Key = Key::try_from(item.key.to_vec()).unwrap();
                        kv_store_arc.lock().unwrap().put(&key, &item.value);
                        debug!("Received KV pair ({:?}, {}) from successor", key, item.value);
                    }
                    Err(err) => {
                        println!("{}", err);
                    }
                }
            }
            info!("Data received, handoff finished!");


            // todo: store data and make available to grpc service
            let finger_entry_peer: FingerEntry = peer_address_str.into();
            let finger_entry_this: FingerEntry = own_grpc_address_str.into();
            info!("Updated predecessor of {:?} to {:?}", &finger_entry_peer, &finger_entry_this);

            let finger_table_len = finger_table.fingers.len();
            // finger table is constructed, send it to grpc thread and shotdown thread
            let finger_table_arc = Arc::new(Mutex::new(finger_table));
            tx1.send((finger_table_arc.clone(), predecessor.into(), kv_store_arc.clone())).unwrap();
            tx2.send((finger_table_arc, kv_store_arc)).unwrap();

            info!("Updating other nodes...");
            for index in 0..finger_table_len {
                let key_to_find_predecessor_for: HashPos = own_id.overflowing_sub(HashPos::two().overflowing_pow(index as u32).0).0;
                info!("Looking for predecessor for key: {} ", key_to_find_predecessor_for);
                let response = join_peer_client.find_predecessor(Request::new(key_to_find_predecessor_for.into()))
                    .await
                    .unwrap();
                let predecessor_to_update_address = response.get_ref().address.clone();
                info!("Predecessor for key {} is {}", key_to_find_predecessor_for, predecessor_to_update_address);

                let mut predecessor_to_update_client = ChordClient::connect(format!("http://{}", predecessor_to_update_address))
                    .await
                    .unwrap();
                info!("Calling update_finger_table on {} with index={}", predecessor_to_update_address, index);
                let _ = predecessor_to_update_client.update_finger_table_entry(Request::new(UpdateFingerTableEntryRequest {
                    index: index as u32,
                    finger_entry: Some(finger_entry_this.clone().into()),
                })).await.unwrap();
            }
            info!("Finished updating other nodes")
        }
        None => {
            info!("Starting up a new cluster");
            finger_table.set_all_fingers(&own_grpc_address_str);
            let finger_table_arc = Arc::new(Mutex::new(finger_table));

            tx1.send((finger_table_arc.clone(), predecessor.into(), kv_store_arc.clone())).unwrap();
            tx2.send((finger_table_arc, kv_store_arc)).unwrap();
        }
    };

    Ok(())
}
