use std::error::Error;
use std::sync::{Arc, Mutex};

use log::info;
use tokio::sync::oneshot::Sender;
use tonic::Request;

use crate::kv::hash_map_store::HashMapStore;
use crate::kv::kv_store::KVStore;
use crate::node::finger_entry::FingerEntry;
use crate::node::finger_table::FingerTable;
use crate::threads::chord::Address;
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::HashPosMsg;
use crate::utils::crypto::hash;

pub async fn process_node_join(peer_address_option: Option<Address>, own_grpc_address_str: &String,
                               tx_grpc_thread: Sender<(Arc<Mutex<FingerTable>>, Arc<Mutex<Option<FingerEntry>>>, Arc<Mutex<dyn KVStore + Send>>)>,
                               tx_handoff_thread: Sender<(Arc<Mutex<FingerTable>>, Arc<Mutex<dyn KVStore + Send>>)>,
                               tx_check_predecessor: Sender<Arc<Mutex<Option<FingerEntry>>>>,
) -> Result<(), Box<dyn Error>> {
    let own_id = hash(own_grpc_address_str.as_bytes());

    let finger_table_arc = Arc::new(Mutex::new(FingerTable::new(&own_id)));
    let kv_store_arc = Arc::new(Mutex::new(HashMapStore::default()));
    let predecessor_option_arc = Arc::new(Mutex::new(None));

    match peer_address_option {
        Some(peer_address_str) => {
            info!("Joining existing cluster");
            let mut join_peer_client = ChordClient::connect(format!("http://{}", peer_address_str))
                .await
                .unwrap();
            let successor_address: Address = join_peer_client.find_successor(Request::new(HashPosMsg {
                key: own_id.to_be_bytes().to_vec(),
            })).await.unwrap().into_inner().into();
            finger_table_arc.lock().unwrap().fingers[0].address = successor_address;
        }
        None => {
            info!("Starting up a new cluster");
            finger_table_arc.lock().unwrap().fingers[0].address = own_grpc_address_str.clone();
        }
    };

    tx_grpc_thread.send((finger_table_arc.clone(), predecessor_option_arc.clone(), kv_store_arc.clone())).unwrap();
    tx_handoff_thread.send((finger_table_arc, kv_store_arc)).unwrap();
    tx_check_predecessor.send(predecessor_option_arc).unwrap();
    Ok(())
}
