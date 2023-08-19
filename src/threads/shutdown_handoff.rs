use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use log::{error, info};
use tokio::signal;
use tokio::sync::oneshot::Receiver;
use tokio_stream::iter;
use tonic::Request;

use chord::utils::crypto::{hash, HashPos, is_between, Key};

use crate::kv::kv_store::{KVStore, Value};
use crate::node::successor_list::SuccessorList;
use crate::threads::chord::{Address, connect_to_first_reachable_node, connect_with_retry};
use crate::threads::chord::chord_proto::{Empty, KvPairMsg};
use crate::utils::crypto::HashRingKey;

pub async fn shutdown_handoff(local_grpc_service_address: Address, rx: Receiver<Arc<Mutex<HashMap<Key, Value>>>>) -> Result<(), Box<dyn Error>> {
    let kv_store_arc = rx.await.unwrap();
    let one = HashPos::one();


    let mut local_grpc_client = connect_with_retry(&local_grpc_service_address)
        .await
        .unwrap();
    info!("Shutdown handoff thread ready...");
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Preparing shutdown...");
            let successor_list: SuccessorList = local_grpc_client.get_successor_list(Request::new(Empty {}))
                .await
                .unwrap().into_inner().into();

            let (mut successor_client, successor_address) = connect_to_first_reachable_node(&successor_list.successors)
                .await
                .unwrap();
            info!("Selected successor for handoff");

            let mut counter = 0;
            let foo: Vec<KvPairMsg> = {
                let bar = kv_store_arc.lock().unwrap();
                bar.iter()
                    .filter(move |(key, _)| is_between(hash(*key), one + 1, one, false, false))
                    .inspect(|_| { counter += 1; })
                    .map(|(k, v)| {
                        KvPairMsg {
                            key: k.to_vec(),
                            value: v.to_string(),
                        }
                    })
                    .collect()
            };

            let _ = successor_client.handoff(Request::new(iter(foo))).await;
            info!("Transfered {} key-value-pairs to {}", counter, successor_address);
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    Ok(())
}
