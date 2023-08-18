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
use crate::threads::chord::{Address, connect_with_retry};
use crate::threads::chord::chord_proto::{Empty, KvPairMsg};
use crate::utils::crypto::HashRingKey;

pub async fn shutdown_handoff(local_grpc_service_address: Address, rx: Receiver<Arc<Mutex<HashMap<Key, Value>>>>) -> Result<(), Box<dyn Error>> {
    let kv_store_arc = rx.await.unwrap();

    let mut local_grpc_client = connect_with_retry(&local_grpc_service_address)
        .await
        .unwrap();
    info!("Shutdown handoff thread ready...");
    match signal::ctrl_c().await {
        Ok(()) => {
            let successor_list: SuccessorList = local_grpc_client.get_successor_list(Request::new(Empty {}))
                .await
                .unwrap().into_inner().into();

            let mut successor_client = connect_with_retry(&successor_list.successors[0])
                .await
                .unwrap();

            let one = HashPos::one();
            let foo: Vec<KvPairMsg> = {
                let bar = kv_store_arc.lock().unwrap();
                bar.iter()
                    .filter(move |(key, _)| is_between(hash(*key), one + 1, one, false, false))
                    .map(|(k, v)| {
                        KvPairMsg {
                            key: k.to_vec(),
                            value: v.to_string(),
                        }
                    })
                    .collect()
            };

            let _ = successor_client.handoff(Request::new(iter(foo))).await;
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    Ok(())
}
