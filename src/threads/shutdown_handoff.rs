use std::error::Error;
use std::sync::{Arc, Mutex};

use log::{error, info};
use tokio::signal;
use tokio::sync::oneshot::Receiver;
use tonic::Request;

use crate::kv::kv_store::KVStore;
use crate::node::successor_list::SuccessorList;
use crate::threads::chord::{Address, connect_with_retry};
use crate::threads::chord::chord_proto::Empty;

pub async fn shutdown_handoff(local_grpc_service_address: Address, rx: Receiver<Arc<Mutex<dyn KVStore + Send>>>) -> Result<(), Box<dyn Error>> {
    let finger_table_arc = rx.await.unwrap();

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

            // todo


        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    Ok(())
}
