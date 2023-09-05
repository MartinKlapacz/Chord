use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{info, warn};

use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;
use tonic::Request;
use chord::utils::constants::HEALTH_SLEEP_MILLIS;
use crate::node::successor_list::{SUCCESSOR_LIST_SIZE, SuccessorList};
use crate::threads::chord::connect_with_retry;
use crate::threads::chord::chord_proto::Empty;

/// periodic successor list checking:
/// this function fetches the successor's successor list and updates this node's successor list
/// with the successor's successor list
pub async fn check_successor_list_periodically(local_grpc_service_address: String, rx: Receiver<Arc<Mutex<SuccessorList>>>) -> ! {
    let successor_list_arc = rx.await.unwrap();
    info!("Starting up periodic successor list check thread");

    let mut local_grpc_client = connect_with_retry(&local_grpc_service_address)
        .await
        .unwrap();

    loop {
        let successor_list: SuccessorList = local_grpc_client.get_successor_list(Request::new(Empty {}))
            .await
            .unwrap().into_inner().into();

        for i in 0..SUCCESSOR_LIST_SIZE {
            match connect_with_retry(&successor_list.successors[i]).await {
                Ok(mut successor_client) => {
                    let successors_successor_list: SuccessorList = successor_client.get_successor_list(Request::new(Empty{}))
                        .await
                        .unwrap().into_inner().into();
                    successor_list_arc.lock().unwrap().update_with_other_succ_list(successors_successor_list.clone());
                    break;
                },
                Err(_) => {
                    warn!("Cannot connect to {}-th successor, retrying with next...", i+1)
                }
            }
        }
        sleep(Duration::from_millis(HEALTH_SLEEP_MILLIS)).await;
    }
}
