use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::debug;

use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;
use tonic::Request;
use chord::utils::constants::{CONNECTION_RETRY_UPON_FAILURE_MILLIS, HEALTH_SLEEP_MILLIS};
use crate::node::finger_entry::FingerEntry;
use crate::node::successor_list::SuccessorList;
use crate::threads::chord::{Address, connect_with_retry};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;


pub async fn check_successor_list_periodically(local_grpc_service_address: String, rx: Receiver<Arc<Mutex<SuccessorList>>>) -> ! {
    let successor_list_arc = rx.await.unwrap();
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut local_grpc_client) => {
                debug!("Connected to local grpc service");
                loop {
                    let successor_list: SuccessorList = local_grpc_client.get_successor_list(Request::new(Empty {}))
                        .await
                        .unwrap().into_inner().into();

                    let first_successor = successor_list.successors[0].clone();

                    match connect_with_retry(&first_successor).await {
                        Ok(mut successor_client) => {
                            let successors_successor_list: SuccessorList = successor_client.get_successor_list(Request::new(Empty{}))
                                .await
                                .unwrap().into_inner().into();
                            successor_list_arc.lock().unwrap().update_with_other_succ_list(successors_successor_list.clone());
                        },
                        Err(_) => {

                        }
                    }

                    sleep(Duration::from_millis(HEALTH_SLEEP_MILLIS)).await;
                }
            }
            Err(e) => {
                debug!("Failed connecting to local grpc service, retrying in {} millis", CONNECTION_RETRY_UPON_FAILURE_MILLIS);
                sleep(Duration::from_millis(CONNECTION_RETRY_UPON_FAILURE_MILLIS)).await
            }
        }
    }
}
