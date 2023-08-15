use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::debug;

use tokio::sync::oneshot::Receiver;
use tokio::time::sleep;
use tonic::Request;
use chord::utils::constants::HEALTH_SLEEP_MILLIS;
use crate::node::finger_entry::FingerEntry;

use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;
use crate::utils::constants::{CONNECTION_RETRY_UPON_FAILURE_MILLIS};

pub async fn check_predecessor_health_periodically(local_grpc_service_address: String, rx: Receiver<Arc<Mutex<Option<FingerEntry>>>>) -> ! {
    let predecessor_arc = rx.await.unwrap();

    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut local_grpc_client) => {
                debug!("Connected to local grpc service");
                loop {
                    let predecessor_address_msg_optional = local_grpc_client.get_predecessor(Request::new(Empty {}))
                        .await
                        .unwrap().into_inner().address_optional;

                    if let Some(predecessor_address_msg) = predecessor_address_msg_optional {
                        match ChordClient::connect(format!("http://{}", predecessor_address_msg.address)).await {
                            Ok(mut predecessor_client) => {
                                match predecessor_client.health(Request::new(Empty {})).await {
                                    Ok(response) => debug!("predecessor node healthy"),
                                    Err(_) => unset_predecessor(predecessor_arc.clone()).await
                                }
                            }
                            Err(_) => unset_predecessor(predecessor_arc.clone()).await
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

async fn unset_predecessor(predecessor_arc: Arc<Mutex<Option<FingerEntry>>>) -> () {
    debug!("Predecessor unavailable, setting predecessor to Nil");
    *predecessor_arc.lock().unwrap() = None;
}
