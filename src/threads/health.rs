use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::time::sleep;
use tokio::time::timeout;
use tonic::Request;
use crate::threads::chord::Address;

use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;
use crate::utils::constants::{CONNECTION_RETRY_UPON_FAILURE_MILLIS, STABILIZE_SLEEP_MILLIS};

pub async fn check_predecessor_health_periodically(local_grpc_service_address: String) -> ! {
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut client) => {
                loop {
                    let predecessor_address: Address = client.get_predecessor(Request::new(Empty {}))
                        .await
                        .unwrap().into_inner().into();

                    if predecessor_address.ne(&local_grpc_service_address) {
                        let mut predecessor_client = ChordClient::connect(format!("http://{}", local_grpc_service_address.clone()))
                            .await
                            .unwrap();

                        let timeout_result = timeout(
                            Duration::from_millis(2_000),
                            predecessor_client.health(Request::new(Empty {})),
                        ).await;

                        match timeout_result {
                            Ok(Ok(_)) => info!("Predecessor {} alive", predecessor_address),
                            Ok(Err(status)) => error!("RPC Error: {}", status),
                            Err(_) => warn!("Timeout Error: the request timed out"),
                        }
                    }

                    sleep(Duration::from_millis(STABILIZE_SLEEP_MILLIS)).await;
                }
            }
            Err(e) => {
                debug!("Failed connecting to local grpc service, retrying in {} millis", CONNECTION_RETRY_UPON_FAILURE_MILLIS);
                sleep(Duration::from_millis(CONNECTION_RETRY_UPON_FAILURE_MILLIS)).await
            }
        }
    }
}
