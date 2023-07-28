use std::time::Duration;

use log::debug;
use tokio::time::sleep;
use tonic::Request;

use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;
use crate::utils::constants::{CONNECTION_RETRY_UPON_FAILURE_MILLIS, STABILIZE_SLEEP_MILLIS};

pub async fn stabilize_periodically(local_grpc_service_address: String) -> ! {
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut client) => {
                debug!("Successfully connected to local grpc service");
                loop {
                    client.stabilize(Request::new(Empty {}))
                        .await
                        .unwrap();
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
