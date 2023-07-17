use std::time::Duration;

use log::debug;
use tokio::time::sleep;
use tonic::Request;

use crate::utils::constants::{CONNECTION_RETRY_UPON_FAILURE_MILLIS, FIX_FINGERS_SLEEP_MILLIS};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;

pub async fn fix_fingers_periodically(local_grpc_service_address: String) -> ! {
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut client) => {
                loop {
                    client.fix_fingers(Request::new(Empty {}))
                        .await
                        .unwrap();
                    sleep(Duration::from_millis(FIX_FINGERS_SLEEP_MILLIS)).await;
                }
            }
            Err(_) => {
                debug!("Failed connecting to local grpc service, retrying in {} millis", CONNECTION_RETRY_UPON_FAILURE_MILLIS);
                sleep(Duration::from_millis(CONNECTION_RETRY_UPON_FAILURE_MILLIS)).await
            }
        }
    }
}
