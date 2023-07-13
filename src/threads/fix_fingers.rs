use std::time::Duration;
use log::debug;
use tokio::time::sleep;
use tonic::Request;
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;

const RETRY_CONNECTION_SLEEP_MILLIS: u64 = 50;
const RETRY_FIX_FINGERS_SLEEP_MILLIS: u64 = 1000;

pub async fn fix_fingers(local_grpc_service_address: String) -> ! {
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut client) => {
                loop {
                    client.fix_fingers(Request::new(Empty {}))
                        .await
                        .unwrap();
                    sleep(Duration::from_millis(RETRY_CONNECTION_SLEEP_MILLIS)).await;
                }
            },
            Err(e) => {
                debug!("Failed connecting to local grpc service, retrying in {} millis", RETRY_FIX_FINGERS_SLEEP_MILLIS);
                sleep(Duration::from_millis(RETRY_FIX_FINGERS_SLEEP_MILLIS)).await
            }
        }
    }
}
