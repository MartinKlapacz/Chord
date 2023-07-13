use std::time::Duration;
use log::debug;
use tokio::time::sleep;
use tonic::Request;
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::threads::chord::chord_proto::Empty;

pub async fn fix_fingers(local_grpc_service_address: String) -> ! {
    let retry_connection_sleep_millis = 50;
    let retry_fix_fingers_sleep_millis = 1000;
    loop {
        match ChordClient::connect(format!("http://{}", local_grpc_service_address.clone())).await {
            Ok(mut client) => {
                loop {
                    client.fix_fingers(Request::new(Empty {}))
                        .await
                        .unwrap();
                    sleep(Duration::from_millis(retry_connection_sleep_millis)).await;
                }
            },
            Err(e) => {
                debug!("Failed connecting to local grpc service, retrying in {} millis", retry_fix_fingers_sleep_millis);
                sleep(Duration::from_millis(retry_fix_fingers_sleep_millis)).await
            }
        }
    }
}