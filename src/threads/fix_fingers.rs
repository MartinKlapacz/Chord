use std::time::Duration;

use log::debug;
use tokio::time::sleep;
use tonic::Request;

use crate::threads::chord::chord_proto::Empty;
use crate::threads::chord::connect_with_retry;
use crate::utils::constants::FIX_FINGERS_SLEEP_MILLIS;

pub async fn fix_fingers_periodically(local_grpc_service_address: String) -> ! {
    let mut client = connect_with_retry(&local_grpc_service_address).await.unwrap();
    debug!("Successfully connected to local grpc service");
    loop {
        client.fix_fingers(Request::new(Empty {}))
            .await
            .unwrap();
        sleep(Duration::from_millis(FIX_FINGERS_SLEEP_MILLIS)).await;
    }
}
