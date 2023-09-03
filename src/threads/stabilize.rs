use std::time::Duration;

use log::{debug, info, warn};
use tokio::time::sleep;
use tonic::Request;

use crate::threads::chord::chord_proto::Empty;
use crate::threads::chord::connect_with_retry;
use crate::utils::constants::STABILIZE_SLEEP_MILLIS;

pub async fn stabilize_periodically(local_grpc_service_address: String) -> ! {
    info!("Starting up periodic stabilization thread");
    let mut client = connect_with_retry(&local_grpc_service_address).await.unwrap();
    debug!("Successfully connected to local grpc service");
    loop {
        match client.stabilize(Request::new(Empty {})).await {
            Err(error) => warn!("An error occured during stabilization: {}", error),
            _ => {}
        }
        sleep(Duration::from_millis(STABILIZE_SLEEP_MILLIS)).await;
    }
}
