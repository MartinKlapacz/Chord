use std::error::Error;
use tokio::signal;
use tokio::sync::oneshot::Receiver;

use std::sync::{Arc, Mutex};

use log::info;
use crate::kv::kv_store::KVStore;
use crate::node::finger_table::FingerTable;

pub async fn shutdown_handoff(rx:  Receiver<(Arc<Mutex<FingerTable>>, Arc<Mutex<dyn KVStore + Send>>)>) -> Result<(), Box<dyn Error>> {
    let (finger_table_arc, kv_store_arc) = rx.await.unwrap();
    info!("shutdown handoff thread received finger table");
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("finish")
        },
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        },
    }
    Ok(())
}
