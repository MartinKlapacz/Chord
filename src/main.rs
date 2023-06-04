use std::error::Error;

use clap::Parser;
use log::{info, LevelFilter};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;

use crate::chord::{ChordService, Address};
use crate::chord::chord_proto::chord_server::ChordServer;
use crate::cli::Cli;
use crate::finger_table::{FingerEntry, FingerTable};
use crate::join::process_node_join;
use crate::tcp_service::handle_client_connection;

mod chord;
mod tcp_service;
mod crypto;
mod cli;
mod finger_table;
mod join;

static DHT_PUT: u16 = 650;
static DHT_GET: u16 = 651;
static DHT_SUCCESS: u16 = 652;
static DHT_FAILURE: u16 = 653;

pub mod chord_proto {
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("chord_descriptor");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    simple_logger::SimpleLogger::new().env().with_level(LevelFilter::Info).init().unwrap();

    let tcp_addr = args.tcp_address;

    let mut thread_handles = Vec::new();

    let peer_address_option = args.peer;
    let cloned_grpc_addr_1 = args.grpc_address.clone();
    let cloned_grpc_addr_2 = args.grpc_address.clone();

    let (tx, rx) = oneshot::channel();

    info!("Starting up setup thread");
    thread_handles.push(tokio::spawn(async move {
        process_node_join(peer_address_option, cloned_grpc_addr_1, tx)
            .await
            .unwrap();
    }));


    info!("Starting up tcp main thread");
    thread_handles.push(tokio::spawn(async move {
        let listener = TcpListener::bind(tcp_addr).await.unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            info!("New client connection established");
            tokio::spawn(async move { handle_client_connection(socket).await.unwrap() });
        }
    }));

    info!("Starting up gRPC service");
    thread_handles.push(tokio::spawn(async move {
        let chord_service = ChordServer::new(ChordService::new(rx, &cloned_grpc_addr_2).await);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(chord_proto::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        Server::builder()
            .add_service(chord_service)
            .add_service(reflection_service)
            .serve(cloned_grpc_addr_2.parse().unwrap())
            .await
            .unwrap();
    }));

    for handle in thread_handles {
        let _ = handle.await?;
    }

    Ok(())
}




