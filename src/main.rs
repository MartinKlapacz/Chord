use std::error::Error;
use std::process::{exit, id};

use clap::Parser;
use log::{info, LevelFilter};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Server, ServerTlsConfig};

use crate::threads::chord::chord_proto::chord_server::ChordServer;
use crate::threads::chord::ChordService;
use crate::threads::fix_fingers::fix_fingers_periodically;
use crate::threads::health::check_predecessor_health_periodically;
use crate::threads::join::process_node_join;
use crate::threads::shutdown_handoff::shutdown_handoff;
use crate::threads::stabilize::stabilize_periodically;
use crate::threads::successor_list::check_successor_list_periodically;
use crate::threads::tcp_service::handle_client_connection;

use tonic::transport::Identity;
use chord::utils::config::Config;

mod node;
mod utils;
mod kv;
mod threads;


pub mod chord_proto {
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("chord_descriptor");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::SimpleLogger::new()
        .env()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let config = Config::load().unwrap();

    let api_address = config.api_address;
    let p2p_address = config.p2p_address;
    let join_address_option = config.join_address;

    let mut thread_handles = Vec::new();

    let cloned_grpc_addr_1 = p2p_address.clone();
    let cloned_grpc_addr_2 = p2p_address.clone();
    let cloned_grpc_addr_3 = p2p_address.clone();
    let cloned_grpc_addr_4 = p2p_address.clone();
    let cloned_grpc_addr_5 = p2p_address.clone();
    let cloned_grpc_addr_6 = p2p_address.clone();
    let cloned_grpc_addr_7 = p2p_address.clone();
    let own_grpc_address_8 = p2p_address.clone();

    let (tx1, rx_grpc_service) = oneshot::channel();
    let (tx2, rx_shutdown_handoff) = oneshot::channel();
    let (tx3, rx_check_predecessor) = oneshot::channel();
    let (tx4, rx_successor_list) = oneshot::channel();


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up setup thread");
        process_node_join(join_address_option, &cloned_grpc_addr_1, tx1, tx2, tx3, tx4)
            .await
            .unwrap();
    }));


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up tcp main thread on {}", api_address);
        let listener = TcpListener::bind(api_address).await.unwrap();
        loop {
            let grpc_address = cloned_grpc_addr_3.clone();
            let (socket, _) = listener.accept().await.unwrap();
            info!("New client connection established");
            tokio::spawn(async move { handle_client_connection(socket, &grpc_address).await.unwrap() });
        }
    }));

    thread_handles.push(tokio::spawn(async move {
        let cert = std::fs::read_to_string("certs/node1.crt").unwrap();
        let key = std::fs::read_to_string("certs/node1.key").unwrap();

        let chord_service = ChordServer::new(ChordService::new(rx_grpc_service, &cloned_grpc_addr_2).await);
        info!("Starting up gRPC service on {}", cloned_grpc_addr_2);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(chord_proto::FILE_DESCRIPTOR_SET)
            .build()
            .unwrap();

        let identity = Identity::from_pem(cert, key);

        Server::builder()
            // .tls_config(ServerTlsConfig::new().identity(identity))
            // .unwrap()
            .add_service(chord_service)
            .add_service(reflection_service)
            .serve(cloned_grpc_addr_2.parse().unwrap())
            .await
            .unwrap();
    }));

    thread_handles.push(tokio::spawn(async move {
        shutdown_handoff(own_grpc_address_8.clone(), rx_shutdown_handoff).await.unwrap();
        exit(0)
    }));


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up periodic fix_fingers thread");
        fix_fingers_periodically(cloned_grpc_addr_4)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up periodic stabilization thread");
        stabilize_periodically(cloned_grpc_addr_5)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up periodic predecessor health check thread");
        check_predecessor_health_periodically(cloned_grpc_addr_6, rx_check_predecessor)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up periodic successor list check thread");
        check_successor_list_periodically(cloned_grpc_addr_7, rx_successor_list)
            .await
    }));


    for handle in thread_handles {
        handle.await?;
    }

    Ok(())
}
