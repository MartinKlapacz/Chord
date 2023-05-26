use std::error::Error;

use clap::Parser;
use log::{info, LevelFilter};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};

use crate::chord::chord_proto::chord_server::ChordServer;
use crate::chord::ChordService;
use crate::cli::Cli;
use crate::tcp_service::handle_client_connection;

mod chord;
mod tcp_service;
mod crypto;
mod cli;

static DHT_PUT: u16 = 650;
static DHT_GET: u16 = 651;
static DHT_SUCCESS: u16 = 652;
static DHT_FAILURE: u16 = 653;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    simple_logger::SimpleLogger::new().env().with_level(LevelFilter::Info).init().unwrap();

    let grpc_addr = args.address;
    let tcp_addr = "127.0.0.1:50052";

    let mut thread_handles = Vec::new();

    // if args.peer.is_some() {
    //     info!("Joining existing cluster");
    //     let peer_address = args.peer.unwrap().as_str();
    //     let channel = Channel::from_static(peer_address)
    //         .connect()
    //         .await?;
    // } else {
    //     info!("Starting new cluster")
    // }


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
        let chord_service = ChordService::new(&grpc_addr, 32);
        Server::builder()
            .add_service(ChordServer::new(chord_service))
            .serve(grpc_addr.parse().unwrap())
    }));

    for handle in thread_handles {
        let _ = handle.await?;
    }

    Ok(())
}




