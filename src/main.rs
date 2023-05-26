use std::error::Error;

use clap::Parser;
use log::{info, LevelFilter};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tonic::transport::Server;

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


    info!("Starting up tcp main thread");
    let tcp_thread_handle: JoinHandle<()> = tokio::spawn(async move {
        let listener = TcpListener::bind(tcp_addr).await.unwrap();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            info!("New client connection established");
            tokio::spawn(async move { handle_client_connection(socket).await.unwrap() });
        }
    });

    info!("Starting up gRPC service");
    let grpc_thread_handle = tokio::spawn(async move {
        let chord_service = ChordService::new(&grpc_addr, 32);
        Server::builder()
            .add_service(ChordServer::new(chord_service))
            .serve(grpc_addr.parse().unwrap())
    });

    tcp_thread_handle.await?;
    grpc_thread_handle.await?;

    Ok(())
}




