use std::error::Error;
use std::io::ErrorKind;
use std::sync::Arc;

use log::{error, info, LevelFilter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;

use crate::chord::chord_proto::chord_server::ChordServer;
use crate::chord::ChordService;
use crate::tcp_service::handle_client_connection;

mod chord;
mod tcp_service;

static DHT_PUT: u16 = 650;
static DHT_GET: u16 = 651;
static DHT_SUCCESS: u16 = 652;
static DHT_FAILURE: u16 = 653;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::SimpleLogger::new().env().with_level(LevelFilter::Info).init().unwrap();

    let grpc_addr = "127.0.0.1:50051".parse()?;
    let tcp_addr = "127.0.0.1:50052";



    tokio::spawn(async move {
        info!("Starting up tcp main thread");
        let listener = TcpListener::bind(tcp_addr).await.unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            info!("New client connection established");
            tokio::spawn(async move { handle_client_connection(socket).await.unwrap() });
        }
    });

    info!("Starting up gRPC service");
    let chord_service = ChordService::default();
    Server::builder()
        .add_service(ChordServer::new(chord_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}




