use std::arch::x86_64::_mm256_permute2f128_ps;
use std::error::Error;

use clap::Parser;
use log::{info, LevelFilter};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};
use tonic::transport::{Channel, Server};

use crate::chord::{ChordService, NodeUrl};
use crate::chord::chord_proto::chord_client::ChordClient;
use crate::chord::chord_proto::chord_server::ChordServer;
use crate::chord::chord_proto::FindSuccessorRequest;
use crate::cli::Cli;
use crate::finger_table::{FingerEntry, FingerTable};
use crate::tcp_service::handle_client_connection;

mod chord;
mod tcp_service;
mod crypto;
mod cli;
mod finger_table;

static DHT_PUT: u16 = 650;
static DHT_GET: u16 = 651;
static DHT_SUCCESS: u16 = 652;
static DHT_FAILURE: u16 = 653;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    simple_logger::SimpleLogger::new().env().with_level(LevelFilter::Info).init().unwrap();

    let tcp_addr = "127.0.0.1:50052";

    let mut thread_handles = Vec::new();

    let peer_address_option = args.peer;
    let grpc_addr1 = args.address.clone();
    let grpc_addr2 = args.address.clone();
    let finger_count = 32;

    let (tx, rx) = oneshot::channel();

    info!("Starting up finger table thread");
    thread_handles.push(tokio::spawn(async move {
        let id = crypto::hash(&grpc_addr1);

        let mut finger_table = FingerTable::new(&id, 32);

        match peer_address_option {
            Some(peer_address) => {
                info!("Joining an existing cluster");
                let channel = Channel::from_static("http://127.0.0.1:50051")
                    .connect()
                    .await
                    .unwrap();
                let mut client = ChordClient::new(channel);

                for finger in &mut finger_table.fingers {
                    let response = client.find_successor(Request::new(FindSuccessorRequest {
                        id: finger.key.to_be_bytes().to_vec(),
                    })).await.unwrap();
                    finger.url = response.get_ref().address.clone();
                }
            }
            None => {
                info!("Starting up a new cluster");
                finger_table.set_all_fingers(&grpc_addr1);
            }
        };
        tx.send(finger_table).unwrap()
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
        let chord_service = ChordService::new(rx).await;
        Server::builder()
            .add_service(ChordServer::new(chord_service))
            .serve(grpc_addr2.parse().unwrap())
            .await
            .unwrap();
    }));

    for handle in thread_handles {
        let _ = handle.await?;
    }

    Ok(())
}




