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
use crate::chord::chord_proto::{Empty, FindSuccessorRequest, GetPredecessorResponse};
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

    let tcp_addr = args.tcp_address;

    let mut thread_handles = Vec::new();

    let peer_address_option = args.peer;
    let cloned_grpc_addr_1 = args.grpc_address.clone();
    let cloned_grpc_addr_2 = args.grpc_address.clone();

    let (tx, rx) = oneshot::channel();

    info!("Starting up finger table thread");
    thread_handles.push(tokio::spawn(async move {
        let id = crypto::hash(&cloned_grpc_addr_1);

        let mut finger_table = FingerTable::new(&id);
        let mut predecessor_url = cloned_grpc_addr_1.clone();

        match peer_address_option {
            Some(peer_address) => {
                info!("Joining existing cluster");
                let mut client = ChordClient::connect(format!("http://{}", peer_address))
                    .await
                    .unwrap();

                for finger in &mut finger_table.fingers {
                    let bytes = finger.key.to_be_bytes().to_vec();
                    let response = client.find_successor(Request::new(FindSuccessorRequest {
                        id: bytes,
                    })).await.unwrap();
                    finger.url = response.get_ref().address.clone();
                }
                info!("Initialized finger table from peer");

                let response = client.get_predecessor(Request::new(Empty{})).await.unwrap();
                predecessor_url = response.get_ref().url.clone();
                info!("Received predecessor from peer")
            }
            None => {
                info!("Starting up a new cluster");
                finger_table.set_all_fingers(&cloned_grpc_addr_1);
            }
        };
        tx.send((finger_table, predecessor_url)).unwrap()
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
            .serve(cloned_grpc_addr_2.parse().unwrap())
            .await
            .unwrap();
    }));

    for handle in thread_handles {
        let _ = handle.await?;
    }

    Ok(())
}




