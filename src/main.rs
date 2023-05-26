use std::error::Error;
use std::io::ErrorKind;

use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tonic::transport::Server;

use crate::chord::chord_proto::chord_server::ChordServer;
use crate::chord::ChordService;

mod chord;
mod client;
mod tcp_server;

static DHT_PUT: u16 = 650;
static DHT_GET: u16 = 651;
static DHT_SUCCESS: u16 = 652;
static DHT_FAILURE: u16 = 653;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let grpc_addr = "127.0.0.1:50051".parse()?;
    let tcp_addr = "127.0.0.1:50052";

    let chord_service = ChordService::default();


    tokio::spawn(async move {
        let listener = TcpListener::bind(tcp_addr).await.unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let size = socket.read_u16().await.unwrap();
                let code = socket.read_u16().await.unwrap();
                match code {
                    code if code == DHT_PUT => handle_put(&mut socket, size).await,
                    code if code == DHT_GET => handle_get(&mut socket).await,
                    _ => panic!("invalid code")
                }.unwrap();
            });
        }
    });

    Server::builder()
        .add_service(ChordServer::new(chord_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}


async fn handle_client(mut socket: TcpStream) -> Result<(), Box<dyn Error>> {
    let size = socket.read_u16().await?;
    let code = socket.read_u16().await?;
    match code {
        code if code == DHT_PUT => handle_put(&mut socket, size).await,
        code if code == DHT_GET => handle_get(&mut socket).await,
        _ => panic!("invalid code {}", code)
    }?;
    Ok(())
}

async fn handle_get(socket: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut key: [u8; 32] = [0; 32];
    socket.read_exact(&mut key).await?;

    // todo: handle get
    send_dht_success(socket, key, vec![1, 2, 3, 4, 5, 6, 7]).await?;
    // send_dht_failure(socket, key).await?;

    Ok(())
}

async fn handle_put(mut socket: &TcpStream, size: u16) -> Result<(), Box<dyn Error>> {
    Ok(())
}

async fn send_dht_success(socket: &mut TcpStream, key: [u8; 32], value: Vec<u8>) -> Result<(), Box<dyn Error>> {
    let size = 36 + value.len() as u16;

    let mut buffer = Vec::new();
    buffer.extend_from_slice(&size.to_be_bytes());
    buffer.extend_from_slice(&DHT_SUCCESS.to_be_bytes());
    buffer.extend_from_slice(&key);
    buffer.extend_from_slice(&value);

    socket.write_all(&buffer).await?;
    Ok(())
}

async fn send_dht_failure(socket: &mut TcpStream, key: [u8; 32]) -> Result<(), Box<dyn Error>> {
    let size = 2 + 2 + 32 as u16;

    let mut buffer = Vec::new();
    buffer.extend_from_slice(&size.to_be_bytes());
    buffer.extend_from_slice(&DHT_FAILURE.to_be_bytes());
    buffer.extend_from_slice(&key);

    socket.write_all(&buffer).await?;
    Ok(())
}

