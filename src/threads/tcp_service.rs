use std::error::Error;
use std::io::ErrorKind;
use std::mem;

use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::Request;
use tonic::transport::Channel;

use crate::threads::chord::chord_proto::{GetRequest, GetStatus, HashPosMsg, PutRequest};
use crate::threads::chord::chord_proto::chord_client::ChordClient;
use crate::utils::constants::{DHT_FAILURE, DHT_GET, DHT_PUT, DHT_SUCCESS};
use crate::utils::crypto;
use crate::utils::types::HashPos;

pub async fn handle_client_connection(mut socket: TcpStream, grpc_address: &String) -> Result<(), Box<dyn Error>> {
    loop {
        let size = match socket.read_u16().await {
            Ok(0) => break,
            Ok(size) => size,
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                info!("Client disconnected");
                return Ok(());
            }
            _ => panic!("Unexpected Error")
        };
        let code = socket.read_u16().await.unwrap();
        match code {
            code if code == DHT_PUT => handle_put(&grpc_address, &mut socket, size).await,
            code if code == DHT_GET => handle_get(&grpc_address, &mut socket).await,
            _ => panic!("invalid code {}", code)
        }.unwrap();
    }
    Ok(())
}

async fn handle_get(grpc_address: &String, socket: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut key_array: [u8; 32] = [0; 32];
    socket.read_exact(&mut key_array).await?;
    info!("Processing GET for key {:?}", key_array);

    let mut responsible_node_client = perform_chord_look_up(
        &crypto::hash(key_array.as_slice()),
        grpc_address.as_str(),
    ).await;

    let response = responsible_node_client.get(Request::new(GetRequest {
        key: key_array.to_vec(),
    })).await.unwrap();

    match GetStatus::from_i32(response.get_ref().status) {
        Some(GetStatus::Ok) => {
            send_dht_success(socket, key_array, response.get_ref().value.as_bytes().to_vec()).await?;
        }
        Some(GetStatus::NotFound) => {
            send_dht_failure(socket, key_array).await?;
        }
        None => panic!("Received invalid get response status")
    }

    Ok(())
}

async fn handle_put(grpc_address: &String, socket: &mut TcpStream, size: u16) -> Result<(), Box<dyn Error>> {
    let ttl = socket.read_u16().await.unwrap();
    let replication = socket.read_u8().await.unwrap();
    let _reserved = socket.read_u8().await.unwrap();

    let mut key_array: [u8; 32] = [0; 32];
    socket.read_exact(&mut key_array).await?;
    let hash_ring_pos: HashPos = crypto::hash(key_array.as_slice());

    let remaining_msg_len: usize = size as usize
        - mem::size_of_val(&size)
        - mem::size_of_val(&DHT_PUT)
        - mem::size_of_val(&ttl)
        - mem::size_of_val(&replication)
        - mem::size_of_val(&_reserved)
        - mem::size_of_val(&key_array);

    let mut value_string = String::new();

    if socket.read_to_string(&mut value_string).await.unwrap() == remaining_msg_len {
        info!("Processing PUT for key {} and value {} ...", hash_ring_pos, value_string);

        let mut responsible_node_client = perform_chord_look_up(&hash_ring_pos, grpc_address.as_str())
            .await;

        let _ = responsible_node_client.put(Request::new(PutRequest {
            key: key_array.to_vec(),
            ttl: ttl as u64,
            replication: replication as u32,
            value: value_string,
        })).await.unwrap();

        Ok(())
    } else {
        panic!("Error reading string value")
    }
}

async fn perform_chord_look_up(key: &HashPos, grpc_address: &str) -> ChordClient<Channel> {
    let mut local_node_client: ChordClient<Channel> = ChordClient::connect(format!("http://{}", grpc_address))
        .await
        .unwrap();
    // todo: retry find_sucessor if error
    let response = local_node_client.find_successor(Request::new(HashPosMsg {
        key: key.to_be_bytes().to_vec()
    })).await.unwrap();

    let responsible_node_address = &response.get_ref().address;
    ChordClient::connect(format!("http://{}", responsible_node_address))
        .await
        .unwrap()
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
