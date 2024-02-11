use std::error::Error;
use std::process::exit;

use actix_web::{App, get, HttpResponse, HttpServer, post, Responder, web};
use log::{error, info};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Server;

use chord::utils::config::Config;

use crate::threads::chord::chord_proto::chord_server::ChordServer;
use crate::threads::chord::ChordService;
use crate::threads::client_api::handle_client_connection;
use crate::threads::fix_fingers::fix_fingers_periodically;
use crate::threads::health::check_predecessor_health_periodically;
use crate::threads::setup::setup;
use crate::threads::shutdown_handoff::shutdown_handoff;
use crate::threads::stabilize::stabilize_periodically;
use crate::threads::successor_list::check_successor_list_periodically;
use crate::threads::web::index;

mod node;
mod utils;
mod threads;

#[allow(warnings, unused, unused_imports, unused_import_braces, re)]
pub mod chord_proto {
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("chord_descriptor");
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load().unwrap();

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(config.log_level_filter)
        .init()
        .unwrap();
    let config_clone = config.clone();

    let api_address = config.api_address;
    let p2p_address = config.p2p_address;
    let web_address = config.web_address;
    let join_address_option = config.join_address;
    let pow_difficulty = config.pow_difficulty;
    let dev_mode = config.dev_mode;

    let mut thread_handles = Vec::new();

    // Most threads need the address to the local gRPC service. Each thread needs an own variable
    // with that address as it needs to be moved into the thread
    let cloned_grpc_addr_1 = p2p_address.clone();
    let cloned_grpc_addr_2 = p2p_address.clone();
    let cloned_grpc_addr_3 = p2p_address.clone();
    let cloned_grpc_addr_4 = p2p_address.clone();
    let cloned_grpc_addr_5 = p2p_address.clone();
    let cloned_grpc_addr_6 = p2p_address.clone();
    let cloned_grpc_addr_7 = p2p_address.clone();
    let own_grpc_address_8 = p2p_address.clone();

    // tokio one-shot-channels used for communication between threads
    let (tx1, rx_grpc_service) = oneshot::channel();
    let (tx2, rx_shutdown_handoff) = oneshot::channel();
    let (tx3, rx_check_predecessor) = oneshot::channel();
    let (tx4, rx_successor_list) = oneshot::channel();
    let (tx5, rx_web_interface) = oneshot::channel();


    // the main thread starts up all other threads and finally awaits them

    thread_handles.push(tokio::spawn(async move {
        setup(join_address_option, &cloned_grpc_addr_1, tx1, tx2, tx3, tx4, tx5)
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
        let chord_service = ChordServer::new(ChordService::new(rx_grpc_service, &cloned_grpc_addr_2, pow_difficulty, dev_mode).await);
        info!("Starting up gRPC service on {}", cloned_grpc_addr_2);

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


    thread_handles.push(tokio::spawn(async move {
        shutdown_handoff(own_grpc_address_8.clone(), rx_shutdown_handoff).await.unwrap();
        exit(0)
    }));


    thread_handles.push(tokio::spawn(async move {
        fix_fingers_periodically(cloned_grpc_addr_4)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        stabilize_periodically(cloned_grpc_addr_5)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        check_predecessor_health_periodically(cloned_grpc_addr_6, rx_check_predecessor)
            .await
    }));


    thread_handles.push(tokio::spawn(async move {
        check_successor_list_periodically(cloned_grpc_addr_7, rx_successor_list)
            .await
    }));

    // Setup for web interface


    thread_handles.push(tokio::spawn(async move {
        info!("Starting up web interface  thread on {}", web_address);
        let finger_table_arc = rx_web_interface.await.unwrap();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(finger_table_arc.clone()))
                .app_data(web::Data::new(config_clone.clone()))
                .service(index)
        })
            .bind(web_address)
            .unwrap()
            .run();
        if let Err(e) = server.await {
            error!("Web server error: {}", e);
        }
    }));

    for handle in thread_handles {
        handle.await?;
    }

    Ok(())
}


