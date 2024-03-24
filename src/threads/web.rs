use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use actix_web::web::Query;
use serde::Deserialize;
use tera::{Context, Tera};
use tonic::Request;

use chord::utils::config::Config;
use chord::utils::types::HashPos;

use crate::node::finger_table::FingerTable;
use crate::threads::chord::chord_proto::{GetRequest, GetStatus, PutRequest};
use crate::threads::chord::connect_with_retry;

#[derive(Deserialize)]
struct QueryParams {
    get_input: Option<String>,
    put_key_input: Option<String>,
    put_value_input: Option<String>,
}


#[get("/")]
pub async fn index(
    finger_table_data: web::Data<Arc<Mutex<FingerTable>>>,
    config: web::Data<Config>,
    local_grpc_address: web::Data<String>,
    query_params_option: Option<Query<QueryParams>>,
) -> impl Responder {
    let tera = Tera::new("static/html/**/*").unwrap();
    let mut context = Context::new();

    if query_params_option.is_some() {
        match query_params_option.unwrap().0 {
            QueryParams {
                get_input: Some(get_input),
                put_key_input: None,
                put_value_input: None
            } => {
                perform_get_and_update_context(&get_input, &local_grpc_address, &mut context)
                    .await;
            }
            QueryParams {
                get_input: None,
                put_key_input: Some(put_key_input),
                put_value_input: Some(put_value_input)
            } => {
                perform_put_and_update_context(&put_key_input, put_value_input, &local_grpc_address, &mut context)
                    .await;
            }
            QueryParams { get_input: None, put_key_input: None, put_value_input: None } => {}
            _ => { panic!("Invalid query params") }
        }
    }

    context.insert("title", "Chord Node");

    let finger_table_guard = finger_table_data.lock().unwrap();

    context.insert("config", &config);
    context.insert("fingers", &finger_table_guard.fingers);
    context.insert("max_pos", &HashPos::MAX);

    let rendered_html = tera.render("index.html", &context).unwrap();

    HttpResponse::Ok()
        .content_type("text/html")
        .body(rendered_html)
}

async fn perform_get_and_update_context(key: &String, local_grpc_address: &String, context: &mut Context) {
    let mut local_chord_client = connect_with_retry(&local_grpc_address).await.unwrap();
    let mut key_array: [u8; 32] = [0; 32];
    for (i, c) in key.chars().enumerate() {
        key_array[i] = c as u8;
    }

    let response = local_chord_client.get(Request::new(GetRequest {
        key: key_array.to_vec(),
    })).await.unwrap();

    match GetStatus::from_i32(response.get_ref().status) {
        Some(GetStatus::Ok) => {
            context.insert("response_status", "OK");
            context.insert("get_response", &response.get_ref().value);
        }
        Some(GetStatus::NotFound) => {
            context.insert("response_status", "NOT_FOUND");
        }
        Some(GetStatus::Expired) => {
            context.insert("response_status", "EXPIRED");
        }
        _ => panic!("Received invalid get response status")
    }
}

async fn perform_put_and_update_context(key: &String, value: String, local_grpc_address: &String, context: &mut Context) {
    let mut local_chord_client = connect_with_retry(&local_grpc_address).await.unwrap();
    let mut key_array: [u8; 32] = [0; 32];
    for (i, c) in key.chars().enumerate() {
        key_array[i] = c as u8;
    }

    let _ = local_chord_client.put(Request::new(PutRequest {
        key: key_array.to_vec(),
        ttl: 100000,
        replication: 0,
        value,
    })).await.unwrap();
}
