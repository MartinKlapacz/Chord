use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use serde::Deserialize;
use tera::{Context, Tera};
use tonic::Request;
use tonic::transport::Channel;

use chord::utils::config::Config;
use chord::utils::types::HashPos;

use crate::node::finger_table::FingerTable;
use crate::threads::chord::chord_proto::{GetRequest, GetStatus};
use crate::threads::chord::chord_proto::chord_client::ChordClient;

#[derive(Deserialize)]
struct QueryParams {
    get_input: String,
}


#[get("/")]
pub async fn index(
    finger_table_data: web::Data<Arc<Mutex<FingerTable>>>,
    config: web::Data<Config>,
    grpc_client: web::Data<Arc<Mutex<ChordClient<Channel>>>>,
    query_params_option: Option<web::Query<QueryParams>>,
) -> impl Responder {
    let tera = Tera::new("static/html/**/*").unwrap();
    let mut context = Context::new();


    if query_params_option.is_some() {
        let get_key_string = query_params_option.unwrap().0.get_input;
        let mut key_array: [u8; 32] = [0; 32];
        for (i, c) in get_key_string.chars().enumerate() {
            key_array[i] = c as u8;
        }

        let response = grpc_client.lock().unwrap().get(Request::new(GetRequest {
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
