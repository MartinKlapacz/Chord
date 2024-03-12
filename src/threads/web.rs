use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use serde::Deserialize;
use tera::{Context, Tera};

use chord::utils::config::Config;
use chord::utils::types::HashPos;

use crate::node::finger_table::FingerTable;

#[derive(Deserialize)]
struct QueryParams {
    get_input: String,
}


#[get("/")]
pub async fn index(
    finger_table_data: web::Data<Arc<Mutex<FingerTable>>>,
    config: web::Data<Config>,
    query_params_option: Option<web::Query<QueryParams>>,
) -> impl Responder {
    let tera = Tera::new("static/html/**/*").unwrap();
    let mut context = Context::new();

    if let Some(query_params) = query_params_option {
        // todo: get response from client
        context.insert("get_response", &query_params.0.get_input);
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
