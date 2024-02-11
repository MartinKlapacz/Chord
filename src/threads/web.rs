use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use tera::{Context, Tera};
use chord::utils::config::Config;
use chord::utils::types::HashPos;

use crate::node::finger_table::FingerTable;

#[get("/")]
pub async fn index(
    finger_table_data: web::Data<Arc<Mutex<FingerTable>>>,
    config: web::Data<Config>
) -> impl Responder {
    let tera = Tera::new("templates/**/*").unwrap();

    let mut context = Context::new();
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
