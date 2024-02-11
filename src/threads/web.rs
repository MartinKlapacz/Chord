use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use tera::{Context, Tera};

use crate::node::finger_table::FingerTable;

#[get("/")]
pub async fn index(finger_table_data: web::Data<Arc<Mutex<FingerTable>>>) -> impl Responder {
// pub async fn index() -> impl Responder {
    let tera = Tera::new("templates/**/*").unwrap();

    let mut context = Context::new();
    context.insert("title", "My Web Page");
    context.insert("num", finger_table_data.lock().unwrap().fingers.len().to_string().as_str());
    // context.insert("fingers", finger_table_data.lock().unwrap().fingers);

    let rendered_html = tera.render("index.html", &context).unwrap();

    HttpResponse::Ok()
        .content_type("text/html")
        .body(rendered_html)
}

#[get("/{name}")]
pub async fn hello(name: web::Path<String>) -> impl Responder {
    format!("Hello {}!", &name)
}
