use std::sync::{Arc, Mutex};

use actix_web::{get, HttpResponse, Responder, web};
use tera::{Context, Tera};

use crate::node::finger_table::FingerTable;

#[get("/")]
pub async fn index(finger_table_data: web::Data<Arc<Mutex<FingerTable>>>) -> impl Responder {
    let tera = Tera::new("templates/**/*").unwrap();

    let mut context = Context::new();
    context.insert("title", "My Web Page");

    let finger_table_guard = finger_table_data.lock().unwrap();
    context.insert("fingers", &finger_table_guard.fingers);

    let rendered_html = tera.render("index.html", &context).unwrap();

    HttpResponse::Ok()
        .content_type("text/html")
        .body(rendered_html)
}
