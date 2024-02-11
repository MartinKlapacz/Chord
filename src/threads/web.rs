use actix_web::{App, get, HttpResponse, post, Responder, web};
use tera::{Tera, Context};

#[get("/")]
pub async fn index() -> impl Responder {
    let tera = Tera::new("templates/**/*").unwrap();

    let mut context = Context::new();
    context.insert("title", "My Web Page");
    context.insert("greeting", "Hello, Actix-web with Tera!");

    let rendered_html = tera.render("index.html", &context).unwrap();

    HttpResponse::Ok()
        .content_type("text/html")
        .body(rendered_html)
}

#[get("/{name}")]
pub async fn hello(name: web::Path<String>) -> impl Responder {
    format!("Hello {}!", &name)
}
