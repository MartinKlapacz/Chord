use tonic::transport::Server;

use crate::chord::ChordService;

mod chord;

async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let chord_service = ChordService::default();

    Server::builder()
        .add_service(chord::chord_server::ChordServer::new(chord_service))
        .serve(addr)
        .await?;

    Ok(())
}
