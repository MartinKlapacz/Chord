extern crate core;

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/Chord.proto"], &["proto"])?;
    Ok(())
}
