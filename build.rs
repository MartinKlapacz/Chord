extern crate core;

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/Chord.proto")?;
    Ok(())
}
