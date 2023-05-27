use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short = 't', long = "tcp")]
    pub tcp_address: String,

    #[arg(short = 'g', long = "grpc")]
    pub grpc_address: String,

    #[arg(short = 'p', long = "peer")]
    pub peer: Option<String>,
}

