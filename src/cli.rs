use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short = 'a', long = "address")]
    pub address: String,

    #[arg(short = 'p', long = "peer")]
    pub peer: Option<String>,
}

