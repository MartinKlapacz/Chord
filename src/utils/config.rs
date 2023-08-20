use clap::Parser;
use ini::{Error, Ini};

use crate::utils::types::Address;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short = 'c', long = "config")]
    pub config_file: String,
}


#[derive(Clone, Debug)]
pub struct Config {
    pub api_address: Address,
    pub p2p_address: Address,
    pub join_address: Option<Address>,
}

impl Config {
    pub fn load() -> Result<Config, Error> {
        let conf = Ini::load_from_file(Cli::parse().config_file)?;

        let dht = conf.section(Some("dht"))
            .ok_or("'dht' section required")
            .unwrap();

        let p2p_address = dht
            .get("p2p_address")
            .ok_or("'p2p_address' value required")
            .unwrap()
            .to_string();

        let api_address = dht
            .get("api_address")
            .ok_or("'api_address' value required")
            .unwrap()
            .to_string();

        let join_address = dht
            .get("join_address")
            .map(|join_address_str| join_address_str.to_string());

        Ok(Config { p2p_address, api_address, join_address })
    }
}
