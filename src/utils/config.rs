use std::str::FromStr;
use clap::Parser;
use ini::{Error, Ini};
use log::LevelFilter;
use crate::utils::constants::POW_DIFFICULTY_DEFAULT;

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
    pub pow_difficulty: usize,
    pub log_level_filter: LevelFilter,
    pub dev_mode: bool
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

        let pow_difficulty = dht
            .get("pow_difficulty")
            .map(|pow_difficulty| pow_difficulty.parse::<usize>().unwrap())
            .unwrap_or(POW_DIFFICULTY_DEFAULT);

        let log_level_filter = dht
            .get("log_level")
            .map(|log_level| LevelFilter::from_str(log_level))
            .map(|log_level| log_level.expect("Invalid log level"))
            .unwrap_or(LevelFilter::Info);
        
        let dev_mode = dht
            .get("dev_mode")
            .map(|dev_mode| bool::from_str(dev_mode))
            .map(|dev_mode| dev_mode.expect("Invalid dev mode argument, use true or false"))
            .unwrap_or(false);

        Ok(Config { p2p_address, api_address, join_address, pow_difficulty, log_level_filter, dev_mode })
    }
}
