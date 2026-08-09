use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub upstream_address: String,
    pub upstream_authority_pubkey: Option<String>,
    pub downstream_listen: SocketAddr,
    pub authority_pubkey: String,
    pub authority_secret: String,
    #[serde(default = "default_cert_validity")]
    pub cert_validity_secs: u64,
    #[serde(default = "default_floor_difficulty")]
    pub min_downstream_difficulty: f64,
    #[serde(default = "default_api_listen")]
    pub api_listen: SocketAddr,
}

fn default_cert_validity() -> u64 {
    86400
}

fn default_floor_difficulty() -> f64 {
    0.0
}

fn default_api_listen() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}
