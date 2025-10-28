//! Entry point for the Job Declarator Server (JDS).
//!
//! This binary parses CLI arguments, loads the TOML configuration file, and
//! starts the main runtime defined in `jd_server::JobDeclaratorServer`.
//!
//! The actual task orchestration and shutdown logic are managed in `lib/mod.rs`.
mod args;
use args::process_cli_args;
use jd_server::JobDeclaratorServer;
use stratum_apps::config_helpers::logging::init_logging;
use tracing::error;

/// Entrypoint for the Job Declarator Server binary.
///
/// Loads the configuration from TOML and initializes the main runtime
/// defined in `jd_server::JobDeclaratorServer`. Errors during startup are logged.
#[tokio::main]
async fn main() {
    let config = match process_cli_args() {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to process CLI arguments: {}", e);
            return;
        }
    };
    init_logging(config.log_file());
    let _ = JobDeclaratorServer::new(config).start().await;
}
