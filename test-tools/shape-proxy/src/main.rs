//! SV2 share-gating proxy for testing pool vardiff behavior.

mod api;
mod config;
mod downstream;
mod metrics;
mod profile;
mod proxy;
mod share_gate;
mod upstream;

use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(
    name = "shape-proxy",
    about = "SV2 share-gating proxy for vardiff testing"
)]
struct Args {
    #[arg(short, long, help = "Path to TOML configuration file")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    // Log filter: honor RUST_LOG when set (so e.g. RUST_LOG=shape_proxy=trace
    // enables the raw-arrival calibration trace), and fall back to
    // shape_proxy=info only when RUST_LOG is unset. Previously an
    // unconditional `add_directive("shape_proxy=info")` pinned shape_proxy at
    // info and silently overrode a RUST_LOG=shape_proxy=trace request.
    let env_filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => EnvFilter::new(v),
        _ => EnvFilter::new("shape_proxy=info"),
    };
    fmt().with_env_filter(env_filter).init();

    let args = Args::parse();

    let cfg = match config::Config::from_file(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    info!("Shape proxy starting");
    info!("  Upstream: {}", cfg.upstream_address);
    info!("  Downstream listen: {}", cfg.downstream_listen);
    info!("  Difficulty floor: {}", cfg.min_downstream_difficulty);
    info!("  API listen: {}", cfg.api_listen);

    let proxy = match proxy::ProxyCore::new(cfg) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to initialize proxy: {e}");
            std::process::exit(1);
        }
    };

    info!("Shape proxy running (upstream connection will be established in background)");

    if let Err(e) = proxy.run().await {
        error!("Proxy exited with error: {e}");
        std::process::exit(1);
    }
}
