//! Interactive vardiff test harness.
//!
//! Drives a fleet of simulated miners (real SV2, fake work) against a pool to
//! exercise the vardiff control algorithm. The pool must run with
//! `ignore_share_validation = true`; pass `--spawn-pool` to have the harness
//! start a suitable pool + regtest template provider itself.
//!
//! Headless (repeatable, CSV output for comparing algorithms):
//!   vardiff-sim --spawn-pool --scenario scenarios/step.toml --csv out.csv
//!
//! Interactive TUI (watch vardiff converge, tweak hashrates live):
//!   vardiff-sim --spawn-pool --tui
//!   vardiff-sim --pool 127.0.0.1:34254 --tui

mod tui;

use std::{convert::TryFrom, net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use integration_tests_sv2::vardiff_sim::{
    engine::{CsvWriter, EngineConfig, SimEngine},
    pool::{start_sim_pool, AUTHORITY_PUBLIC_KEY},
    scenario::{DueAction, EventAction, Scenario, ScenarioDriver, ScenarioMiner},
    MinerConfig,
};
use stratum_apps::key_utils::Secp256k1PublicKey;

#[derive(Parser, Debug)]
#[command(name = "vardiff-sim", about = "Simulated-miner harness for vardiff testing")]
struct Args {
    /// Address of a running pool (must have ignore_share_validation = true).
    #[arg(long, conflicts_with = "spawn_pool")]
    pool: Option<SocketAddr>,

    /// Authority public key of the pool (base58). Defaults to the SRI test key.
    #[arg(long)]
    pubkey: Option<String>,

    /// Start a local template provider + pool configured for simulation.
    #[arg(long)]
    spawn_pool: bool,

    /// shares_per_minute for the spawned pool (vardiff setpoint).
    #[arg(long, default_value_t = 6.0)]
    shares_per_minute: f32,

    /// How often (seconds) the spawned pool re-evaluates channel difficulty.
    /// The classic algorithm skips windows of 15s or less, so the effective
    /// minimum is 16.
    #[arg(long, default_value_t = 60)]
    vardiff_interval: u64,

    /// Scenario file (TOML). Required in headless mode.
    #[arg(long)]
    scenario: Option<PathBuf>,

    /// Run the interactive TUI dashboard.
    #[arg(long)]
    tui: bool,

    /// Write per-miner stats to a CSV file, one row per miner per second.
    #[arg(long)]
    csv: Option<PathBuf>,

    /// Override run duration in seconds (headless mode).
    #[arg(long)]
    duration: Option<u64>,

    /// Number of default miners when starting the TUI without a scenario.
    #[arg(long, default_value_t = 3)]
    miners: usize,

    /// Hashrate (H/s) of the default TUI miners.
    #[arg(long, default_value_t = 100e12)]
    hashrate: f64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if !args.tui {
        integration_tests_sv2::start_tracing();
    }

    // Everything that can fail is resolved before the pool spawns, so no
    // error path has to tear it down.
    let scenario = args.scenario.as_ref().map(|p| {
        Scenario::load(p).unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        })
    });
    if !args.tui && scenario.is_none() {
        eprintln!("error: headless mode needs --scenario (or use --tui)");
        std::process::exit(1);
    }

    let mut csv = args.csv.as_ref().map(|p| {
        CsvWriter::create(p).unwrap_or_else(|e| {
            eprintln!("error: cannot create {}: {e}", p.display());
            std::process::exit(1);
        })
    });

    let pubkey_str = args.pubkey.as_deref().unwrap_or(AUTHORITY_PUBLIC_KEY);
    let authority_pubkey = Some(
        Secp256k1PublicKey::try_from(pubkey_str.to_string())
            .expect("invalid authority public key"),
    );

    // Keep the pool and template provider handles alive for the whole run;
    // both shut down when dropped at the end of main.
    let mut embedded_pool: Option<pool_sv2::PoolSv2> = None;
    let mut _template_provider: Option<integration_tests_sv2::template_provider::TemplateProvider> =
        None;
    let pool_address: SocketAddr = if args.spawn_pool {
        eprintln!("starting regtest template provider + pool (ignore_share_validation=true)...");
        let (pool, address, tp) =
            start_sim_pool(args.shares_per_minute, args.vardiff_interval).await;
        embedded_pool = Some(pool);
        _template_provider = Some(tp);
        eprintln!("pool listening on {address}");
        address
    } else {
        match args.pool {
            Some(a) => a,
            None => {
                eprintln!("error: pass --pool <addr> or --spawn-pool");
                std::process::exit(1);
            }
        }
    };

    let mut engine = SimEngine::new(EngineConfig {
        pool_address,
        authority_pubkey,
    });
    let shutdown_signal = spawn_signal_listener();

    if args.tui {
        let driver = scenario.map(ScenarioDriver::new);
        let default_fleet = if driver.is_none() {
            (0..args.miners)
                .map(|i| MinerConfig {
                    name: format!("sim-{i}"),
                    hashrate: args.hashrate,
                    reported_hashrate: None,
                })
                .collect()
        } else {
            Vec::new()
        };
        let result = tui::run(engine, driver, default_fleet, csv, shutdown_signal).await;
        shutdown_embedded_pool(embedded_pool, _template_provider).await;
        if let Err(e) = result {
            eprintln!("tui error: {e}");
            std::process::exit(1);
        }
        return;
    }

    // Headless mode (--scenario presence was checked before the pool spawned).
    let scenario = scenario.expect("checked above");
    println!("running scenario '{}' against {pool_address}", scenario.name);
    let mut driver = ScenarioDriver::new(scenario);
    let duration = args
        .duration
        .or(driver.scenario().duration_secs)
        .unwrap_or(300);

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut last_print = 0u64;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = shutdown_signal.recv() => {
                println!("signal received, shutting down...");
                break;
            }
        }
        let elapsed = engine.elapsed_secs();
        engine.drain_events();
        for action in driver.due_actions(elapsed) {
            apply_action(&mut engine, action);
        }
        engine.apply_drift();
        engine.drain_events();
        if let Some(csv) = csv.as_mut() {
            if let Err(e) = csv.write_tick(&engine) {
                eprintln!("csv write failed: {e}");
            }
        }
        let elapsed_whole = elapsed as u64;
        if elapsed_whole >= last_print + 10 {
            last_print = elapsed_whole;
            println!("--- t={elapsed_whole}s");
            for line in engine.summary_lines() {
                println!("{line}");
            }
        }
        if elapsed >= duration as f64 {
            break;
        }
    }

    println!("=== final state at t={:.0}s", engine.elapsed_secs());
    for line in engine.summary_lines() {
        println!("{line}");
    }
    shutdown_embedded_pool(embedded_pool, _template_provider).await;
}

/// Resolves once SIGINT (Ctrl-C) or SIGTERM arrives, so both run modes can
/// shut the embedded pool down instead of dying mid-flight and leaking the
/// template provider's child processes.
fn spawn_signal_listener() -> async_channel::Receiver<()> {
    let (tx, rx) = async_channel::bounded(1);
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
        }
        let _ = tx.send(()).await;
    });
    rx
}

/// Gracefully stops a `--spawn-pool` pool and its template provider (the
/// template provider's child processes are killed on drop).
async fn shutdown_embedded_pool(
    pool: Option<pool_sv2::PoolSv2>,
    template_provider: Option<integration_tests_sv2::template_provider::TemplateProvider>,
) {
    if let Some(pool) = pool {
        eprintln!("shutting down embedded pool...");
        pool.shutdown().await;
    }
    drop(template_provider);
}

pub(crate) fn apply_action(engine: &mut SimEngine, action: DueAction) {
    match action {
        DueAction::Start(m) => start_scenario_miner(engine, &m),
        DueAction::Apply { miner, action } => match action {
            EventAction::SetHashrate { hashrate } => engine.set_hashrate(&miner, hashrate),
            EventAction::Disconnect => engine.disconnect(&miner),
            EventAction::Reconnect => engine.reconnect(&miner),
        },
    }
}

pub(crate) fn start_scenario_miner(engine: &mut SimEngine, m: &ScenarioMiner) {
    engine.spawn_miner(
        MinerConfig {
            name: m.name.clone(),
            hashrate: m.hashrate,
            reported_hashrate: m.reported_hashrate,
        },
        m.drift.clone(),
    );
}
