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

    /// shares_per_minute for the spawned pool (vardiff setpoint). The
    /// interactive default is high so each vardiff window carries enough
    /// shares to react quickly; use production-like values (e.g. 6) when
    /// testing that regime deliberately.
    #[arg(long, default_value_t = 60.0)]
    shares_per_minute: f32,

    /// How often (seconds) the spawned pool re-evaluates channel difficulty.
    /// The classic algorithm skips windows of 15s or less (effective minimum
    /// 16); pid accepts any interval and gates on statistical significance.
    /// The interactive default is snappier than the production default (60).
    #[arg(long, default_value_t = 20)]
    vardiff_interval: u64,

    /// Vardiff algorithm for the spawned pool: "classic", "pid" or "qpid".
    #[arg(long, default_value = "classic")]
    algorithm: String,

    /// PID proportional gain (pid algorithm only).
    #[arg(long)]
    kp: Option<f64>,

    /// PID integral gain per second (pid algorithm only).
    #[arg(long)]
    ki: Option<f64>,

    /// PID derivative gain (pid algorithm only).
    #[arg(long)]
    kd: Option<f64>,

    /// Largest multiplicative difficulty change per update (pid only).
    #[arg(long)]
    max_step: Option<f64>,

    /// Integral-pressure deadband fraction (pid only).
    #[arg(long)]
    deadband: Option<f64>,

    /// Single-window significance threshold in sigmas (pid only).
    #[arg(long)]
    significance_z: Option<f64>,

    /// Anti-windup tracking time constant in seconds (pid only).
    #[arg(long)]
    tracking_secs: Option<f64>,

    /// Q-learning rate (qpid only).
    #[arg(long)]
    q_alpha: Option<f64>,

    /// Q-learning discount factor (qpid only).
    #[arg(long)]
    q_gamma: Option<f64>,

    /// Q-learning exploration rate (qpid only).
    #[arg(long)]
    q_epsilon: Option<f64>,

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

    /// Sim clock speed factor (e.g. 8 = one wall second counts as eight
    /// simulated seconds). Requires --spawn-pool: the pool's vardiff clock
    /// must live in this process to stay consistent. In the TUI, 1 and 2
    /// halve/double the speed live.
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
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
        let algorithm = match args.algorithm.as_str() {
            "classic" => pool_sv2::config::VardiffAlgorithm::Classic,
            "pid" => pool_sv2::config::VardiffAlgorithm::Pid,
            "qpid" => pool_sv2::config::VardiffAlgorithm::QPid,
            other => {
                eprintln!("error: unknown --algorithm '{other}' (use classic, pid or qpid)");
                std::process::exit(1);
            }
        };
        let mut vardiff = pool_sv2::config::VardiffConfig {
            algorithm,
            ..Default::default()
        };
        if let Some(v) = args.kp {
            vardiff.kp = v;
        }
        if let Some(v) = args.ki {
            vardiff.ki = v;
        }
        if let Some(v) = args.kd {
            vardiff.kd = v;
        }
        if let Some(v) = args.max_step {
            vardiff.max_step = v;
        }
        if let Some(v) = args.deadband {
            vardiff.deadband = v;
        }
        if let Some(v) = args.significance_z {
            vardiff.significance_z = v;
        }
        if let Some(v) = args.tracking_secs {
            vardiff.tracking_secs = v;
        }
        if let Some(v) = args.q_alpha {
            vardiff.alpha = v;
        }
        if let Some(v) = args.q_gamma {
            vardiff.gamma = v;
        }
        if let Some(v) = args.q_epsilon {
            vardiff.epsilon = v;
        }
        eprintln!(
            "starting regtest template provider + pool (ignore_share_validation=true, vardiff={})...",
            args.algorithm
        );
        let (pool, address, tp) =
            start_sim_pool(args.shares_per_minute, args.vardiff_interval, vardiff).await;
        embedded_pool = Some(pool);
        _template_provider = Some(tp);
        eprintln!("pool listening on {address}");
        address
    } else {
        let address = match args.pool {
            Some(a) => a,
            None => {
                eprintln!("error: pass --pool <addr> or --spawn-pool");
                std::process::exit(1);
            }
        };
        eprintln!("waiting for pool at {address}...");
        if !integration_tests_sv2::vardiff_sim::pool::wait_for_pool(
            address,
            Duration::from_secs(60),
        )
        .await
        {
            eprintln!("error: pool at {address} not reachable after 60s");
            std::process::exit(1);
        }
        address
    };

    if args.speed != 1.0 {
        if !args.spawn_pool {
            eprintln!(
                "error: --speed needs --spawn-pool (an external pool's vardiff clock \
                 cannot be accelerated from here, so results would be distorted)"
            );
            std::process::exit(1);
        }
        integration_tests_sv2::vardiff_sim::set_clock_speed(args.speed);
        eprintln!("sim clock speed x{:.2}", args.speed);
    }

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
        let result = tui::run(
            engine,
            driver,
            default_fleet,
            csv,
            shutdown_signal,
            args.spawn_pool,
        )
        .await;
        shutdown_embedded_pool(embedded_pool, _template_provider).await;
        if let Err(e) = result {
            eprintln!("tui error: {e}");
            std::process::exit(1);
        }
        // Exit explicitly: everything that matters was torn down above, and
        // dropping the tokio runtime can hang forever if a blocking-pool
        // thread is wedged in a syscall.
        std::process::exit(0);
    }

    // Headless mode (--scenario presence was checked before the pool spawned).
    let scenario = scenario.expect("checked above");
    println!("running scenario '{}' against {pool_address}", scenario.name);
    let mut driver = ScenarioDriver::new(scenario);
    let duration = args
        .duration
        .or(driver.scenario().duration_secs)
        .unwrap_or(300);

    let mut last_print = 0u64;
    loop {
        // One iteration per *virtual* second: at speed 8 the loop runs 8x
        // faster in wall time, keeping one CSV row per simulated second.
        let wall_secs = (1.0 / engine.speed()).clamp(0.05, 10.0);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(wall_secs)) => {}
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
    // See the TUI path: skip the runtime drop, which can hang on a wedged
    // blocking-pool thread.
    std::process::exit(0);
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
/// template provider's child processes are killed on drop). Every stage is
/// bounded by a timeout so a wedged component can't hang the exit forever.
async fn shutdown_embedded_pool(
    pool: Option<pool_sv2::PoolSv2>,
    template_provider: Option<integration_tests_sv2::template_provider::TemplateProvider>,
) {
    if let Some(pool) = pool {
        eprintln!("shutting down embedded pool...");
        match tokio::time::timeout(Duration::from_secs(10), pool.shutdown()).await {
            Ok(()) => eprintln!("pool stopped"),
            Err(_) => eprintln!("warning: pool did not shut down within 10s; continuing"),
        }
    }
    if let Some(tp) = template_provider {
        eprintln!("stopping template provider (bitcoind/sv2-tp)...");
        // Drop blocks on child-process teardown; run it off the async runtime
        // and bound it.
        let teardown = tokio::task::spawn_blocking(move || drop(tp));
        match tokio::time::timeout(Duration::from_secs(15), teardown).await {
            Ok(_) => eprintln!("template provider stopped"),
            Err(_) => eprintln!(
                "warning: template provider did not stop within 15s; \
                 check for leftovers with: pgrep -af 'bitcoin-node|sv2-tp'"
            ),
        }
    }
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
