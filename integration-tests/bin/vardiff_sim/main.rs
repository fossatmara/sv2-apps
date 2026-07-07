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

mod http;
mod hub;
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
    /// Production-like default: share-driven evaluation with evidence
    /// gating no longer needs a high rate to be responsive, so the sim
    /// mirrors the statistics a real pool sees. Raise it (e.g. 60) when you
    /// deliberately want dense windows.
    #[arg(long, default_value_t = 4.0)]
    shares_per_minute: f32,

    /// Idle-backstop interval (seconds). Difficulty adjusts share-driven;
    /// this timer only rescues channels producing no shares (difficulty far
    /// too high). The classic algorithm additionally skips windows of 15s or
    /// less.
    #[arg(long, default_value_t = 20)]
    vardiff_interval: u64,

    /// Vardiff algorithm for the spawned pool: "classic", "pid" or "qpid".
    /// Defaults to qpid (PID with Q-learning gain scheduling): reacts
    /// share-driven within seconds, while classic's time-threshold ladder is
    /// minute-scale by design.
    #[arg(long, default_value = "qpid")]
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

    /// Serve the web dashboard on this address (e.g. 127.0.0.1:8080). The
    /// dashboard mirrors the TUI (stats table, difficulty chart, controls)
    /// and works alongside it or headless.
    #[arg(long)]
    http: Option<SocketAddr>,

    /// Access token for the dashboard and API (Bearer header or ?token=
    /// query). When omitted, the dashboard requires no auth.
    #[arg(long)]
    http_token: Option<String>,

    /// Session-hub mode: serve this address publicly and give every browser
    /// session its own fully isolated child sim (own pool, template
    /// provider, clock, Q-table). All other sim flags are forwarded to the
    /// children. Idle sessions are reaped after --session-ttl-secs.
    #[arg(long, conflicts_with_all = ["http", "tui", "scenario", "pool"])]
    hub: Option<SocketAddr>,

    /// Idle TTL in seconds for hub sessions (no WebSocket + no requests).
    #[arg(long, default_value_t = 600)]
    session_ttl_secs: u64,

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
    if !args.tui && scenario.is_none() && args.http.is_none() && args.hub.is_none() {
        eprintln!("error: headless mode needs --scenario, --http, --hub, or --tui");
        std::process::exit(1);
    }

    let mut csv = args.csv.as_ref().map(|p| {
        CsvWriter::create(p).unwrap_or_else(|e| {
            eprintln!("error: cannot create {}: {e}", p.display());
            std::process::exit(1);
        })
    });

    if let Some(addr) = args.hub {
        let mut child_args: Vec<String> = vec![
            "--algorithm".into(), args.algorithm.clone(),
            "--shares-per-minute".into(), args.shares_per_minute.to_string(),
            "--vardiff-interval".into(), args.vardiff_interval.to_string(),
            "--speed".into(), args.speed.to_string(),
            "--miners".into(), args.miners.to_string(),
            "--hashrate".into(), args.hashrate.to_string(),
        ];
        for (flag, v) in [
            ("--kp", args.kp), ("--ki", args.ki), ("--kd", args.kd),
            ("--max-step", args.max_step), ("--deadband", args.deadband),
            ("--significance-z", args.significance_z),
            ("--tracking-secs", args.tracking_secs),
            ("--q-alpha", args.q_alpha), ("--q-gamma", args.q_gamma),
            ("--q-epsilon", args.q_epsilon),
        ] {
            if let Some(v) = v {
                child_args.push(flag.into());
                child_args.push(v.to_string());
            }
        }
        let shutdown = spawn_signal_listener();
        hub::serve(
            hub::HubConfig {
                addr,
                token: args.http_token.clone(),
                ttl: Duration::from_secs(args.session_ttl_secs),
                child_args,
            },
            shutdown,
        )
        .await;
        std::process::exit(0);
    }

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

    let engine = std::sync::Arc::new(std::sync::Mutex::new(SimEngine::new(EngineConfig {
        pool_address,
        authority_pubkey,
    })));
    let shutdown_signal = spawn_signal_listener();

    if let Some(addr) = args.http {
        tokio::spawn(http::serve(
            addr,
            http::HttpState {
                engine: engine.clone(),
                speed_control: args.spawn_pool,
                token: args.http_token.clone(),
            },
        ));
    }

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
            engine.clone(),
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

    // Headless: scenario-driven, or dashboard-driven when only --http was
    // given (default fleet, runs until a signal or --duration).
    let mut driver = scenario.map(|s| {
        println!("running scenario '{}' against {pool_address}", s.name);
        ScenarioDriver::new(s)
    });
    if driver.is_none() {
        let mut eng = engine.lock().expect("engine lock");
        for i in 0..args.miners {
            eng.spawn_miner(
                MinerConfig {
                    name: format!("sim-{i}"),
                    hashrate: args.hashrate,
                    reported_hashrate: None,
                },
                None,
            );
        }
        println!(
            "dashboard mode: {} miners against {pool_address}; ctrl-c to stop",
            args.miners
        );
    }
    let duration = args
        .duration
        .or_else(|| driver.as_ref().and_then(|d| d.scenario().duration_secs))
        .or(if driver.is_some() { Some(300) } else { None });

    let mut last_print = 0u64;
    loop {
        // One iteration per *virtual* second: at speed 8 the loop runs 8x
        // faster in wall time, keeping one CSV row per simulated second.
        let wall_secs = (1.0 / engine.lock().expect("engine lock").speed()).clamp(0.05, 10.0);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(wall_secs)) => {}
            _ = shutdown_signal.recv() => {
                println!("signal received, shutting down...");
                break;
            }
        }
        let mut eng = engine.lock().expect("engine lock");
        let elapsed = eng.elapsed_secs();
        eng.drain_events();
        if let Some(driver) = driver.as_mut() {
            for action in driver.due_actions(elapsed) {
                apply_action(&mut eng, action);
            }
        }
        eng.apply_drift();
        eng.drain_events();
        if let Some(csv) = csv.as_mut() {
            if let Err(e) = csv.write_tick(&eng) {
                eprintln!("csv write failed: {e}");
            }
        }
        let elapsed_whole = elapsed as u64;
        if elapsed_whole >= last_print + 10 {
            last_print = elapsed_whole;
            println!("--- t={elapsed_whole}s");
            for line in eng.summary_lines() {
                println!("{line}");
            }
        }
        drop(eng);
        if duration.is_some_and(|d| elapsed >= d as f64) {
            break;
        }
    }

    // Scoped so clippy can see the guard is not held across the await.
    {
        let eng = engine.lock().expect("engine lock");
        println!("=== final state at t={:.0}s", eng.elapsed_secs());
        for line in eng.summary_lines() {
            println!("{line}");
        }
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
