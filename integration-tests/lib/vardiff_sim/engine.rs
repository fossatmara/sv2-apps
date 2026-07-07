//! Fleet engine: spawns simulated miners, applies drift models and scenario
//! events, and aggregates live per-miner statistics for the CLI/TUI.

use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    net::SocketAddr,
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use rand::Rng;
use stratum_apps::key_utils::Secp256k1PublicKey;
use tracing::info;

use super::{
    expected_shares_per_minute, miner::run_miner, scenario::{Drift, DriftMode},
    target_le_to_difficulty, MinerCommand, MinerConfig, MinerEvent,
};

/// Window over which the realized share rate is measured.
const REALIZED_RATE_WINDOW: Duration = Duration::from_secs(60);

pub struct EngineConfig {
    pub pool_address: SocketAddr,
    pub authority_pubkey: Option<Secp256k1PublicKey>,
}

struct MinerSlot {
    config: MinerConfig,
    commands: Sender<MinerCommand>,
    drift: Option<DriftState>,
}

struct DriftState {
    spec: Drift,
    base_hashrate: f64,
    /// Multiplier accumulated by the random walk.
    walk: f64,
    last_step: Instant,
}

/// Live statistics for one miner, updated from its event stream.
#[derive(Debug, Clone, Default)]
pub struct MinerStats {
    pub connected: bool,
    pub hashrate: f64,
    pub reported_hashrate: f64,
    pub channel_id: Option<u32>,
    pub difficulty: f64,
    pub expected_spm: f64,
    pub submitted: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub target_updates: u64,
    pub disconnects: u64,
    pub last_error: Option<String>,
    share_times: VecDeque<Instant>,
    /// (elapsed_secs, difficulty) at every target change, for plotting.
    pub difficulty_history: Vec<(f64, f64)>,
}

impl MinerStats {
    /// Shares per minute over the trailing window.
    pub fn realized_spm(&self) -> f64 {
        let now = Instant::now();
        let in_window = self
            .share_times
            .iter()
            .filter(|t| now.duration_since(**t) <= REALIZED_RATE_WINDOW)
            .count();
        in_window as f64 * 60.0 / REALIZED_RATE_WINDOW.as_secs_f64()
    }
}

pub struct SimEngine {
    config: EngineConfig,
    miners: HashMap<String, MinerSlot>,
    pub stats: HashMap<String, MinerStats>,
    events_tx: Sender<(String, MinerEvent)>,
    events_rx: Receiver<(String, MinerEvent)>,
    started: Instant,
}

impl SimEngine {
    pub fn new(config: EngineConfig) -> Self {
        let (events_tx, events_rx) = async_channel::unbounded();
        Self {
            config,
            miners: HashMap::new(),
            stats: HashMap::new(),
            events_tx,
            events_rx,
            started: Instant::now(),
        }
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    /// Names of all miners ever spawned, sorted for stable display.
    pub fn miner_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.stats.keys().cloned().collect();
        names.sort();
        names
    }

    /// Spawns a new simulated miner task.
    pub fn spawn_miner(&mut self, config: MinerConfig, drift: Option<Drift>) {
        let (cmd_tx, cmd_rx) = async_channel::unbounded();
        let stats = self.stats.entry(config.name.clone()).or_default();
        stats.hashrate = config.hashrate;
        stats.reported_hashrate = config.nominal_hashrate() as f64;
        stats.connected = true;

        let drift_state = drift.map(|spec| DriftState {
            base_hashrate: config.hashrate,
            walk: 1.0,
            last_step: Instant::now(),
            spec,
        });
        let slot = MinerSlot {
            config: config.clone(),
            commands: cmd_tx,
            drift: drift_state,
        };
        info!(
            "spawning simulated miner {} at {:.3e} H/s (reported {:.3e})",
            config.name,
            config.hashrate,
            config.nominal_hashrate()
        );
        self.miners.insert(config.name.clone(), slot);
        tokio::spawn(run_miner(
            config,
            self.config.pool_address,
            self.config.authority_pubkey,
            cmd_rx,
            self.events_tx.clone(),
        ));
    }

    pub fn set_hashrate(&mut self, name: &str, hashrate: f64) {
        if let Some(slot) = self.miners.get_mut(name) {
            slot.config.hashrate = hashrate;
            if let Some(drift) = slot.drift.as_mut() {
                drift.base_hashrate = hashrate;
                drift.walk = 1.0;
            }
            let _ = slot.commands.try_send(MinerCommand::SetHashrate(hashrate));
        }
    }

    /// Multiplies the miner's base hashrate (used by TUI +/- keys).
    pub fn scale_hashrate(&mut self, name: &str, factor: f64) {
        let current = self.miners.get(name).map(|s| s.config.hashrate);
        if let Some(h) = current {
            self.set_hashrate(name, h * factor);
        }
    }

    pub fn disconnect(&mut self, name: &str) {
        if let Some(slot) = self.miners.get(name) {
            let _ = slot.commands.try_send(MinerCommand::Disconnect);
        }
    }

    /// Disconnects a miner and drops it from the fleet and the stats table.
    pub fn remove_miner(&mut self, name: &str) {
        if let Some(slot) = self.miners.remove(name) {
            let _ = slot.commands.try_send(MinerCommand::Disconnect);
        }
        self.stats.remove(name);
    }

    /// Reconnects a disconnected miner, preserving its config and drift.
    pub fn reconnect(&mut self, name: &str) {
        let Some(slot) = self.miners.get(name) else {
            return;
        };
        if self.stats.get(name).map(|s| s.connected).unwrap_or(false) {
            return;
        }
        let config = slot.config.clone();
        let drift = slot.drift.as_ref().map(|d| d.spec.clone());
        self.spawn_miner(config, drift);
    }

    pub fn is_connected(&self, name: &str) -> bool {
        self.stats.get(name).map(|s| s.connected).unwrap_or(false)
    }

    /// Drains pending miner events into the stats tables. Call every tick.
    pub fn drain_events(&mut self) {
        let elapsed = self.elapsed_secs();
        while let Ok((name, event)) = self.events_rx.try_recv() {
            let stats = self.stats.entry(name.clone()).or_default();
            match event {
                MinerEvent::Connected => {
                    stats.connected = true;
                    stats.last_error = None;
                }
                MinerEvent::ChannelOpened { channel_id, target_le } => {
                    stats.channel_id = Some(channel_id);
                    stats.difficulty = target_le_to_difficulty(&target_le);
                    stats.expected_spm = expected_shares_per_minute(stats.hashrate, &target_le);
                    stats.difficulty_history.push((elapsed, stats.difficulty));
                }
                MinerEvent::TargetUpdated { target_le } => {
                    stats.difficulty = target_le_to_difficulty(&target_le);
                    stats.expected_spm = expected_shares_per_minute(stats.hashrate, &target_le);
                    stats.target_updates += 1;
                    stats.difficulty_history.push((elapsed, stats.difficulty));
                }
                MinerEvent::NewJob { .. } => {}
                MinerEvent::ShareSubmitted { .. } => {
                    stats.submitted += 1;
                    stats.share_times.push_back(Instant::now());
                    while stats.share_times.len() > 10_000 {
                        stats.share_times.pop_front();
                    }
                }
                MinerEvent::SharesAccepted { count } => {
                    stats.accepted += count as u64;
                }
                MinerEvent::ShareRejected { code } => {
                    stats.rejected += 1;
                    stats.last_error = Some(code);
                }
                MinerEvent::HashrateChanged { hashrate } => {
                    stats.hashrate = hashrate;
                }
                MinerEvent::Disconnected { reason } => {
                    stats.connected = false;
                    stats.channel_id = None;
                    stats.disconnects += 1;
                    stats.last_error = Some(reason);
                }
            }
        }
    }

    /// Applies drift models; call every tick.
    pub fn apply_drift(&mut self) {
        let elapsed = self.elapsed_secs();
        let mut updates: Vec<(String, f64)> = Vec::new();
        for (name, slot) in self.miners.iter_mut() {
            let Some(drift) = slot.drift.as_mut() else {
                continue;
            };
            if drift.last_step.elapsed().as_secs_f64() < drift.spec.step_secs {
                continue;
            }
            drift.last_step = Instant::now();
            let hashrate = match drift.spec.mode {
                DriftMode::Sine => {
                    let phase = elapsed * std::f64::consts::TAU / drift.spec.period_secs;
                    drift.base_hashrate * (1.0 + drift.spec.amplitude * phase.sin())
                }
                DriftMode::RandomWalk => {
                    let step: f64 = rand::thread_rng()
                        .gen_range(-drift.spec.amplitude..drift.spec.amplitude)
                        * 0.1;
                    drift.walk = (drift.walk * (1.0 + step))
                        .clamp(1.0 - drift.spec.amplitude, 1.0 + drift.spec.amplitude);
                    drift.base_hashrate * drift.walk
                }
            };
            updates.push((name.clone(), hashrate));
        }
        for (name, hashrate) in updates {
            if let Some(slot) = self.miners.get(&name) {
                let _ = slot.commands.try_send(MinerCommand::SetHashrate(hashrate));
            }
        }
    }

    /// One-line status summary per miner (headless mode).
    pub fn summary_lines(&self) -> Vec<String> {
        self.miner_names()
            .iter()
            .map(|name| {
                let s = &self.stats[name];
                format!(
                    "{:<12} {} hashrate={:>10} diff={:>12.4} expected={:>6.2}/min realized={:>6.2}/min submitted={} accepted={} rejected={} target_updates={}",
                    name,
                    if s.connected { "up  " } else { "DOWN" },
                    format_hashrate(s.hashrate),
                    s.difficulty,
                    s.expected_spm,
                    s.realized_spm(),
                    s.submitted,
                    s.accepted,
                    s.rejected,
                    s.target_updates,
                ) + &s
                    .last_error
                    .as_ref()
                    .map(|e| format!(" last_error={e}"))
                    .unwrap_or_default()
            })
            .collect()
    }
}

/// Appends one row per miner per tick; plot difficulty/rates over time to
/// compare vardiff algorithms.
pub struct CsvWriter {
    file: std::fs::File,
}

impl CsvWriter {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        let mut file = std::fs::File::create(path)?;
        writeln!(
            file,
            "t_secs,miner,connected,hashrate,difficulty,expected_spm,realized_spm,submitted,accepted,rejected,target_updates"
        )?;
        Ok(Self { file })
    }

    pub fn write_tick(&mut self, engine: &SimEngine) -> std::io::Result<()> {
        let t = engine.elapsed_secs();
        for name in engine.miner_names() {
            let s = &engine.stats[&name];
            writeln!(
                self.file,
                "{t:.1},{name},{},{},{},{},{},{},{},{},{}",
                s.connected as u8,
                s.hashrate,
                s.difficulty,
                s.expected_spm,
                s.realized_spm(),
                s.submitted,
                s.accepted,
                s.rejected,
                s.target_updates,
            )?;
        }
        Ok(())
    }
}

pub fn format_hashrate(h: f64) -> String {
    const UNITS: [(f64, &str); 5] = [
        (1e15, "PH/s"),
        (1e12, "TH/s"),
        (1e9, "GH/s"),
        (1e6, "MH/s"),
        (1e3, "kH/s"),
    ];
    for (scale, unit) in UNITS {
        if h >= scale {
            return format!("{:.2} {unit}", h / scale);
        }
    }
    format!("{h:.0} H/s")
}
