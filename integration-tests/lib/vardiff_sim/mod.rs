//! Vardiff simulation framework.
//!
//! Simulated miners speak real Stratum V2 with the pool (noise handshake,
//! standard channels) but never hash: each miner models share discovery as a
//! Poisson process with rate `hashrate * (target / 2^256)` and submits dummy
//! shares at exponentially distributed intervals. The pool must run with
//! `ignore_share_validation = true` so the dummy shares are accepted and feed
//! the vardiff controller.
//!
//! Building blocks:
//! - [`miner`]: a single simulated miner task
//! - [`engine`]: fleet management, drift models, live stats
//! - [`scenario`]: TOML scenario files (step changes, churn, drift)

pub mod engine;
pub mod miner;
pub mod mock_tp;
pub mod pool;
pub mod scenario;

use std::time::Duration;

/// Sentinel `job_id` a simulated miner stamps on an invalid/stale share so
/// the pool (running with `ignore_share_validation`) can reject it without
/// real proof-of-work. The pool never issues this job id, so a genuine share
/// can never collide with it.
pub const BAD_SHARE_JOB_ID: u32 = u32::MAX;

/// Sentinel `job_id` a simulated miner stamps on a duplicate/replayed share so
/// the pool can reject it as a duplicate under the validation bypass. One less
/// than [`BAD_SHARE_JOB_ID`], and likewise never issued by the pool.
pub const DUPLICATE_SHARE_JOB_ID: u32 = u32::MAX - 1;

/// Commands the engine sends to a running miner task.
#[derive(Debug, Clone)]
pub enum MinerCommand {
    /// Change the simulated hashrate (H/s). Takes effect on the next share
    /// interval sample.
    SetHashrate(f64),
    /// Re-sample the share timer (e.g. after the sim clock speed changed, so
    /// a deadline scheduled at the old speed doesn't linger).
    Resample,
    /// Change the fraction of submitted shares that are invalid/stale.
    SetBadShareFraction(f64),
    /// Change the fraction of submitted shares that are duplicates/replays.
    SetDuplicateShareFraction(f64),
    /// Close the connection and end the miner task.
    Disconnect,
}

/// Current sim clock speed factor (1.0 = real time).
pub fn clock_speed() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::sim_clock::scale()
}

/// Sets the sim clock speed factor. Affects the vardiff algorithms and the
/// pool's vardiff loop in this process, so it is only meaningful with an
/// embedded (`--spawn-pool`) pool.
pub fn set_clock_speed(speed: f64) {
    stratum_apps::stratum_core::channels_sv2::vardiff::sim_clock::set_scale(speed);
}

/// Current virtual time as fractional seconds since the Unix epoch.
pub fn virtual_now_secs() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::sim_clock::now_secs_f64()
}

/// The PID base tuning the *active* algorithm actually runs when no live
/// override is set. qpid schedules gains around its own `tuned_base()` (a
/// higher confidence K and raised down-bar than the PID defaults), so a
/// dashboard that fell back to the plain PID defaults would misreport qpid's
/// real operating parameters. classic/pid/champion use the PID defaults.
fn active_base() -> stratum_apps::stratum_core::channels_sv2::vardiff::pid::PidParams {
    use stratum_apps::stratum_core::channels_sv2::vardiff::{qpid::QPidVardiffState, pid::PidParams};
    if algorithm() == "qpid" {
        QPidVardiffState::tuned_base()
    } else {
        PidParams::default()
    }
}

/// Current PID confidence shrinkage constant K (live-tunable; embedded
/// pool only, like the clock speed). Falls back to the active algorithm's base
/// K (see [`active_base`]) when no live override is set.
pub fn confidence_k() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::confidence_k_override()
        .unwrap_or_else(|| active_base().confidence_k)
}

/// Sets the PID confidence shrinkage constant K.
pub fn set_confidence_k(k: f64) {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::set_confidence_k(k)
}

/// Current PID significance threshold Z (the live override, or the active
/// algorithm's base value when untouched).
pub fn significance_z() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::significance_z_override()
        .unwrap_or_else(|| active_base().significance_z)
}

/// Sets the live PID significance-Z override.
pub fn set_significance_z(z: f64) {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::set_significance_z(z)
}

/// Current downward significance threshold (live override, or the active
/// algorithm's base value when untouched).
pub fn significance_z_down() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::significance_z_down_override()
        .unwrap_or_else(|| active_base().significance_z_down)
}

/// Sets the live downward significance override.
pub fn set_significance_z_down(z: f64) {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::set_significance_z_down(z)
}

/// Current vardiff setpoint in shares per minute (the live override; primed
/// by the sim CLI at startup so it is always set in simulator processes).
pub fn setpoint_spm() -> f64 {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::shares_per_minute_override()
        .unwrap_or(4.0)
}

/// Sets the live vardiff setpoint (shares per minute).
pub fn set_setpoint_spm(spm: f64) {
    stratum_apps::stratum_core::channels_sv2::vardiff::tuning::set_shares_per_minute(spm)
}

/// Current vardiff algorithm as a lowercase name.
pub fn algorithm() -> &'static str {
    use stratum_apps::stratum_core::channels_sv2::vardiff::{tuning, VardiffKind};
    match tuning::algorithm_override() {
        Some(VardiffKind::Classic) => "classic",
        Some(VardiffKind::Pid) => "pid",
        Some(VardiffKind::Champion) => "champion",
        Some(VardiffKind::QPid) | None => "qpid",
    }
}

/// Sets the live vardiff algorithm ("classic" | "pid" | "qpid" | "champion");
/// existing channels rebuild their controllers on the next evaluation.
pub fn set_algorithm(name: &str) -> bool {
    use stratum_apps::stratum_core::channels_sv2::vardiff::{tuning, VardiffKind};
    let kind = match name {
        "classic" => VardiffKind::Classic,
        "pid" => VardiffKind::Pid,
        "qpid" => VardiffKind::QPid,
        "champion" => VardiffKind::Champion,
        _ => return false,
    };
    tuning::set_algorithm(kind);
    true
}

/// Current manual gain values (override, else pid defaults).
pub fn manual_gains() -> (f64, f64, f64) {
    use stratum_apps::stratum_core::channels_sv2::vardiff::{pid, tuning};
    let (kp, ki, kd) = tuning::gain_overrides();
    (
        kp.unwrap_or(pid::DEFAULT_KP),
        ki.unwrap_or(pid::DEFAULT_KI),
        kd.unwrap_or(pid::DEFAULT_KD),
    )
}

/// Sets the live manual gains (plain pid only; qpid owns its gains).
pub fn set_manual_gains(kp: f64, ki: f64, kd: f64) {
    use stratum_apps::stratum_core::channels_sv2::vardiff::tuning;
    tuning::set_kp(kp);
    tuning::set_ki(ki);
    tuning::set_kd(kd);
}

/// Current controller gains (kp, ki, kd) for a miner's channel, published by
/// the embedded pool's pid/qpid controller under the miner's user identity.
pub fn controller_gains(name: &str) -> Option<(f64, f64, f64)> {
    stratum_apps::stratum_core::channels_sv2::vardiff::telemetry::gains(name)
}

/// Events a miner task reports back to the engine.
#[derive(Debug, Clone)]
pub enum MinerEvent {
    Connected,
    ChannelOpened { channel_id: u32, target_le: [u8; 32] },
    TargetUpdated { target_le: [u8; 32] },
    NewJob { job_id: u32 },
    ShareSubmitted { sequence: u32 },
    SharesAccepted { count: u32 },
    ShareRejected { code: String },
    HashrateChanged { hashrate: f64 },
    /// On-wire SV2 frame bytes for one message (header + payload). `down` =
    /// pool->miner (acks, SetTarget, jobs), `up` = miner->pool (shares, setup).
    Bytes { down: u64, up: u64 },
    Disconnected { reason: String },
}

/// Static configuration for one simulated miner.
#[derive(Debug, Clone, Default)]
pub struct MinerConfig {
    pub name: String,
    /// Simulated hashrate (H/s) used for the Poisson share model.
    pub hashrate: f64,
    /// Hashrate reported to the pool in `OpenStandardMiningChannel`. Defaults
    /// to `hashrate`; set it differently to test vardiff convergence from a
    /// wrong initial estimate.
    pub reported_hashrate: Option<f64>,
    /// Fraction of submitted shares that are invalid/stale (0.0..=1.0). Bad
    /// shares are marked with [`BAD_SHARE_JOB_ID`] so the pool rejects them.
    pub bad_share_fraction: f64,
    /// Fraction of submitted shares that are duplicates/replays (0.0..=1.0),
    /// marked with [`DUPLICATE_SHARE_JOB_ID`] so the pool rejects them.
    pub duplicate_share_fraction: f64,
    /// One-way share-delivery latency in ms of virtual time (0 = instant).
    pub latency_ms: u64,
    /// Uniform +/- jitter (ms) on the delivery latency.
    pub latency_jitter_ms: u64,
}

impl MinerConfig {
    pub fn nominal_hashrate(&self) -> f32 {
        self.reported_hashrate.unwrap_or(self.hashrate) as f32
    }
}

/// Interprets a 32-byte little-endian target as an f64.
pub fn target_le_to_f64(target_le: &[u8; 32]) -> f64 {
    target_le
        .iter()
        .rev()
        .fold(0.0_f64, |acc, &b| acc * 256.0 + b as f64)
}

/// Pool difficulty for a little-endian target (difficulty 1 = 0xffff * 2^208).
pub fn target_le_to_difficulty(target_le: &[u8; 32]) -> f64 {
    let diff1 = 65535.0 * 2.0_f64.powi(208);
    let t = target_le_to_f64(target_le);
    if t == 0.0 {
        f64::INFINITY
    } else {
        diff1 / t
    }
}

/// Expected shares per minute for `hashrate` (H/s) against a target.
pub fn expected_shares_per_minute(hashrate: f64, target_le: &[u8; 32]) -> f64 {
    hashrate * share_probability(target_le) * 60.0
}

/// Probability that a single hash is below the target.
pub fn share_probability(target_le: &[u8; 32]) -> f64 {
    target_le_to_f64(target_le) / 2.0_f64.powi(256)
}

/// Samples an exponentially distributed share inter-arrival time for a miner
/// with `hashrate` (H/s) against `target_le`, in **wall** time (the virtual
/// interval divided by the sim clock speed). Returns `None` when the rate is
/// zero (no shares will ever be found).
pub fn sample_share_interval(hashrate: f64, target_le: &[u8; 32]) -> Option<Duration> {
    use rand::Rng;
    let lambda = hashrate * share_probability(target_le); // shares per virtual second
    if lambda <= 0.0 || !lambda.is_finite() {
        return None;
    }
    let u: f64 = rand::thread_rng().gen_range(f64::MIN_POSITIVE..1.0);
    let dt = -u.ln() / lambda;
    // Cap at one virtual hour so a mis-targeted miner still wakes up
    // occasionally, then convert to wall time.
    Some(Duration::from_secs_f64(dt.min(3600.0) / clock_speed()))
}
