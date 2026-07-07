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
pub mod pool;
pub mod scenario;

use std::time::Duration;

/// Commands the engine sends to a running miner task.
#[derive(Debug, Clone)]
pub enum MinerCommand {
    /// Change the simulated hashrate (H/s). Takes effect on the next share
    /// interval sample.
    SetHashrate(f64),
    /// Close the connection and end the miner task.
    Disconnect,
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
    Disconnected { reason: String },
}

/// Static configuration for one simulated miner.
#[derive(Debug, Clone)]
pub struct MinerConfig {
    pub name: String,
    /// Simulated hashrate (H/s) used for the Poisson share model.
    pub hashrate: f64,
    /// Hashrate reported to the pool in `OpenStandardMiningChannel`. Defaults
    /// to `hashrate`; set it differently to test vardiff convergence from a
    /// wrong initial estimate.
    pub reported_hashrate: Option<f64>,
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
/// with `hashrate` (H/s) against `target_le`. Returns `None` when the rate is
/// zero (no shares will ever be found).
pub fn sample_share_interval(hashrate: f64, target_le: &[u8; 32]) -> Option<Duration> {
    use rand::Rng;
    let lambda = hashrate * share_probability(target_le); // shares per second
    if lambda <= 0.0 || !lambda.is_finite() {
        return None;
    }
    let u: f64 = rand::thread_rng().gen_range(f64::MIN_POSITIVE..1.0);
    let dt = -u.ln() / lambda;
    // Cap at one hour so a mis-targeted miner still wakes up occasionally.
    Some(Duration::from_secs_f64(dt.min(3600.0)))
}
