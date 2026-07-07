//! TOML scenario files for repeatable vardiff experiments.
//!
//! Example:
//! ```toml
//! name = "step-change"
//! duration_secs = 600
//!
//! [[miners]]
//! name = "asic-1"
//! hashrate = 100e12
//! # reported_hashrate = 10e12   # lie to the pool to test convergence
//! # start_at = 0
//!
//! [miners.drift]
//! mode = "sine"        # or "random_walk"
//! amplitude = 0.15     # +/-15% around the base hashrate
//! period_secs = 300    # sine period (ignored for random_walk)
//! step_secs = 5        # how often drift is applied
//!
//! [[miners.events]]
//! at = 120
//! action = "set_hashrate"
//! hashrate = 1000e12
//!
//! [[miners.events]]
//! at = 300
//! action = "disconnect"
//!
//! [[miners.events]]
//! at = 360
//! action = "reconnect"
//! ```

use std::{collections::HashSet, path::Path};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Scenario {
    pub name: String,
    /// Total run time; headless mode exits after this. `None` runs forever.
    #[serde(default)]
    pub duration_secs: Option<u64>,
    #[serde(default)]
    pub miners: Vec<ScenarioMiner>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioMiner {
    pub name: String,
    /// Simulated hashrate in H/s.
    pub hashrate: f64,
    /// Hashrate reported to the pool when opening the channel (defaults to
    /// `hashrate`).
    #[serde(default)]
    pub reported_hashrate: Option<f64>,
    /// Seconds after scenario start at which this miner connects.
    #[serde(default)]
    pub start_at: u64,
    #[serde(default)]
    pub drift: Option<Drift>,
    #[serde(default)]
    pub events: Vec<ScenarioEvent>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Drift {
    pub mode: DriftMode,
    /// Fractional amplitude around the base hashrate (0.1 = +/-10%).
    pub amplitude: f64,
    /// Sine period in seconds (ignored for random walk).
    #[serde(default = "default_period")]
    pub period_secs: f64,
    /// How often the drifted hashrate is pushed to the miner.
    #[serde(default = "default_step")]
    pub step_secs: f64,
}

fn default_period() -> f64 {
    300.0
}

fn default_step() -> f64 {
    5.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftMode {
    Sine,
    RandomWalk,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioEvent {
    /// Seconds after scenario start.
    pub at: u64,
    #[serde(flatten)]
    pub action: EventAction,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum EventAction {
    SetHashrate { hashrate: f64 },
    Disconnect,
    Reconnect,
}

impl Scenario {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        toml::from_str(&raw).map_err(|e| format!("cannot parse {}: {e}", path.display()))
    }
}

/// Tracks which scenario actions have fired; both the headless runner and the
/// TUI poll this every tick.
pub struct ScenarioDriver {
    scenario: Scenario,
    started: HashSet<String>,
    fired: HashSet<(String, u64)>,
}

/// A scenario action that is due at the current tick.
#[derive(Debug, Clone)]
pub enum DueAction {
    Start(ScenarioMiner),
    Apply { miner: String, action: EventAction },
}

impl ScenarioDriver {
    pub fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            started: HashSet::new(),
            fired: HashSet::new(),
        }
    }

    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    pub fn finished(&self, elapsed_secs: f64) -> bool {
        self.scenario
            .duration_secs
            .is_some_and(|d| elapsed_secs >= d as f64)
    }

    /// Returns every action whose time has come, at most once each.
    pub fn due_actions(&mut self, elapsed_secs: f64) -> Vec<DueAction> {
        let mut due = Vec::new();
        for miner in &self.scenario.miners {
            if elapsed_secs >= miner.start_at as f64 && !self.started.contains(&miner.name) {
                self.started.insert(miner.name.clone());
                due.push(DueAction::Start(miner.clone()));
            }
            for (idx, event) in miner.events.iter().enumerate() {
                let key = (miner.name.clone(), idx as u64);
                if elapsed_secs >= event.at as f64 && !self.fired.contains(&key) {
                    self.fired.insert(key);
                    due.push(DueAction::Apply {
                        miner: miner.name.clone(),
                        action: event.action.clone(),
                    });
                }
            }
        }
        due
    }
}
