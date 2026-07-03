use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::profile::RateProfile;

#[derive(Debug)]
pub enum ApiCommand {
    SetProfile {
        channel_id: u32,
        profile: RateProfile,
    },
    SetAllProfiles {
        profile: RateProfile,
    },
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProxyStatus {
    pub upstream_connected: bool,
    pub channels: Vec<ChannelStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelStatus {
    pub id: u32,
    pub miner_connected: bool,
    pub profile: ProfileInfo,
    pub profile_elapsed_secs: f64,
    pub profile_duration_secs: Option<f64>,
    pub target_spm: f64,
    pub forwarded_spm: f64,
    pub supply_spm: f64,
    pub headroom: String,
    pub floor_active: bool,
    pub pool_difficulty: Option<f64>,
    pub miner_difficulty: f64,
    pub shares_forwarded: u64,
    pub shares_gated: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileInfo {
    #[serde(rename = "type")]
    pub profile_type: String,
    pub description: String,
}

impl ProfileInfo {
    pub fn from_profile(p: &RateProfile) -> Self {
        match p {
            RateProfile::Track { factor } => ProfileInfo {
                profile_type: "track".into(),
                description: format!("{:.1}× supply", factor),
            },
            RateProfile::Step {
                before,
                after,
                at_secs,
            } => ProfileInfo {
                profile_type: "step".into(),
                description: format!("{:.1}× → {:.1}× @ {:.0}s", before, after, at_secs),
            },
            RateProfile::Ramp {
                from,
                to,
                duration_secs,
            } => ProfileInfo {
                profile_type: "ramp".into(),
                description: format!("{:.1}× → {:.1}× over {:.0}s", from, to, duration_secs),
            },
            RateProfile::Stall {
                at_secs,
                duration_secs,
            } => ProfileInfo {
                profile_type: "stall".into(),
                description: format!("zero @ {:.0}s for {:.0}s", at_secs, duration_secs),
            },
            RateProfile::Burst {
                base,
                peak,
                at_secs,
                duration_secs,
            } => ProfileInfo {
                profile_type: "burst".into(),
                description: format!(
                    "{:.1}× → {:.1}× @ {:.0}s for {:.0}s",
                    base, peak, at_secs, duration_secs
                ),
            },
            RateProfile::Oscillate {
                base,
                amp,
                period_secs,
            } => ProfileInfo {
                profile_type: "oscillate".into(),
                description: format!("{:.1}±{:.1}×, period {:.0}s", base, amp, period_secs),
            },
            RateProfile::AbsStep {
                before_spm,
                after_spm,
                at_secs,
            } => ProfileInfo {
                profile_type: "abs_step".into(),
                description: format!(
                    "{:.1} → {:.1} spm ABSOLUTE @ {:.0}s (supply-independent)",
                    before_spm, after_spm, at_secs
                ),
            },
        }
    }
}

struct AppState {
    status_rx: watch::Receiver<ProxyStatus>,
    cmd_tx: mpsc::UnboundedSender<ApiCommand>,
}

pub fn create_router(
    status_rx: watch::Receiver<ProxyStatus>,
    cmd_tx: mpsc::UnboundedSender<ApiCommand>,
) -> Router {
    let state = Arc::new(AppState { status_rx, cmd_tx });

    Router::new()
        .route("/status", get(get_status))
        .route("/channels/{id}/profile", post(set_channel_profile))
        .route("/profile", post(set_all_profiles))
        .with_state(state)
}

async fn get_status(State(state): State<Arc<AppState>>) -> Json<ProxyStatus> {
    Json(state.status_rx.borrow().clone())
}

#[derive(Deserialize)]
struct ProfileRequest {
    #[serde(flatten)]
    profile: RateProfile,
}

async fn set_channel_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u32>,
    Json(req): Json<ProfileRequest>,
) -> StatusCode {
    let cmd = ApiCommand::SetProfile {
        channel_id: id,
        profile: req.profile,
    };
    match state.cmd_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn set_all_profiles(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProfileRequest>,
) -> StatusCode {
    let cmd = ApiCommand::SetAllProfiles {
        profile: req.profile,
    };
    match state.cmd_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
