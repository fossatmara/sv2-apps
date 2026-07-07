//! Web dashboard: the same engine the TUI drives, served over HTTP.
//!
//! - `GET /` — self-contained dashboard page (no external assets)
//! - `GET /api/stats` — JSON snapshot incl. windowed chart series
//! - `POST /api/miners` — add a miner `{name?, hashrate?}`
//! - `POST /api/miners/:name/hashrate` — `{hashrate}` (H/s)
//! - `POST /api/miners/:name/disconnect|reconnect|remove`
//! - `POST /api/speed` — `{speed}` (embedded pool only)

use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use integration_tests_sv2::vardiff_sim::{engine::SimEngine, MinerConfig};
use serde::{Deserialize, Serialize};

pub type SharedEngine = Arc<Mutex<SimEngine>>;

/// Matches the TUI chart window.
const WINDOW_SECS: f64 = 300.0;

#[derive(Clone)]
pub struct HttpState {
    pub engine: SharedEngine,
    /// Speed control only makes sense with an embedded pool.
    pub speed_control: bool,
}

#[derive(Serialize)]
struct MinerSnapshot {
    name: String,
    connected: bool,
    hashrate: f64,
    difficulty: f64,
    expected_spm: f64,
    realized_spm: f64,
    submitted: u64,
    accepted: u64,
    rejected: u64,
    target_updates: u64,
    disconnects: u64,
    last_error: Option<String>,
    /// (elapsed_secs, difficulty) points within the window.
    difficulty_history: Vec<(f64, f64)>,
    /// (elapsed_secs, from, to) commanded hashrate changes within the window.
    hashrate_changes: Vec<(f64, f64, f64)>,
    /// Share submission times (elapsed_secs) within the window.
    shares: Vec<f64>,
}

#[derive(Serialize)]
struct StatsSnapshot {
    elapsed_secs: f64,
    speed: f64,
    speed_control: bool,
    window_secs: f64,
    miners: Vec<MinerSnapshot>,
}

async fn index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

async fn stats(State(st): State<HttpState>) -> Json<StatsSnapshot> {
    let engine = st.engine.lock().expect("engine lock");
    let elapsed = engine.elapsed_secs();
    let from = (elapsed - WINDOW_SECS).max(0.0);
    let miners = engine
        .miner_names()
        .into_iter()
        .map(|name| {
            let s = &engine.stats[&name];
            // Windowed difficulty series with a held entry point, mirroring
            // the TUI chart.
            let mut history: Vec<(f64, f64)> = Vec::new();
            if let Some(&(_, held)) = s
                .difficulty_history
                .iter()
                .rev()
                .find(|(t, _)| *t < from)
            {
                history.push((from, held));
            }
            history.extend(s.difficulty_history.iter().filter(|(t, _)| *t >= from));
            MinerSnapshot {
                connected: s.connected,
                hashrate: s.hashrate,
                difficulty: s.difficulty,
                expected_spm: s.expected_spm,
                realized_spm: s.realized_spm(),
                submitted: s.submitted,
                accepted: s.accepted,
                rejected: s.rejected,
                target_updates: s.target_updates,
                disconnects: s.disconnects,
                last_error: s.last_error.clone(),
                difficulty_history: history,
                hashrate_changes: s
                    .hashrate_changes
                    .iter()
                    .filter(|c| c.at >= from)
                    .map(|c| (c.at, c.from, c.to))
                    .collect(),
                shares: engine.share_times_since(&name, from),
                name,
            }
        })
        .collect();
    Json(StatsSnapshot {
        elapsed_secs: elapsed,
        speed: engine.speed(),
        speed_control: st.speed_control,
        window_secs: WINDOW_SECS,
        miners,
    })
}

#[derive(Deserialize)]
struct HashrateBody {
    hashrate: f64,
}

#[derive(Deserialize)]
struct SpeedBody {
    speed: f64,
}

#[derive(Deserialize, Default)]
struct AddBody {
    name: Option<String>,
    hashrate: Option<f64>,
}

async fn set_hashrate(
    State(st): State<HttpState>,
    Path(name): Path<String>,
    Json(body): Json<HashrateBody>,
) -> StatusCode {
    if !(body.hashrate.is_finite() && body.hashrate > 0.0) {
        return StatusCode::BAD_REQUEST;
    }
    let mut engine = st.engine.lock().expect("engine lock");
    if !engine.stats.contains_key(&name) {
        return StatusCode::NOT_FOUND;
    }
    engine.set_hashrate(&name, body.hashrate);
    StatusCode::NO_CONTENT
}

async fn disconnect(State(st): State<HttpState>, Path(name): Path<String>) -> StatusCode {
    let mut engine = st.engine.lock().expect("engine lock");
    if !engine.stats.contains_key(&name) {
        return StatusCode::NOT_FOUND;
    }
    engine.disconnect(&name);
    StatusCode::NO_CONTENT
}

async fn reconnect(State(st): State<HttpState>, Path(name): Path<String>) -> StatusCode {
    let mut engine = st.engine.lock().expect("engine lock");
    if !engine.stats.contains_key(&name) {
        return StatusCode::NOT_FOUND;
    }
    engine.reconnect(&name);
    StatusCode::NO_CONTENT
}

async fn remove(State(st): State<HttpState>, Path(name): Path<String>) -> StatusCode {
    let mut engine = st.engine.lock().expect("engine lock");
    if !engine.stats.contains_key(&name) {
        return StatusCode::NOT_FOUND;
    }
    engine.remove_miner(&name);
    StatusCode::NO_CONTENT
}

async fn add_miner(State(st): State<HttpState>, Json(body): Json<AddBody>) -> StatusCode {
    let mut engine = st.engine.lock().expect("engine lock");
    let name = body.name.unwrap_or_else(|| {
        let mut i = engine.stats.len();
        loop {
            let candidate = format!("web-{i}");
            if !engine.stats.contains_key(&candidate) {
                break candidate;
            }
            i += 1;
        }
    });
    if engine.stats.contains_key(&name) {
        return StatusCode::CONFLICT;
    }
    engine.spawn_miner(
        MinerConfig {
            name,
            hashrate: body.hashrate.unwrap_or(100e12),
            reported_hashrate: None,
        },
        None,
    );
    StatusCode::NO_CONTENT
}

async fn set_speed(State(st): State<HttpState>, Json(body): Json<SpeedBody>) -> StatusCode {
    if !st.speed_control {
        return StatusCode::FORBIDDEN;
    }
    if !(body.speed.is_finite() && body.speed > 0.0) {
        return StatusCode::BAD_REQUEST;
    }
    st.engine
        .lock()
        .expect("engine lock")
        .set_speed(body.speed);
    StatusCode::NO_CONTENT
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/stats", get(stats))
        .route("/api/miners", post(add_miner))
        .route("/api/miners/:name/hashrate", post(set_hashrate))
        .route("/api/miners/:name/disconnect", post(disconnect))
        .route("/api/miners/:name/reconnect", post(reconnect))
        .route("/api/miners/:name/remove", post(remove))
        .route("/api/speed", post(set_speed))
        .with_state(state)
}

/// Binds and serves until the process exits.
pub async fn serve(addr: std::net::SocketAddr, state: HttpState) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("http: failed to bind {addr}: {e}");
            return;
        }
    };
    eprintln!("dashboard: http://{addr}/");
    if let Err(e) = axum::serve(listener, router(state)).await {
        eprintln!("http server error: {e}");
    }
}
