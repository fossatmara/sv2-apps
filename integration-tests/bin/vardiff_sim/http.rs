//! Web dashboard: the same engine the TUI drives, served over HTTP.
//!
//! - `GET /` — self-contained dashboard page (no external assets)
//! - `GET /api/stats` — JSON snapshot incl. windowed chart series
//! - `GET /api/ws` — WebSocket pushing the same snapshots event-driven
//! - `POST /api/miners` — add a miner `{name?, hashrate?}`
//! - `POST /api/miners/:name/hashrate` — `{hashrate}` (H/s)
//! - `POST /api/miners/:name/disconnect|reconnect|remove`
//! - `POST /api/speed` — `{speed}` (embedded pool only)

use std::sync::{Arc, Mutex};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Request, State, WebSocketUpgrade,
    },
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use integration_tests_sv2::vardiff_sim::{engine::SimEngine, MinerConfig};
use serde::{Deserialize, Serialize};

pub type SharedEngine = Arc<Mutex<SimEngine>>;

/// Web chart window base (virtual seconds at speed 1): longer than the
/// TUI's, and scaled by the sim clock speed at snapshot time so the window
/// spans a consistent wall-clock viewing time under acceleration.
const WINDOW_BASE_SECS: f64 = 600.0;

#[derive(Clone)]
pub struct HttpState {
    pub engine: SharedEngine,
    /// Speed control only makes sense with an embedded pool.
    pub speed_control: bool,
    /// When set, required on every request (Bearer header or ?token= query)
    /// except the static chart-library assets. When None, the dashboard is
    /// open (bind loopback or front with a proxy if that matters).
    pub token: Option<String>,
}

/// Accepts `Authorization: Bearer <token>` or `?token=<token>`; the vendored
/// chart-library assets are public (static third-party code, nothing to
/// protect). Comparison is not constant-time; this guards a lab dashboard,
/// not production credentials.
async fn auth(State(st): State<HttpState>, req: Request, next: Next) -> Result<Response, StatusCode> {
    let Some(token) = st.token.as_deref() else {
        // No token configured: the dashboard is open by choice.
        return Ok(next.run(req).await);
    };
    if req.uri().path().starts_with("/assets/") {
        return Ok(next.run(req).await);
    }
    let bearer_ok = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t == token)
        .unwrap_or(false);
    let query_ok = req
        .uri()
        .query()
        .map(|q| q.split('&').any(|kv| kv.strip_prefix("token=") == Some(token)))
        .unwrap_or(false);
    if bearer_ok || query_ok {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn uplot_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!("assets/uplot.min.js"),
    )
}

async fn uplot_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css")],
        include_str!("assets/uplot.min.css"),
    )
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
    confidence_k: f64,
    significance_z: f64,
    window_secs: f64,
    miners: Vec<MinerSnapshot>,
}

async fn index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

/// `full` disables the sliding window for the difficulty/marker series
/// (capped to the most recent points as a safety bound); the share rug stays
/// windowed — it is only legible zoomed-in and dominates payload size.
fn build_snapshot(st: &HttpState, full: bool) -> StatsSnapshot {
    const FULL_HISTORY_CAP: usize = 10_000;
    let engine = st.engine.lock().expect("engine lock");
    let elapsed = engine.elapsed_secs();
    let window = WINDOW_BASE_SECS * engine.speed();
    let from = if full {
        0.0
    } else {
        (elapsed - window).max(0.0)
    };
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
            if history.len() > FULL_HISTORY_CAP {
                history.drain(..history.len() - FULL_HISTORY_CAP);
            }
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
    StatsSnapshot {
        elapsed_secs: elapsed,
        speed: engine.speed(),
        speed_control: st.speed_control,
        confidence_k: engine.confidence_k(),
        significance_z: engine.significance_z(),
        window_secs: window,
        miners,
    }
}

#[derive(Deserialize, Default)]
struct StatsParams {
    #[serde(default)]
    full: Option<u8>,
}

async fn stats(
    State(st): State<HttpState>,
    axum::extract::Query(params): axum::extract::Query<StatsParams>,
) -> Json<StatsSnapshot> {
    Json(build_snapshot(&st, params.full.unwrap_or(0) != 0))
}

async fn ws_upgrade(State(st): State<HttpState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| ws_stream(socket, st))
}

/// Pushes a snapshot on connect, then again within ~100ms of any engine
/// change (bursts coalesced) with a 1s heartbeat — the browser never polls.
async fn ws_stream(mut socket: WebSocket, st: HttpState) {
    let notify = st
        .engine
        .lock()
        .expect("engine lock")
        .change_notifier();
    loop {
        let json = match serde_json::to_string(&build_snapshot(&st, false)) {
            Ok(j) => j,
            Err(_) => return,
        };
        if socket.send(Message::Text(json)).await.is_err() {
            return;
        }
        tokio::select! {
            _ = notify.notified() => {
                // Coalesce event bursts into one frame.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            // Drain client frames so pings/closes are handled promptly.
            msg = socket.recv() => {
                match msg {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                    _ => {}
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct HashrateBody {
    hashrate: f64,
}

#[derive(Deserialize)]
struct SpeedBody {
    speed: f64,
}

#[derive(Deserialize)]
struct ConfidenceBody {
    k: f64,
}

#[derive(Deserialize)]
struct SignificanceBody {
    z: f64,
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

async fn set_confidence(State(st): State<HttpState>, Json(body): Json<ConfidenceBody>) -> StatusCode {
    if !st.speed_control {
        return StatusCode::FORBIDDEN;
    }
    if !body.k.is_finite() || body.k < 0.0 {
        return StatusCode::BAD_REQUEST;
    }
    st.engine
        .lock()
        .expect("engine lock")
        .set_confidence_k(body.k);
    StatusCode::NO_CONTENT
}

async fn set_significance(
    State(st): State<HttpState>,
    Json(body): Json<SignificanceBody>,
) -> StatusCode {
    if !st.speed_control {
        return StatusCode::FORBIDDEN;
    }
    if !body.z.is_finite() || body.z <= 0.0 {
        return StatusCode::BAD_REQUEST;
    }
    st.engine
        .lock()
        .expect("engine lock")
        .set_significance_z(body.z);
    StatusCode::NO_CONTENT
}

pub fn router(state: HttpState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/uplot.js", get(uplot_js))
        .route("/assets/uplot.css", get(uplot_css))
        .route("/api/stats", get(stats))
        .route("/api/ws", get(ws_upgrade))
        .route("/api/miners", post(add_miner))
        .route("/api/miners/:name/hashrate", post(set_hashrate))
        .route("/api/miners/:name/disconnect", post(disconnect))
        .route("/api/miners/:name/reconnect", post(reconnect))
        .route("/api/miners/:name/remove", post(remove))
        .route("/api/speed", post(set_speed))
        .route("/api/confidence", post(set_confidence))
        .route("/api/significance", post(set_significance))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
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
    match state.token.as_deref() {
        Some(token) => eprintln!("dashboard: http://{addr}/?token={token}"),
        None => eprintln!("dashboard: http://{addr}/ (no auth token configured)"),
    }
    if let Err(e) = axum::serve(listener, router(state)).await {
        eprintln!("http server error: {e}");
    }
}
