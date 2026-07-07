//! Session hub: one fully-isolated vardiff-sim process per browser session.
//!
//! `vardiff-sim --hub 0.0.0.0:8080` serves a single public port. Each new
//! browser (tracked by a `vdsid` cookie) gets its own child `vardiff-sim
//! --spawn-pool --http` process — own pool, template provider, sim clock and
//! Q-table — and the hub proxies HTTP and WebSocket traffic to it. Sessions
//! with no WebSocket attached and no request activity for the idle TTL are
//! torn down with SIGTERM so the child's graceful pool/bitcoind teardown
//! runs.

use std::{
    collections::HashMap,
    net::SocketAddr,
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::{
        ws::{Message as AxMessage, WebSocket},
        Request, State, WebSocketUpgrade,
    },
    http::{header, uri::Uri, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tokio_tungstenite::tungstenite::Message as TgMessage;

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Body>;

/// How long a child may take to become ready (first run downloads bitcoind
/// and sv2-tp, which can take minutes on a cold cache).
const CHILD_READY_TIMEOUT: Duration = Duration::from_secs(180);
/// Reaper cadence.
const REAP_INTERVAL: Duration = Duration::from_secs(15);

pub struct HubConfig {
    pub addr: SocketAddr,
    /// Hub access token (opt-in, same semantics as the single-session mode).
    pub token: Option<String>,
    /// Idle TTL: sessions with no live WebSocket and no requests for this
    /// long are torn down.
    pub ttl: Duration,
    /// CLI flags forwarded to every child (algorithm, rates, gains, ...).
    pub child_args: Vec<String>,
}

struct Session {
    port: u16,
    token: String,
    pid: u32,
    last_seen: Instant,
    /// Live proxied WebSocket connections; a session with one attached never
    /// idles out.
    ws_conns: usize,
    /// False while the child is still booting; terminate_all covers these,
    /// the reaper leaves them alone.
    ready: bool,
}

#[derive(Clone)]
struct HubState {
    cfg: Arc<HubConfig>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    client: HttpClient,
}

fn rand_hex(n: usize) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..n).map(|_| format!("{:x}", rng.gen_range(0..16u8))).collect()
}

fn free_port() -> std::io::Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

/// Owned extraction: `&Request` must never be held across an await (axum's
/// Body is !Sync, which would make the handler future !Send).
fn cookie_sid(req: &Request) -> Option<String> {
    req.headers()
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|kv| kv.trim().strip_prefix("vdsid=").map(str::to_owned))
}

fn hub_authorized(st: &HubState, req: &Request) -> bool {
    let Some(token) = st.cfg.token.as_deref() else {
        return true;
    };
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
    bearer_ok || query_ok
}

/// Spawns a child sim and waits for its dashboard to come up.
async fn spawn_child(st: &HubState, sid: &str) -> Result<(), String> {
    let port = free_port().map_err(|e| format!("no free port: {e}"))?;
    let token = rand_hex(32);
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let child = tokio::process::Command::new(exe)
        .args(["--spawn-pool", "--http", &format!("127.0.0.1:{port}")])
        .args(["--http-token", &token])
        .args(&st.cfg.child_args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn session child: {e}"))?;
    let pid = child.id().ok_or("child exited immediately")?;
    eprintln!("hub: session {sid} -> child pid {pid} on port {port}");
    // Register before the readiness probe: a hub shutdown mid-spawn must
    // still know this child exists.
    st.sessions.lock().expect("sessions lock").insert(
        sid.to_string(),
        Session {
            port,
            token: token.clone(),
            pid,
            last_seen: Instant::now(),
            ws_conns: 0,
            ready: false,
        },
    );

    // Readiness: the child's pool must be up before we route traffic.
    let url: Uri = format!("http://127.0.0.1:{port}/api/stats?token={token}")
        .parse()
        .expect("static uri shape");
    let deadline = Instant::now() + CHILD_READY_TIMEOUT;
    loop {
        if let Ok(resp) = st.client.get(url.clone()).await {
            if resp.status().is_success() {
                break;
            }
        }
        if Instant::now() > deadline {
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            st.sessions.lock().expect("sessions lock").remove(sid);
            return Err("session child never became ready".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    if let Some(s) = st.sessions.lock().expect("sessions lock").get_mut(sid) {
        s.ready = true;
        s.last_seen = Instant::now();
    }
    // The Child handle is dropped without kill_on_drop: lifecycle is owned by
    // the session table + reaper. Reap zombies opportunistically.
    tokio::spawn(async move {
        let mut child = child;
        let _ = child.wait().await;
    });
    Ok(())
}

/// Resolves the request's session, creating one if needed. Returns
/// (sid, child port, child token, freshly_created).
enum Resolved {
    Session(String, u16, String, bool),
    /// Non-page request without a live session: do NOT spawn a child for
    /// stray API calls (each one would fork a pool + bitcoind).
    NoSession,
}

async fn get_or_create_session(
    st: &HubState,
    cookie: Option<String>,
    may_create: bool,
) -> Result<Resolved, String> {
    if let Some(sid) = cookie.as_deref() {
        let mut sessions = st.sessions.lock().expect("sessions lock");
        if let Some(s) = sessions.get_mut(sid) {
            if s.ready {
                s.last_seen = Instant::now();
                return Ok(Resolved::Session(
                    sid.to_string(),
                    s.port,
                    s.token.clone(),
                    false,
                ));
            }
        }
    }
    if !may_create {
        return Ok(Resolved::NoSession);
    }
    let sid = rand_hex(16);
    spawn_child(st, &sid).await?;
    let sessions = st.sessions.lock().expect("sessions lock");
    let s = sessions.get(&sid).expect("just inserted");
    Ok(Resolved::Session(sid.clone(), s.port, s.token.clone(), true))
}

async fn handle(State(st): State<HubState>, req: Request) -> Response {
    use axum::extract::FromRequestParts;
    // Everything borrowed from the request happens before the first await;
    // only owned values (and the moved request) cross it.
    if !hub_authorized(&st, &req) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let cookie = cookie_sid(&req);
    let is_ws = req.uri().path() == "/api/ws";
    let may_create = req.uri().path() == "/";
    let (sid, port, child_token, fresh) =
        match get_or_create_session(&st, cookie, may_create).await {
            Ok(Resolved::Session(sid, port, token, fresh)) => (sid, port, token, fresh),
            Ok(Resolved::NoSession) => {
                return (
                    StatusCode::CONFLICT,
                    "session expired or missing; reload the dashboard page",
                )
                    .into_response();
            }
            Err(e) => {
                eprintln!("hub: session error: {e}");
                return (StatusCode::BAD_GATEWAY, e).into_response();
            }
        };

    let mut response = if is_ws {
        let (mut parts, _body) = req.into_parts();
        match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(ws) => proxy_ws(st.clone(), sid.clone(), port, child_token, ws),
            Err(rej) => rej.into_response(),
        }
    } else {
        proxy_http(&st, port, &child_token, req).await
    };
    if fresh {
        response.headers_mut().append(
            header::SET_COOKIE,
            HeaderValue::from_str(&format!(
                "vdsid={sid}; Path=/; HttpOnly; SameSite=Lax"
            ))
            .expect("valid cookie"),
        );
    }
    response
}

async fn proxy_http(st: &HubState, port: u16, child_token: &str, mut req: Request) -> Response {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let uri: Uri = match format!("http://127.0.0.1:{port}{path_and_query}").parse() {
        Ok(u) => u,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    *req.uri_mut() = uri;
    req.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {child_token}")).expect("valid header"),
    );
    match st.client.request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("proxy error: {e}")).into_response(),
    }
}

/// Browser <-> hub <-> child WebSocket pipe.
fn proxy_ws(
    st: HubState,
    sid: String,
    port: u16,
    child_token: String,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |browser| async move {
        let url = format!("ws://127.0.0.1:{port}/api/ws?token={child_token}");
        let Ok((child_ws, _)) = tokio_tungstenite::connect_async(&url).await else {
            return;
        };
        {
            let mut sessions = st.sessions.lock().expect("sessions lock");
            if let Some(s) = sessions.get_mut(&sid) {
                s.ws_conns += 1;
            }
        }
        pump(browser, child_ws).await;
        let mut sessions = st.sessions.lock().expect("sessions lock");
        if let Some(s) = sessions.get_mut(&sid) {
            s.ws_conns = s.ws_conns.saturating_sub(1);
            s.last_seen = Instant::now();
        }
    })
}

async fn pump(
    browser: WebSocket,
    child: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let (mut btx, mut brx) = browser.split();
    let (mut ctx, mut crx) = child.split();
    let to_browser = async {
        while let Some(Ok(msg)) = crx.next().await {
            let out = match msg {
                TgMessage::Text(t) => AxMessage::Text(t),
                TgMessage::Binary(b) => AxMessage::Binary(b),
                TgMessage::Ping(p) => AxMessage::Ping(p),
                TgMessage::Pong(p) => AxMessage::Pong(p),
                TgMessage::Close(_) => break,
                TgMessage::Frame(_) => continue,
            };
            if btx.send(out).await.is_err() {
                break;
            }
        }
    };
    let to_child = async {
        while let Some(Ok(msg)) = brx.next().await {
            let out = match msg {
                AxMessage::Text(t) => TgMessage::Text(t),
                AxMessage::Binary(b) => TgMessage::Binary(b),
                AxMessage::Ping(p) => TgMessage::Ping(p),
                AxMessage::Pong(p) => TgMessage::Pong(p),
                AxMessage::Close(_) => break,
            };
            if ctx.send(out).await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        _ = to_browser => {}
        _ = to_child => {}
    }
}

fn terminate(pid: u32) {
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
}

async fn reaper(st: HubState) {
    loop {
        tokio::time::sleep(REAP_INTERVAL).await;
        let expired: Vec<(String, u32)> = {
            let sessions = st.sessions.lock().expect("sessions lock");
            sessions
                .iter()
                .filter(|(_, s)| s.ready && s.ws_conns == 0 && s.last_seen.elapsed() > st.cfg.ttl)
                .map(|(sid, s)| (sid.clone(), s.pid))
                .collect()
        };
        for (sid, pid) in expired {
            eprintln!("hub: reaping idle session {sid} (pid {pid})");
            terminate(pid);
            st.sessions.lock().expect("sessions lock").remove(&sid);
        }
    }
}

/// Terminates every session child (hub shutdown path).
fn terminate_all(st: &HubState) {
    let sessions = st.sessions.lock().expect("sessions lock");
    for (sid, s) in sessions.iter() {
        eprintln!("hub: terminating session {sid} (pid {})", s.pid);
        terminate(s.pid);
    }
}

pub async fn serve(cfg: HubConfig, shutdown: async_channel::Receiver<()>) {
    let addr = cfg.addr;
    let state = HubState {
        cfg: Arc::new(cfg),
        sessions: Arc::new(Mutex::new(HashMap::new())),
        client: Client::builder(TokioExecutor::new()).build_http(),
    };
    tokio::spawn(reaper(state.clone()));

    let app = Router::new()
        .route("/", any(handle))
        .route("/*path", any(handle))
        .with_state(state.clone());
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("hub: failed to bind {addr}: {e}");
            return;
        }
    };
    match state.cfg.token.as_deref() {
        Some(token) => eprintln!("hub: http://{addr}/?token={token}"),
        None => eprintln!("hub: http://{addr}/ (no auth token configured)"),
    }
    tokio::select! {
        r = axum::serve(listener, app) => {
            if let Err(e) = r {
                eprintln!("hub server error: {e}");
            }
        }
        _ = shutdown.recv() => {
            eprintln!("hub: signal received, terminating sessions...");
        }
    }
    terminate_all(&state);
    // Children run a bounded graceful teardown (pool + template provider);
    // give it room before the hub exits.
    tokio::time::sleep(Duration::from_secs(8)).await;
}
