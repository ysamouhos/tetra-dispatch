//! Console web server: the page, and one WebSocket per console carrying JSON
//! commands/status and binary PCM16LE @ 8 kHz audio both ways. With operator
//! accounts, `POST /login` opens a session (a cookie) that the WebSocket, the
//! recordings and the account API are bound to.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use anyhow::Context;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::accounts::{self, Accounts, Refused};
use crate::config::{Role, WebConfig};
use crate::dispatcher::{ClientId, Event, UiCmd, UiOut};
use crate::recording;

const INDEX_HTML: &str = include_str!("../static/index.html");
const LOG_HTML: &str = include_str!("../static/log.html");
/// Longest microphone chunk accepted in one message (1 s).
const MAX_PCM_SAMPLES: usize = 8_000;
/// A failed login waits this long before answering.
const LOGIN_DELAY: Duration = Duration::from_millis(500);

/// How consoles are let in.
#[derive(Clone)]
pub enum Auth {
    /// Optional console password (HTTP Basic, any user name). Empty = open.
    Basic(Arc<String>),
    /// Operator accounts: name and password on a login page, then a session cookie.
    Accounts(Arc<Mutex<Accounts>>),
}

#[derive(Clone)]
struct AppState {
    events: mpsc::Sender<Event>,
    ui: broadcast::Sender<UiOut>,
    auth: Auth,
    /// Mark the session cookie `Secure` (the console is served over TLS).
    secure: bool,
    next_id: Arc<AtomicU64>,
    recordings: Arc<PathBuf>,
}

/// Who a request comes from, once authorized.
enum Who {
    /// No accounts: any console (Basic password checked if one is set).
    Anyone,
    Account { name: String, role: Role },
}

pub async fn run(cfg: WebConfig, auth: Auth, recordings: PathBuf, events: mpsc::Sender<Event>, ui: broadcast::Sender<UiOut>) -> anyhow::Result<()> {
    let state = AppState { events, ui, auth, secure: cfg.tls, next_id: Arc::new(AtomicU64::new(1)), recordings: Arc::new(recordings) };
    let app = Router::new()
        .route("/", get(index))
        .route("/log", get(log_page))
        .route("/ws", get(ws_upgrade))
        .route("/recordings", get(recordings_list))
        .route("/recordings/{name}", get(recordings_file))
        .route("/login", post(login))
        .route("/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/operators", get(operators_list))
        .route("/api/operators/{name}", post(operator_set).delete(operator_delete))
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(state);
    let listen = &cfg.listen;
    if cfg.tls {
        let tls = RustlsConfig::from_pem_file(&cfg.tls_cert_path, &cfg.tls_key_path).await
            .with_context(|| format!("loading console TLS cert {} and key {}", cfg.tls_cert_path.display(), cfg.tls_key_path.display()))?;
        let addr: SocketAddr = tokio::net::lookup_host(listen).await
            .with_context(|| format!("resolving web.listen {listen}"))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("web.listen {listen} resolves to nothing"))?;
        info!("console on https://{listen}/");
        axum_server::bind_rustls(addr, tls)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .with_context(|| format!("serving console on {listen}"))?;
    } else {
        let listener = tokio::net::TcpListener::bind(listen).await
            .with_context(|| format!("binding console on {listen}"))?;
        info!("console on http://{listen}/");
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    }
    Ok(())
}

/// Who is asking: HTTP Basic with any user name when a console password is
/// configured, the session cookie when accounts are.
fn authorized(state: &AppState, headers: &HeaderMap) -> Option<Who> {
    match &state.auth {
        Auth::Basic(password) => {
            if password.is_empty() {
                return Some(Who::Anyone);
            }
            headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Basic "))
                .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok())
                .and_then(|raw| String::from_utf8(raw).ok())
                .and_then(|s| s.split_once(':').map(|(_, p)| p.to_string()))
                .is_some_and(|p| constant_time_eq(p.as_bytes(), password.as_bytes()))
                .then_some(Who::Anyone)
        }
        Auth::Accounts(accounts) => {
            let token = session_token(headers)?;
            let a = accounts.lock().unwrap();
            let name = a.session(token)?.to_string();
            let role = a.role(&name)?;
            Some(Who::Account { name, role })
        }
    }
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::COOKIE).and_then(|v| v.to_str().ok()).and_then(accounts::cookie_token)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The answer to an unauthorized request: a Basic challenge, or "log in".
fn challenge(state: &AppState) -> Response {
    match &state.auth {
        Auth::Basic(_) => (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE, "Basic realm=\"Tetra Dispatch\"")], "authentication required\n").into_response(),
        Auth::Accounts(_) => api_error(StatusCode::UNAUTHORIZED, "login required"),
    }
}

fn api_error(status: StatusCode, msg: &str) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(json!({"error": msg}))).into_response()
}

fn api_json(v: Value) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Json(v)).into_response()
}

fn cookie(state: &AppState, token: &str, max_age: Option<u32>) -> String {
    let mut c = format!("{}={token}; Path=/; HttpOnly; SameSite=Strict", accounts::COOKIE);
    if let Some(age) = max_age {
        c.push_str(&format!("; Max-Age={age}"));
    }
    if state.secure {
        c.push_str("; Secure");
    }
    c
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    // With accounts the page itself is public: it shows the login form, and
    // everything behind it needs the session.
    let mode = match &state.auth {
        Auth::Basic(_) => {
            if authorized(&state, &headers).is_none() {
                return challenge(&state);
            }
            "open"
        }
        Auth::Accounts(_) => "accounts",
    };
    let html = INDEX_HTML.replace("__VERSION__", env!("CARGO_PKG_VERSION")).replace("__AUTH__", mode);
    ([(header::CACHE_CONTROL, "no-store")], Html(html)).into_response()
}

async fn log_page(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if authorized(&state, &headers).is_none() {
        return match &state.auth {
            Auth::Basic(_) => challenge(&state),
            Auth::Accounts(_) => Redirect::to("/").into_response(),
        };
    }
    ([(header::CACHE_CONTROL, "no-store")], Html(LOG_HTML)).into_response()
}

async fn recordings_list(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if authorized(&state, &headers).is_none() {
        return challenge(&state);
    }
    let dir = state.recordings.clone();
    let entries = tokio::task::spawn_blocking(move || recording::list(&dir)).await.unwrap_or_default();
    let body = serde_json::json!(entries.iter().map(|e| serde_json::json!({
        "name": e.name, "bytes": e.bytes, "seconds": e.seconds(), "modified_ms": e.modified_ms,
    })).collect::<Vec<_>>());
    ([(header::CACHE_CONTROL, "no-store"), (header::CONTENT_TYPE, "application/json")], body.to_string()).into_response()
}

/// `?dl=1` asks the browser to save the file instead of playing it.
async fn recordings_file(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(who) = authorized(&state, &headers) else {
        return challenge(&state);
    };
    let Some(path) = recording::path_of(&state.recordings, &name) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let action = if q.contains_key("dl") { "download" } else { "play" };
            let from = match who {
                Who::Account { name, .. } => name,
                Who::Anyone => addr.ip().to_string(),
            };
            let _ = state.events.send(Event::RecordingAccess { file: name.clone(), action, from }).await;
            let disp = if q.contains_key("dl") { format!("attachment; filename=\"{name}\"") } else { "inline".to_string() };
            ([(header::CONTENT_TYPE, "audio/wav".to_string()), (header::CONTENT_DISPOSITION, disp)], bytes).into_response()
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// ── Sessions ─────────────────────────────────────────────────────────

async fn login(State(state): State<AppState>, Json(body): Json<Value>) -> Response {
    let Auth::Accounts(accounts) = &state.auth else {
        return api_error(StatusCode::NOT_FOUND, "no operator accounts configured");
    };
    let name = body["name"].as_str().unwrap_or("").trim();
    let password = body["password"].as_str().unwrap_or("");
    let session = accounts.lock().unwrap().login(name, password);
    match session {
        Some(token) => {
            let (role, issi) = {
                let a = accounts.lock().unwrap();
                let o = a.get(name).expect("just logged in");
                (o.role, o.issi)
            };
            info!(name, role = role.as_str(), "operator logged in");
            (
                [(header::SET_COOKIE, cookie(&state, &token, None)), (header::CACHE_CONTROL, "no-store".to_string())],
                Json(json!({"name": name, "role": role.as_str(), "issi": (issi != 0).then_some(issi)})),
            )
                .into_response()
        }
        None => {
            warn!(name, "login failed");
            tokio::time::sleep(LOGIN_DELAY).await;
            api_error(StatusCode::UNAUTHORIZED, "wrong name or password")
        }
    }
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let (Auth::Accounts(accounts), Some(token)) = (&state.auth, session_token(&headers)) {
        accounts.lock().unwrap().logout(token);
    }
    ([(header::SET_COOKIE, cookie(&state, "", Some(0))), (header::CACHE_CONTROL, "no-store".to_string())], StatusCode::NO_CONTENT).into_response()
}

/// Who the browser is logged in as, so the page knows whether to show the login.
async fn me(State(state): State<AppState>, headers: HeaderMap) -> Response {
    match authorized(&state, &headers) {
        None => challenge(&state),
        Some(Who::Anyone) => api_json(json!({"auth": "open"})),
        Some(Who::Account { name, role }) => {
            let Auth::Accounts(accounts) = &state.auth else { unreachable!() };
            let issi = accounts.lock().unwrap().get(&name).map(|o| o.issi).unwrap_or(0);
            api_json(json!({"auth": "accounts", "name": name, "role": role.as_str(), "issi": (issi != 0).then_some(issi)}))
        }
    }
}

// ── Account management (admin) ───────────────────────────────────────

/// The accounts store and the admin asking, or the response that refuses.
fn admin(state: &AppState, headers: &HeaderMap) -> Result<(Arc<Mutex<Accounts>>, String), Response> {
    let Auth::Accounts(accounts) = &state.auth else {
        return Err(api_error(StatusCode::NOT_FOUND, "no operator accounts configured"));
    };
    match authorized(state, headers) {
        Some(Who::Account { name, role: Role::Admin }) => Ok((accounts.clone(), name)),
        Some(_) => Err(api_error(StatusCode::FORBIDDEN, "admin role required")),
        None => Err(challenge(state)),
    }
}

fn refused(r: Refused) -> Response {
    let status = match r {
        Refused::Invalid(_) => StatusCode::BAD_REQUEST,
        Refused::LastAdmin | Refused::Yourself => StatusCode::CONFLICT,
        Refused::NotFound => StatusCode::NOT_FOUND,
    };
    api_error(status, &r.to_string())
}

/// Every account, without passwords.
async fn operators_list(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Auth::Accounts(accounts) = &state.auth else {
        return api_error(StatusCode::NOT_FOUND, "no operator accounts configured");
    };
    if authorized(&state, &headers).is_none() {
        return challenge(&state);
    }
    let a = accounts.lock().unwrap();
    let list: Vec<Value> = a.operators().iter().map(|(name, o)| json!({
        "name": name, "issi": (o.issi != 0).then_some(o.issi), "role": o.role.as_str(),
        "groups": o.groups, "tx_group": (o.tx_group != 0).then_some(o.tx_group),
    })).collect();
    api_json(json!(list))
}

fn ssi_list(v: &Value) -> Option<Vec<u32>> {
    v.as_array().map(|a| a.iter().filter_map(Value::as_u64).filter_map(|n| u32::try_from(n).ok()).collect())
}

/// `{password?, issi, role, groups?, tx_group?}`: add or update an account.
async fn operator_set(State(state): State<AppState>, headers: HeaderMap, Path(name): Path<String>, Json(body): Json<Value>) -> Response {
    let (accounts, by) = match admin(&state, &headers) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let Some(role) = Role::parse(body.get("role").and_then(Value::as_str).unwrap_or("operator")) else {
        return api_error(StatusCode::BAD_REQUEST, "role must be admin, operator or viewer");
    };
    let password = body.get("password").and_then(Value::as_str).map(str::to_string);
    let issi = ssi(&body, "issi");
    let groups = body.get("groups").filter(|g| !g.is_null()).and_then(ssi_list);
    let tx_group = ssi(&body, "tx_group");
    let result = accounts.lock().unwrap().upsert(&name, password, issi, role, groups, tx_group);
    match result {
        Ok(account) => {
            info!(name, by, "account saved");
            let _ = state.events.send(Event::OperatorSet { name: name.clone(), account, by }).await;
            api_json(json!({"ok": true, "name": name}))
        }
        Err(r) => refused(r),
    }
}

async fn operator_delete(State(state): State<AppState>, headers: HeaderMap, Path(name): Path<String>) -> Response {
    let (accounts, by) = match admin(&state, &headers) {
        Ok(a) => a,
        Err(r) => return r,
    };
    let result = accounts.lock().unwrap().remove(&name, &by);
    match result {
        Ok(()) => {
            info!(name, by, "account removed");
            let _ = state.events.send(Event::OperatorRemoved { name: name.clone(), by }).await;
            api_json(json!({"ok": true, "name": name}))
        }
        Err(r) => refused(r),
    }
}

// ── Console WebSocket ────────────────────────────────────────────────

async fn ws_upgrade(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(who) = authorized(&state, &headers) else {
        return challenge(&state);
    };
    ws.on_upgrade(move |socket| console(state, socket, addr, who))
}

async fn console(state: AppState, socket: WebSocket, addr: SocketAddr, who: Who) {
    let id: ClientId = state.next_id.fetch_add(1, Ordering::Relaxed);
    let mut ui_rx = state.ui.subscribe();
    let joined = match &who {
        Who::Anyone => Event::ClientJoined(id, addr.ip().to_string()),
        Who::Account { name, .. } => Event::ClientLoggedIn(id, addr.ip().to_string(), name.clone()),
    };
    if state.events.send(joined).await.is_err() {
        return;
    }
    match &who {
        Who::Anyone => info!(id, %addr, "console connected"),
        Who::Account { name, .. } => info!(id, %addr, name, "console connected"),
    }
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                let ev = match msg {
                    Message::Text(t) => parse_command(&t).map(|c| Event::Ui(id, c)),
                    Message::Binary(b) if b.len() >= 2 && b.len() % 2 == 0 && b.len() / 2 <= MAX_PCM_SAMPLES => {
                        Some(Event::UlPcm(id, b.chunks_exact(2).map(|c| i16::from_le_bytes([c[0], c[1]])).collect()))
                    }
                    Message::Close(_) => break,
                    _ => None,
                };
                if let Some(ev) = ev {
                    if state.events.send(ev).await.is_err() {
                        break;
                    }
                }
            }
            out = ui_rx.recv() => {
                let msg = match out {
                    Ok(UiOut::Text(t)) => Message::Text(t.into()),
                    Ok(UiOut::TextTo(to, t)) if to == id => Message::Text(t.into()),
                    Ok(UiOut::Pcm(to, b)) if to == id => Message::Binary(b.into()),
                    Ok(UiOut::Close(to)) if to == id => {
                        // The account is gone: the page will find its session gone too.
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(id, n, "console lagging, messages dropped");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        }
    }
    info!(id, %addr, "console disconnected");
    let _ = state.events.send(Event::ClientLeft(id)).await;
}

fn ssi(v: &Value, key: &str) -> u32 {
    v.get(key).and_then(Value::as_u64).and_then(|n| u32::try_from(n).ok()).unwrap_or(0)
}

fn parse_command(text: &str) -> Option<UiCmd> {
    let v: Value = serde_json::from_str(text).ok()?;
    Some(match v.get("type")?.as_str()? {
        "claim" => UiCmd::Claim,
        "release" => UiCmd::Release,
        "groups" => UiCmd::SetGroups {
            listen: v.get("listen")?.as_array()?.iter()
                .filter_map(Value::as_u64)
                .filter_map(|n| u32::try_from(n).ok())
                .collect(),
            tx: ssi(&v, "tx"),
        },
        "ptt" => UiCmd::Ptt { down: v.get("down").and_then(Value::as_bool).unwrap_or(false) },
        "call" => UiCmd::Call { issi: ssi(&v, "issi"), duplex: v.get("duplex").and_then(Value::as_bool).unwrap_or(false) },
        "answer" => UiCmd::Answer,
        "hangup" => UiCmd::Hangup,
        "sds" => UiCmd::Sds { dest: ssi(&v, "to"), text: v.get("text")?.as_str()?.to_string() },
        "ambience" => UiCmd::AmbienceListen {
            issi: ssi(&v, "issi"),
            password: v.get("password").and_then(Value::as_str).unwrap_or("").to_string(),
        },
        "locate" => UiCmd::LocationRequest {
            issi: ssi(&v, "issi"),
            period_s: v.get("period_s").and_then(Value::as_u64).and_then(|n| u32::try_from(n).ok()).unwrap_or(0),
        },
        "locate_stop" => UiCmd::LocationStop { issi: ssi(&v, "issi") },
        "rec_start" => UiCmd::RecordStart { name: v.get("name")?.as_str()?.to_string() },
        "rec_stop" => UiCmd::RecordStop,
        "rec_delete" => UiCmd::RecordDelete { file: v.get("file")?.as_str()?.to_string() },
        "block" => UiCmd::Block { issi: ssi(&v, "issi"), block: v.get("block").and_then(Value::as_bool).unwrap_or(true) },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_console_commands() {
        assert!(matches!(parse_command(r#"{"type":"ptt","down":true}"#), Some(UiCmd::Ptt { down: true })));
        assert!(matches!(
            parse_command(r#"{"type":"groups","listen":[91,92],"tx":92}"#),
            Some(UiCmd::SetGroups { ref listen, tx: 92 }) if listen == &vec![91, 92]
        ));
        assert!(matches!(parse_command(r#"{"type":"sds","to":2001,"text":"hi"}"#), Some(UiCmd::Sds { dest: 2001, .. })));
        assert!(matches!(parse_command(r#"{"type":"block","issi":4013,"block":false}"#), Some(UiCmd::Block { issi: 4013, block: false })));
        assert!(matches!(parse_command(r#"{"type":"rec_start","name":"a"}"#), Some(UiCmd::RecordStart { .. })));
        assert!(matches!(parse_command(r#"{"type":"rec_stop"}"#), Some(UiCmd::RecordStop)));
        assert!(matches!(parse_command(r#"{"type":"rec_delete","file":"a.wav"}"#), Some(UiCmd::RecordDelete { .. })));
        assert!(parse_command(r#"{"type":"nope"}"#).is_none());
        assert!(parse_command("not json").is_none());
    }

    #[test]
    fn session_cookie_identifies_the_account() {
        let (events, _) = mpsc::channel(1);
        let (ui, _) = broadcast::channel(1);
        let dir = std::env::temp_dir().join(format!("td-web-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tetra-dispatch.toml");
        std::fs::write(&path, "[brew]\nhost = \"h:9000\"\n").unwrap();
        let mut ops = std::collections::BTreeMap::new();
        ops.insert("dave".to_string(), crate::config::OperatorConfig { password: "pw".into(), issi: 9990001, role: Role::Admin, groups: None, tx_group: 0 });
        let accounts = Arc::new(Mutex::new(Accounts::new(ops, path)));
        let state = AppState { events, ui, auth: Auth::Accounts(accounts.clone()), secure: true, next_id: Arc::new(AtomicU64::new(1)), recordings: Arc::new(dir.clone()) };
        let mut headers = HeaderMap::new();
        assert!(authorized(&state, &headers).is_none(), "no cookie, no access");
        headers.insert(header::COOKIE, "td_session=nope".parse().unwrap());
        assert!(authorized(&state, &headers).is_none());
        let token = accounts.lock().unwrap().login("dave", "pw").unwrap();
        headers.insert(header::COOKIE, format!("x=1; td_session={token}").parse().unwrap());
        assert!(matches!(authorized(&state, &headers), Some(Who::Account { ref name, role: Role::Admin }) if name == "dave"));
        assert!(admin(&state, &headers).is_ok());
        let c = cookie(&state, &token, None);
        assert!(c.contains("HttpOnly") && c.contains("SameSite=Strict") && c.contains("Secure"));
        accounts.lock().unwrap().logout(&token);
        assert!(authorized(&state, &headers).is_none());
        // Without accounts, the Basic password works as before.
        let basic = AppState { auth: Auth::Basic(Arc::new("s3cret".into())), ..state.clone() };
        let mut h = HeaderMap::new();
        assert!(authorized(&basic, &h).is_none());
        h.insert(header::AUTHORIZATION, format!("Basic {}", base64::engine::general_purpose::STANDARD.encode("any:s3cret")).parse().unwrap());
        assert!(matches!(authorized(&basic, &h), Some(Who::Anyone)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
