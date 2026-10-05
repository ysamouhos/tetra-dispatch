//! Console web server: the page, and one WebSocket per console carrying JSON
//! commands/status and binary PCM16LE @ 8 kHz audio both ways.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use anyhow::Context;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::config::WebConfig;
use crate::dispatcher::{ClientId, Event, UiCmd, UiOut};

const INDEX_HTML: &str = include_str!("../static/index.html");
/// Longest microphone chunk accepted in one message (1 s).
const MAX_PCM_SAMPLES: usize = 8_000;

#[derive(Clone)]
struct AppState {
    events: mpsc::Sender<Event>,
    ui: broadcast::Sender<UiOut>,
    password: Arc<String>,
    next_id: Arc<AtomicU64>,
}

pub async fn run(cfg: WebConfig, events: mpsc::Sender<Event>, ui: broadcast::Sender<UiOut>) -> anyhow::Result<()> {
    let state = AppState { events, ui, password: Arc::new(cfg.password.clone()), next_id: Arc::new(AtomicU64::new(1)) };
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_upgrade))
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

/// HTTP Basic with any user name, when a console password is configured.
fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    if state.password.is_empty() {
        return true;
    }
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b.trim()).ok())
        .and_then(|raw| String::from_utf8(raw).ok())
        .and_then(|s| s.split_once(':').map(|(_, p)| p.to_string()))
        .is_some_and(|p| constant_time_eq(p.as_bytes(), state.password.as_bytes()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn challenge() -> Response {
    (StatusCode::UNAUTHORIZED, [(header::WWW_AUTHENTICATE, "Basic realm=\"Tetra Dispatch\"")], "authentication required\n").into_response()
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return challenge();
    }
    ([(header::CACHE_CONTROL, "no-store")], Html(INDEX_HTML)).into_response()
}

async fn ws_upgrade(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !authorized(&state, &headers) {
        return challenge();
    }
    ws.on_upgrade(move |socket| console(state, socket, addr))
}

async fn console(state: AppState, socket: WebSocket, addr: SocketAddr) {
    let id: ClientId = state.next_id.fetch_add(1, Ordering::Relaxed);
    let mut ui_rx = state.ui.subscribe();
    if state.events.send(Event::ClientJoined(id, addr.ip().to_string())).await.is_err() {
        return;
    }
    info!(id, %addr, "console connected");
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
        assert!(parse_command(r#"{"type":"nope"}"#).is_none());
        assert!(parse_command("not json").is_none());
    }
}
