//! The connection to the brew-server: the same discovery / Digest / WebSocket
//! sequence a Basestation performs (adapted from brew-server's federation
//! dialer), redialled forever. Frames are handed to the dispatcher as raw
//! bytes; the dispatcher owns all Brew state.

use crate::config::BrewConfig;
use crate::dispatcher::Event;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{pem::PemObject, CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

const USER_AGENT: &str = concat!("TetraDispatch/", env!("CARGO_PKG_VERSION"));
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// No frame (data or pong) for this long closes the link and redials.
const SILENCE_TIMEOUT: Duration = Duration::from_secs(30);
const OUTBOUND_CAP: usize = 512;

pub async fn run(cfg: BrewConfig, events: mpsc::Sender<Event>) {
    let retry = Duration::from_secs(cfg.reconnect_seconds.max(1));
    loop {
        match connect(&cfg).await {
            Ok(ws) => {
                info!(host = %cfg.host, "brew: connected");
                session(ws, &events).await;
                if events.is_closed() {
                    return;
                }
            }
            Err(e) => {
                warn!(host = %cfg.host, "brew: connect failed: {e:#}");
                let _ = events.send(Event::LinkDown(format!("{e:#}"))).await;
            }
        }
        tokio::time::sleep(retry).await;
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Ws = tokio_tungstenite::WebSocketStream<Box<dyn Io>>;

async fn session(ws: Ws, events: &mpsc::Sender<Event>) {
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(OUTBOUND_CAP);
    if events.send(Event::LinkUp(out_tx)).await.is_err() {
        return;
    }
    let (mut sink, mut stream) = ws.split();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await;
    let mut last_rx = tokio::time::Instant::now();
    let reason = loop {
        tokio::select! {
            msg = stream.next() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    last_rx = tokio::time::Instant::now();
                    if events.send(Event::Brew(data.to_vec())).await.is_err() {
                        break "dispatcher stopped".to_string();
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    break format!("closed by brew-server{}", frame.map(|f| format!(": {}", f.reason)).unwrap_or_default());
                }
                Some(Ok(_)) => last_rx = tokio::time::Instant::now(),
                Some(Err(e)) => break e.to_string(),
                None => break "connection closed".to_string(),
            },
            out = out_rx.recv() => match out {
                Some(data) => {
                    if let Err(e) = sink.send(Message::Binary(data.into())).await {
                        break e.to_string();
                    }
                }
                None => break "dispatcher stopped".to_string(),
            },
            _ = ping.tick() => {
                if last_rx.elapsed() > SILENCE_TIMEOUT {
                    break format!("no traffic for {}s", SILENCE_TIMEOUT.as_secs());
                }
                if let Err(e) = sink.send(Message::Ping(Vec::new().into())).await {
                    break e.to_string();
                }
            }
        }
    };
    warn!("brew: link down: {reason}");
    let _ = sink.close().await;
    let _ = events.send(Event::LinkDown(reason)).await;
}

async fn connect(cfg: &BrewConfig) -> anyhow::Result<Ws> {
    let tls = if cfg.tls { Some(tls_connector(cfg)?) } else { None };
    tokio::time::timeout(DIAL_TIMEOUT, async {
        let path = normalize_path(&cfg.path, true);
        let ws_path = discover(cfg, tls.as_ref(), &path).await?;
        let scheme = if tls.is_some() { "wss" } else { "ws" };
        let mut request = format!("{scheme}://{}{}", cfg.host, normalize_path(&ws_path, false)).into_client_request()?;
        request.headers_mut().insert("User-Agent", USER_AGENT.parse()?);
        request.headers_mut().insert("Sec-WebSocket-Protocol", "brew".parse()?);
        let stream = dial(cfg, tls.as_ref()).await?;
        let (ws, _resp) = tokio_tungstenite::client_async(request, stream).await?;
        anyhow::Ok(ws)
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out after {}s", DIAL_TIMEOUT.as_secs()))?
}

/// Discovery path keeps its trailing slash (brew-server serves both); the
/// returned upgrade path is used as given.
fn normalize_path(path: &str, keep_slash: bool) -> String {
    let mut p = if path.starts_with('/') { path.to_string() } else { format!("/{path}") };
    if !keep_slash {
        while p.len() > 1 && p.ends_with('/') {
            p.pop();
        }
    }
    p
}

async fn dial(cfg: &BrewConfig, tls: Option<&TlsConnector>) -> anyhow::Result<Box<dyn Io>> {
    let tcp = TcpStream::connect(&cfg.host).await.with_context(|| format!("connecting to {}", cfg.host))?;
    tcp.set_nodelay(true)?;
    match tls {
        None => Ok(Box::new(tcp)),
        Some(c) => {
            let name = tls_server_name(cfg);
            let server_name = ServerName::try_from(name.to_string())
                .with_context(|| format!("invalid TLS server name {name:?}"))?;
            let stream = c.connect(server_name, tcp).await
                .with_context(|| format!("TLS handshake with {}", cfg.host))?;
            Ok(Box::new(stream))
        }
    }
}

fn tls_server_name(cfg: &BrewConfig) -> &str {
    if !cfg.tls_server_name.is_empty() {
        return &cfg.tls_server_name;
    }
    let host = cfg.host.as_str();
    if let Some(rest) = host.strip_prefix('[') {
        return rest.split_once(']').map_or(rest, |(h, _)| h);
    }
    host.rsplit_once(':').map_or(host, |(h, _)| h)
}

fn tls_connector(cfg: &BrewConfig) -> anyhow::Result<TlsConnector> {
    let builder = rustls::ClientConfig::builder();
    let config = if !cfg.tls_pinned_cert_path.as_os_str().is_empty() {
        let cert = CertificateDer::from_pem_file(&cfg.tls_pinned_cert_path)
            .with_context(|| format!("reading tls_pinned_cert_path {}", cfg.tls_pinned_cert_path.display()))?;
        let algs = builder.crypto_provider().signature_verification_algorithms;
        builder.dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCert { cert, algs }))
            .with_no_client_auth()
    } else {
        let certs = CertificateDer::pem_file_iter(&cfg.tls_ca_path)
            .and_then(|it| it.collect::<Result<Vec<_>, _>>())
            .with_context(|| format!("reading tls_ca_path {}", cfg.tls_ca_path.display()))?;
        let mut roots = rustls::RootCertStore::empty();
        let (added, _ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            anyhow::bail!("no usable CA certificate in {}", cfg.tls_ca_path.display());
        }
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Trusts exactly one certificate (a self-signed brew-server). The handshake
/// signature is still verified against its key.
#[derive(Debug)]
struct PinnedCert {
    cert: CertificateDer<'static>,
    algs: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedCert {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.cert.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::ApplicationVerificationFailure))
        }
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algs)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algs)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algs.supported_schemes()
    }
}

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn http_get(cfg: &BrewConfig, tls: Option<&TlsConnector>, path: &str, authorization: Option<&str>) -> anyhow::Result<HttpResponse> {
    let mut stream = dial(cfg, tls).await?;
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: {USER_AGENT}\r\nX-Brew-Version: 1\r\nConnection: close\r\n",
        cfg.host
    );
    if let Some(a) = authorization {
        req.push_str(&format!("Authorization: {a}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let mut buf = Vec::new();
    match stream.read_to_end(&mut buf).await {
        Ok(_) => {}
        // A TLS server may close without close_notify; the response is complete.
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && !buf.is_empty() => {}
        Err(e) => return Err(e.into()),
    }
    parse_http_response(&buf)
}

fn parse_http_response(buf: &[u8]) -> anyhow::Result<HttpResponse> {
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response"))?;
    let header_text = std::str::from_utf8(&buf[..split])?;
    let mut lines = header_text.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line.split_whitespace().nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| anyhow::anyhow!("bad HTTP status line: {status_line}"))?;
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    Ok(HttpResponse { status, headers, body: buf[split + 4..].to_vec() })
}

/// Brew discovery: one GET, or the 401 Digest challenge and an authorized
/// retry. Returns the path to upgrade the WebSocket at.
async fn discover(cfg: &BrewConfig, tls: Option<&TlsConnector>, path: &str) -> anyhow::Result<String> {
    let resp = http_get(cfg, tls, path, None).await?;
    let resp = match resp.status {
        200 => resp,
        401 => {
            if cfg.username.is_empty() {
                anyhow::bail!("brew-server requires authentication: set brew.username / brew.password");
            }
            let challenge = parse_digest_params(
                resp.headers.get("www-authenticate").ok_or_else(|| anyhow::anyhow!("401 with no WWW-Authenticate"))?,
            );
            let cnonce = uuid::Uuid::new_v4().simple().to_string();
            let authz = digest_authorization(&challenge, &cfg.username, &cfg.password, "GET", path, &cnonce, 1);
            let resp = http_get(cfg, tls, path, Some(&authz)).await?;
            if resp.status != 200 {
                anyhow::bail!("brew-server rejected the credentials (HTTP {})", resp.status);
            }
            resp
        }
        other => anyhow::bail!("unexpected discovery status {other}"),
    };
    let ws_path = String::from_utf8(resp.body)?.trim().to_string();
    if ws_path.is_empty() {
        Ok(path.to_string())
    } else {
        Ok(ws_path)
    }
}

fn parse_digest_params(header: &str) -> HashMap<String, String> {
    let value = header.trim().strip_prefix("Digest ").unwrap_or(header.trim());
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in value.chars() {
        match c {
            '"' => { in_quotes = !in_quotes; cur.push(c); }
            ',' if !in_quotes => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    parts.push(cur);
    parts
        .iter()
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().trim_matches('"').to_string()))
        .collect()
}

fn md5_hex(s: &str) -> String {
    format!("{:x}", md5::compute(s.as_bytes()))
}

fn digest_authorization(challenge: &HashMap<String, String>, username: &str, password: &str, method: &str, uri: &str, cnonce: &str, nc: u32) -> String {
    let get = |k: &str| challenge.get(k).map(String::as_str);
    let realm = get("realm").unwrap_or("");
    let nonce = get("nonce").unwrap_or("");
    let nc = format!("{nc:08x}");
    let ha1 = md5_hex(&format!("{username}:{realm}:{password}"));
    let ha2 = md5_hex(&format!("{method}:{uri}"));
    let qop_auth = get("qop").is_some_and(|q| q.contains("auth"));
    let response = if qop_auth {
        md5_hex(&format!("{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}"))
    } else {
        md5_hex(&format!("{ha1}:{nonce}:{ha2}"))
    };
    let mut out = format!(
        "Digest username=\"{username}\", realm=\"{realm}\", nonce=\"{nonce}\", uri=\"{uri}\", response=\"{response}\", algorithm=MD5"
    );
    if qop_auth {
        out.push_str(&format!(", qop=auth, nc={nc}, cnonce=\"{cnonce}\""));
    }
    if let Some(o) = get("opaque") {
        out.push_str(&format!(", opaque=\"{o}\""));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_rfc2617_example() {
        let challenge = parse_digest_params(
            r#"Digest realm="testrealm@host.com", qop="auth,auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", opaque="5ccc069c403ebaf9f0171e9517f40e41""#,
        );
        let h = digest_authorization(&challenge, "Mufasa", "Circle Of Life", "GET", "/dir/index.html", "0a4f113b", 1);
        assert!(h.contains(r#"response="6629fae49393a05397450978507c4ef1""#), "{h}");
        assert!(h.contains(r#"opaque="5ccc069c403ebaf9f0171e9517f40e41""#));
    }

    #[test]
    fn parses_discovery_response() {
        let r = parse_http_response(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n/brew/session/abc").unwrap();
        assert_eq!(r.status, 200);
        assert_eq!(r.body, b"/brew/session/abc");
        assert_eq!(r.headers["content-type"], "text/plain");
    }

    #[test]
    fn server_name_from_host() {
        let mut cfg: crate::config::Config = toml::from_str("[brew]\nhost = \"brew.example.org:9000\"\n").unwrap();
        assert_eq!(tls_server_name(&cfg.brew), "brew.example.org");
        cfg.brew.host = "[::1]:9000".into();
        assert_eq!(tls_server_name(&cfg.brew), "::1");
    }
}
