//! Tetra Dispatch: a browser TETRA dispatch console that logs straight into a
//! brew-server as a Brew client, with its own operator ISSI.

mod accounts;
mod brew_link;
mod codec;
mod config;
mod dispatcher;
#[allow(dead_code)]
mod protocol;
mod recording;
mod sds;
mod web;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let path = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "tetra-dispatch.toml".into()));
    let cfg = config::Config::load(&path)?;
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        brew = %cfg.brew.host,
        issi = cfg.dispatch.operator_issi,
        "Tetra Dispatch starting"
    );

    // Operator accounts replace the single position and the console password.
    let auth = if cfg.dispatch.operators.is_empty() {
        web::Auth::Basic(Arc::new(cfg.web.password.clone()))
    } else {
        if !cfg.web.password.is_empty() {
            tracing::warn!("[web] password is ignored: operator accounts are configured, the console asks for a login");
        }
        for (name, o) in &cfg.dispatch.operators {
            tracing::info!(name, issi = o.issi, role = o.role.as_str(), "operator account");
        }
        web::Auth::Accounts(Arc::new(Mutex::new(accounts::Accounts::new(cfg.dispatch.operators.clone(), path.clone()))))
    };

    let (events_tx, events_rx) = mpsc::channel(1024);
    let (ui_tx, _) = broadcast::channel(1024);

    let dispatcher = dispatcher::Dispatcher::new(cfg.dispatch.clone(), ui_tx.clone());
    tokio::spawn(dispatcher.run(events_rx));
    tokio::spawn(brew_link::run(cfg.brew.clone(), events_tx.clone()));

    let ticker = events_tx.clone();
    tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_millis(500));
        loop {
            t.tick().await;
            if ticker.send(dispatcher::Event::Tick).await.is_err() {
                break;
            }
        }
    });

    tokio::select! {
        r = web::run(cfg.web.clone(), auth, cfg.dispatch.recordings_dir.clone(), events_tx, ui_tx) => r,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
    }
}
