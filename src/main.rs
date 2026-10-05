//! Tetra Dispatch: a browser TETRA dispatch console that logs straight into a
//! brew-server as a Brew client, with its own operator ISSI.

mod brew_link;
mod codec;
mod config;
mod dispatcher;
#[allow(dead_code)]
mod protocol;
mod sds;
mod web;

use std::path::PathBuf;
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
        r = web::run(cfg.web.listen.clone(), cfg.web.password.clone(), events_tx, ui_tx) => r,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
    }
}
