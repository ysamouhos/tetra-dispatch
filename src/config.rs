//! `tetra-dispatch.toml`.

use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub brew: BrewConfig,
    #[serde(default)]
    pub dispatch: DispatchConfig,
    #[serde(default)]
    pub web: WebConfig,
}

/// The brew-server this console logs into, the same way a Basestation does.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrewConfig {
    /// `host:port` of the brew-server Brew listener (default port 9000).
    pub host: String,
    /// Discovery path (brew-server `websocket_path`).
    #[serde(default = "default_path")]
    pub path: String,
    /// `wss://` / `https://` when the brew-server has `[tls] enabled = true`.
    #[serde(default)]
    pub tls: bool,
    /// Name the certificate is checked against (and sent as SNI). Default: the host part of `host`.
    #[serde(default)]
    pub tls_server_name: String,
    /// CA bundle used to verify the brew-server certificate.
    #[serde(default = "default_ca_path")]
    pub tls_ca_path: PathBuf,
    /// Trust exactly this certificate instead (the usual way to reach a self-signed brew-server).
    #[serde(default)]
    pub tls_pinned_cert_path: PathBuf,
    /// Brew credentials from brew-server `[auth.users]` (numeric, up to 7 digits).
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    /// Seconds between reconnect attempts.
    #[serde(default = "default_reconnect")]
    pub reconnect_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchConfig {
    /// ISSI the dispatcher registers on the Brew network.
    #[serde(default = "default_operator_issi")]
    pub operator_issi: u32,
    /// Talkgroups listened to on start (the console can change the list).
    #[serde(default)]
    pub groups: Vec<u32>,
    /// Talkgroup PTT transmits on (0 = the first of `groups`).
    #[serde(default)]
    pub tx_group: u32,
    /// Brew priority of the dispatcher's group transmissions.
    #[serde(default)]
    pub priority: u8,
    /// Password that authorizes ambience-listening (AL) calls. Empty = no
    /// authorization required. When set, the console must supply it before an
    /// AL call starts, and every attempt is recorded in the activity log.
    #[serde(default)]
    pub ambience_password: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebConfig {
    /// Console listen address. Browsers only allow the microphone on
    /// `https://` or `localhost`, so serve it behind a TLS proxy for remote use.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Optional console password (HTTP Basic, any user name). Empty = open.
    #[serde(default)]
    pub password: String,
    /// Serve the console over HTTPS (needed for the microphone off localhost).
    #[serde(default)]
    pub tls: bool,
    /// PEM certificate chain and private key for `tls`.
    #[serde(default)]
    pub tls_cert_path: PathBuf,
    #[serde(default)]
    pub tls_key_path: PathBuf,
}

fn default_path() -> String { "/brew/".into() }
fn default_ca_path() -> PathBuf { "/etc/ssl/certs/ca-certificates.crt".into() }
fn default_reconnect() -> u64 { 5 }
fn default_operator_issi() -> u32 { 9_990_001 }
fn default_listen() -> String { "0.0.0.0:8443".into() }

impl Default for DispatchConfig {
    fn default() -> Self {
        Self { operator_issi: default_operator_issi(), groups: Vec::new(), tx_group: 0, priority: 0, ambience_password: String::new() }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            password: String::new(),
            tls: false,
            tls_cert_path: PathBuf::new(),
            tls_key_path: PathBuf::new(),
        }
    }
}

pub const MAX_SSI: u32 = 16_777_214;

pub fn valid_ssi(n: u32) -> bool {
    (1..=MAX_SSI).contains(&n)
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        let cfg: Config = toml::from_str(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if !valid_ssi(self.dispatch.operator_issi) {
            anyhow::bail!("dispatch.operator_issi must be 1..={MAX_SSI}");
        }
        if let Some(g) = self.dispatch.groups.iter().find(|g| !valid_ssi(**g)) {
            anyhow::bail!("dispatch.groups: invalid GSSI {g}");
        }
        if self.dispatch.tx_group != 0 && !valid_ssi(self.dispatch.tx_group) {
            anyhow::bail!("dispatch.tx_group: invalid GSSI {}", self.dispatch.tx_group);
        }
        if self.web.tls && (self.web.tls_cert_path.as_os_str().is_empty() || self.web.tls_key_path.as_os_str().is_empty()) {
            anyhow::bail!("web.tls needs web.tls_cert_path and web.tls_key_path");
        }
        if self.brew.username.is_empty() != self.brew.password.is_empty() {
            anyhow::bail!("brew.username and brew.password go together");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_parses() {
        let text = include_str!("../tetra-dispatch.toml");
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.web.listen, "0.0.0.0:8443");
    }

    #[test]
    fn minimal_config_takes_defaults() {
        let cfg: Config = toml::from_str("[brew]\nhost = \"127.0.0.1:9000\"\n").unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.brew.path, "/brew/");
        assert_eq!(cfg.dispatch.operator_issi, 9_990_001);
        assert!(!cfg.web.tls);
    }

    #[test]
    fn web_tls_needs_cert_and_key() {
        let cfg: Config = toml::from_str("[brew]\nhost = \"h:9000\"\n[web]\ntls = true\ntls_cert_path = \"c.pem\"\n").unwrap();
        assert!(cfg.validate().is_err());
    }
}
