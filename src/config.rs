//! `tetra-dispatch.toml` (an example is in `sample/`).

use serde::Deserialize;
use std::collections::BTreeMap;
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
    /// Directory server-side voice recordings (WAV) are written to.
    #[serde(default = "default_recordings_dir")]
    pub recordings_dir: PathBuf,
    /// Operator accounts (`[dispatch.operators.<name>]`). Empty = the single
    /// operator position of `operator_issi`, open to the first console that
    /// takes it. With accounts, the console asks for a name and password, and
    /// every account with a seat registers its own ISSI on the network.
    #[serde(default)]
    pub operators: BTreeMap<String, OperatorConfig>,
}

/// One operator account. Passwords are plain text, like brew-server's.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorConfig {
    #[serde(default)]
    pub password: String,
    /// This operator's ISSI on the network; unique per operator. A viewer has
    /// none (0).
    #[serde(default)]
    pub issi: u32,
    #[serde(default)]
    pub role: Role,
    /// Initial listen list (default: `[dispatch] groups`).
    #[serde(default)]
    pub groups: Option<Vec<u32>>,
    /// Talkgroup PTT transmits on (0 = the first of `groups`).
    #[serde(default)]
    pub tx_group: u32,
}

/// What an account may do. A viewer watches; an operator holds a seat; an
/// admin also manages accounts from the console.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    #[default]
    Operator,
    Viewer,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Operator => "operator",
            Role::Viewer => "viewer",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "admin" => Some(Role::Admin),
            "operator" => Some(Role::Operator),
            "viewer" => Some(Role::Viewer),
            _ => None,
        }
    }

    /// Whether this role gets a seat (an ISSI, a listen list, PTT).
    pub fn has_seat(self) -> bool {
        self != Role::Viewer
    }
}

/// Longest account name; names are `[A-Za-z0-9_.-]`.
pub const MAX_OPERATOR_NAME: usize = 32;

pub fn valid_operator_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_OPERATOR_NAME && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

/// Checks a set of accounts the way `Config::load` does: names, passwords,
/// ISSIs (valid, present for every seat, unique) and talkgroups.
pub fn validate_operators(ops: &BTreeMap<String, OperatorConfig>) -> anyhow::Result<()> {
    let mut issis: BTreeMap<u32, &str> = BTreeMap::new();
    for (name, o) in ops {
        if !valid_operator_name(name) {
            anyhow::bail!("dispatch.operators: invalid account name {name:?} (letters, digits, '_', '.', '-'; up to {MAX_OPERATOR_NAME})");
        }
        if o.password.is_empty() {
            anyhow::bail!("dispatch.operators.{name}: password is required");
        }
        if o.role.has_seat() && !valid_ssi(o.issi) {
            anyhow::bail!("dispatch.operators.{name}: issi must be 1..={MAX_SSI}");
        }
        if o.issi != 0 {
            if !valid_ssi(o.issi) {
                anyhow::bail!("dispatch.operators.{name}: issi must be 1..={MAX_SSI}");
            }
            if let Some(other) = issis.insert(o.issi, name) {
                anyhow::bail!("dispatch.operators: {name} and {other} share ISSI {}", o.issi);
            }
        }
        if let Some(g) = o.groups.iter().flatten().find(|g| !valid_ssi(**g)) {
            anyhow::bail!("dispatch.operators.{name}.groups: invalid GSSI {g}");
        }
        if o.tx_group != 0 && !valid_ssi(o.tx_group) {
            anyhow::bail!("dispatch.operators.{name}.tx_group: invalid GSSI {}", o.tx_group);
        }
    }
    Ok(())
}

/// Rewrites the `dispatch.operators` table of the config file at `path` and
/// writes the result atomically (sibling temp file, then rename), so a crash
/// mid-write never leaves a partial file. The rest of the file, comments
/// included, is kept as it is.
pub fn save_operators(path: &Path, ops: &BTreeMap<String, OperatorConfig>) -> anyhow::Result<()> {
    use toml_edit::{value, Array, DocumentMut, Item, Table};
    let text = std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
    let mut doc: DocumentMut = text.parse().map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
    let dispatch = doc
        .entry("dispatch")
        .or_insert_with(|| {
            let mut t = Table::new();
            t.set_implicit(true);
            Item::Table(t)
        })
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("{}: [dispatch] is not a table", path.display()))?;
    if ops.is_empty() {
        dispatch.remove("operators");
    } else {
        let mut table = Table::new();
        table.set_implicit(true);
        for (name, o) in ops {
            let mut t = Table::new();
            t["password"] = value(&o.password);
            if o.issi != 0 {
                t["issi"] = value(o.issi as i64);
            }
            t["role"] = value(o.role.as_str());
            if let Some(groups) = &o.groups {
                t["groups"] = value(groups.iter().map(|g| *g as i64).collect::<Array>());
            }
            if o.tx_group != 0 {
                t["tx_group"] = value(o.tx_group as i64);
            }
            table.insert(name, Item::Table(t));
        }
        dispatch.insert("operators", Item::Table(table));
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, doc.to_string()).map_err(|e| anyhow::anyhow!("writing {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| anyhow::anyhow!("renaming {} to {}: {e}", tmp.display(), path.display()))?;
    Ok(())
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
fn default_recordings_dir() -> PathBuf { "recordings".into() }
fn default_listen() -> String { "0.0.0.0:8443".into() }

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            operator_issi: default_operator_issi(),
            groups: Vec::new(),
            tx_group: 0,
            priority: 0,
            ambience_password: String::new(),
            recordings_dir: default_recordings_dir(),
            operators: BTreeMap::new(),
        }
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
        validate_operators(&self.dispatch.operators)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_parses() {
        let text = include_str!("../sample/tetra-dispatch.toml");
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

    const OPERATORS: &str = "[brew]\nhost = \"h:9000\"\n[dispatch]\ngroups = [91]\n\
        [dispatch.operators.dave]\npassword = \"a\"\nissi = 9990001\nrole = \"admin\"\ngroups = [91, 92]\ntx_group = 91\n\
        [dispatch.operators.eve]\npassword = \"b\"\nissi = 9990002\n\
        [dispatch.operators.watch]\npassword = \"c\"\nrole = \"viewer\"\n";

    #[test]
    fn operators_parse_with_defaults() {
        let cfg: Config = toml::from_str(OPERATORS).unwrap();
        cfg.validate().unwrap();
        let ops = &cfg.dispatch.operators;
        assert_eq!(ops["dave"].role, Role::Admin);
        assert_eq!(ops["dave"].groups.as_deref(), Some(&[91, 92][..]));
        assert_eq!(ops["eve"].role, Role::Operator);
        assert_eq!(ops["eve"].groups, None);
        assert_eq!(ops["watch"].issi, 0);
        assert!(!ops["watch"].role.has_seat());
    }

    #[test]
    fn operators_need_unique_issis_and_passwords() {
        let dup = OPERATORS.replace("issi = 9990002", "issi = 9990001");
        let cfg: Config = toml::from_str(&dup).unwrap();
        assert!(cfg.validate().unwrap_err().to_string().contains("share ISSI"));
        let no_issi = OPERATORS.replace("issi = 9990002\n", "");
        let cfg: Config = toml::from_str(&no_issi).unwrap();
        assert!(cfg.validate().is_err(), "an operator needs an ISSI");
        let no_pw = OPERATORS.replace("password = \"b\"", "password = \"\"");
        let cfg: Config = toml::from_str(&no_pw).unwrap();
        assert!(cfg.validate().is_err(), "a password is required");
        let bad_name = OPERATORS.replace("operators.eve", "operators.\"e v\"");
        let cfg: Config = toml::from_str(&bad_name).unwrap();
        assert!(cfg.validate().is_err(), "names are [A-Za-z0-9_.-]");
    }

    #[test]
    fn save_operators_rewrites_only_that_table() {
        let dir = std::env::temp_dir().join(format!("td-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tetra-dispatch.toml");
        std::fs::write(&path, "# keep me\n[brew]\nhost = \"h:9000\" # and me\n\n[dispatch]\ngroups = [91]\n\n[web]\nlisten = \"0.0.0.0:1\"\n").unwrap();
        let mut ops = BTreeMap::new();
        ops.insert("dave".to_string(), OperatorConfig { password: "a".into(), issi: 9990001, role: Role::Admin, groups: Some(vec![91, 92]), tx_group: 91 });
        ops.insert("watch".to_string(), OperatorConfig { password: "c".into(), issi: 0, role: Role::Viewer, groups: None, tx_group: 0 });
        save_operators(&path, &ops).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("# keep me\n"));
        assert!(text.contains("host = \"h:9000\" # and me"));
        assert!(text.contains("[dispatch.operators.dave]"));
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.dispatch.operators, ops);
        assert_eq!(cfg.dispatch.groups, vec![91]);
        assert_eq!(cfg.web.listen, "0.0.0.0:1");
        // Removing every account removes the table.
        save_operators(&path, &BTreeMap::new()).unwrap();
        let cfg = Config::load(&path).unwrap();
        assert!(cfg.dispatch.operators.is_empty());
        assert!(!std::fs::read_to_string(&path).unwrap().contains("operators"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
