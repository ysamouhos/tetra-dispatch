//! Operator accounts and browser sessions, when `[dispatch.operators]` is
//! configured. One store is shared by the web server (login, admin API) and
//! kept in step with the config file on disk.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use uuid::Uuid;

use crate::config::{self, OperatorConfig, Role};

/// Name of the session cookie.
pub const COOKIE: &str = "td_session";

pub struct Accounts {
    operators: BTreeMap<String, OperatorConfig>,
    /// Session token → account name. Tokens live in memory only: a restart
    /// logs every browser out.
    sessions: HashMap<String, String>,
    config_path: PathBuf,
}

/// Why an account change was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// Bad name, ISSI, role, groups or a missing password: the message says which.
    Invalid(String),
    LastAdmin,
    Yourself,
    NotFound,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refused::Invalid(m) => f.write_str(m),
            Refused::LastAdmin => f.write_str("cannot remove the last admin"),
            Refused::Yourself => f.write_str("cannot remove your own account"),
            Refused::NotFound => f.write_str("no such account"),
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

impl Accounts {
    pub fn new(operators: BTreeMap<String, OperatorConfig>, config_path: PathBuf) -> Self {
        Self { operators, sessions: HashMap::new(), config_path }
    }

    pub fn get(&self, name: &str) -> Option<&OperatorConfig> {
        self.operators.get(name)
    }

    pub fn operators(&self) -> &BTreeMap<String, OperatorConfig> {
        &self.operators
    }

    /// Checks a name and password and opens a session; the token goes in the
    /// cookie. A wrong name takes as long as a wrong password.
    pub fn login(&mut self, name: &str, password: &str) -> Option<String> {
        let stored = self.operators.get(name).map(|o| o.password.as_bytes()).unwrap_or(b"");
        if stored.is_empty() || !constant_time_eq(stored, password.as_bytes()) {
            return None;
        }
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        self.sessions.insert(token.clone(), name.to_string());
        Some(token)
    }

    pub fn logout(&mut self, token: &str) {
        self.sessions.remove(token);
    }

    /// The account a session token belongs to, if it is still valid.
    pub fn session(&self, token: &str) -> Option<&str> {
        self.sessions.get(token).map(String::as_str)
    }

    pub fn role(&self, name: &str) -> Option<Role> {
        self.operators.get(name).map(|o| o.role)
    }

    /// Adds or replaces an account, writes the config file and returns the
    /// account as stored (the dispatcher applies it live). `password` None
    /// keeps the current one.
    pub fn upsert(&mut self, name: &str, password: Option<String>, issi: u32, role: Role, groups: Option<Vec<u32>>, tx_group: u32) -> Result<OperatorConfig, Refused> {
        let current = self.operators.get(name);
        let password = match password.filter(|p| !p.is_empty()) {
            Some(p) => p,
            None => current.map(|o| o.password.clone()).ok_or_else(|| Refused::Invalid("a new account needs a password".into()))?,
        };
        if current.is_some_and(|o| o.role == Role::Admin) && role != Role::Admin && self.admins() == 1 {
            return Err(Refused::LastAdmin);
        }
        let account = OperatorConfig { password, issi, role, groups, tx_group };
        let mut next = self.operators.clone();
        next.insert(name.to_string(), account.clone());
        config::validate_operators(&next).map_err(|e| Refused::Invalid(e.to_string()))?;
        self.commit(next)?;
        Ok(account)
    }

    /// Removes an account and its sessions. Refuses the last admin and the
    /// caller's own account.
    pub fn remove(&mut self, name: &str, by: &str) -> Result<(), Refused> {
        let Some(victim) = self.operators.get(name) else { return Err(Refused::NotFound) };
        if name == by {
            return Err(Refused::Yourself);
        }
        if victim.role == Role::Admin && self.admins() == 1 {
            return Err(Refused::LastAdmin);
        }
        let mut next = self.operators.clone();
        next.remove(name);
        self.commit(next)?;
        self.sessions.retain(|_, n| n != name);
        Ok(())
    }

    fn admins(&self) -> usize {
        self.operators.values().filter(|o| o.role == Role::Admin).count()
    }

    /// Writes `next` to the config file, then makes it the live set: a failed
    /// write changes nothing.
    fn commit(&mut self, next: BTreeMap<String, OperatorConfig>) -> Result<(), Refused> {
        config::save_operators(&self.config_path, &next).map_err(|e| Refused::Invalid(format!("saving the config file: {e}")))?;
        self.operators = next;
        Ok(())
    }

    /// Account names with a live session.
    pub fn online(&self) -> Vec<&str> {
        let mut v: Vec<&str> = self.sessions.values().map(String::as_str).collect();
        v.sort_unstable();
        v.dedup();
        v
    }
}

/// The `td_session` value from a `Cookie` header.
pub fn cookie_token(cookie_header: &str) -> Option<&str> {
    cookie_header.split(';').map(str::trim).find_map(|kv| kv.strip_prefix(COOKIE).and_then(|v| v.strip_prefix('=')))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (Accounts, PathBuf) {
        let dir = std::env::temp_dir().join(format!("td-acc-{}-{}", std::process::id(), Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tetra-dispatch.toml");
        std::fs::write(&path, "[brew]\nhost = \"h:9000\"\n").unwrap();
        let mut ops = BTreeMap::new();
        ops.insert("dave".to_string(), OperatorConfig { password: "secret".into(), issi: 9990001, role: Role::Admin, groups: None, tx_group: 0 });
        ops.insert("eve".to_string(), OperatorConfig { password: "pw".into(), issi: 9990002, role: Role::Operator, groups: None, tx_group: 0 });
        (Accounts::new(ops, path), dir)
    }

    #[test]
    fn login_opens_a_session_and_logout_closes_it() {
        let (mut a, dir) = store();
        assert!(a.login("dave", "wrong").is_none());
        assert!(a.login("nobody", "secret").is_none());
        assert!(a.login("dave", "").is_none());
        let token = a.login("dave", "secret").unwrap();
        assert_eq!(token.len(), 64);
        assert_eq!(a.session(&token), Some("dave"));
        assert_eq!(a.online(), vec!["dave"]);
        assert!(a.session("bogus").is_none());
        a.logout(&token);
        assert!(a.session(&token).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cookie_header_is_parsed() {
        assert_eq!(cookie_token("a=b; td_session=abc123; c=d"), Some("abc123"));
        assert_eq!(cookie_token("td_session=x"), Some("x"));
        assert_eq!(cookie_token("other=1"), None);
    }

    #[test]
    fn account_changes_are_checked_and_saved() {
        let (mut a, dir) = store();
        // A new account needs a password and a free ISSI.
        assert!(matches!(a.upsert("new", None, 9990003, Role::Operator, None, 0), Err(Refused::Invalid(_))));
        assert!(matches!(a.upsert("new", Some("x".into()), 9990002, Role::Operator, None, 0), Err(Refused::Invalid(_))));
        assert!(matches!(a.upsert("bad name", Some("x".into()), 9990003, Role::Operator, None, 0), Err(Refused::Invalid(_))));
        let acc = a.upsert("new", Some("x".into()), 9990003, Role::Operator, Some(vec![91]), 91).unwrap();
        assert_eq!(acc.issi, 9990003);
        // An update without a password keeps the old one.
        a.upsert("new", None, 9990004, Role::Viewer, None, 0).unwrap();
        assert_eq!(a.get("new").unwrap().password, "x");
        assert_eq!(a.get("new").unwrap().role, Role::Viewer);
        // The file follows.
        let saved = crate::config::Config::load(&a.config_path).unwrap();
        assert_eq!(saved.dispatch.operators, *a.operators());
        // The last admin cannot be demoted or removed, nor can you remove yourself.
        assert_eq!(a.upsert("dave", None, 9990001, Role::Operator, None, 0), Err(Refused::LastAdmin));
        assert_eq!(a.remove("dave", "eve"), Err(Refused::LastAdmin));
        assert_eq!(a.remove("eve", "eve"), Err(Refused::Yourself));
        assert_eq!(a.remove("ghost", "dave"), Err(Refused::NotFound));
        // Removing an account closes its sessions.
        let t = a.login("eve", "pw").unwrap();
        a.remove("eve", "dave").unwrap();
        assert!(a.session(&t).is_none());
        assert!(a.get("eve").is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
