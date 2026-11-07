//! Dispatcher state machine: one task owns the Brew session state (registration,
//! affiliations, group and private calls, SDS) and the console audio.
//!
//! Inputs arrive as [`Event`]s from the Brew link and the web consoles;
//! outputs go to the brew-server as raw Brew frames and to the consoles as
//! [`UiOut`] broadcasts. Each operator account has a [`Seat`]: its own ISSI on
//! the network, listen list, TX group, calls and PTT. One console at a time
//! holds a seat ("claim"); the other consoles of that account watch it.
//! Without accounts there is a single seat, as there always was.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::codec::Codec;
use crate::config::{valid_ssi, DispatchConfig, OperatorConfig, Role};
use crate::protocol::*;
use crate::recording::{self, Recorder};
use crate::sds;

pub type ClientId = u64;

/// Disconnect causes (ETSI EN 300 392-2 §14.8.18) used here.
const CAUSE_USER_REQUESTED: u8 = 1;
/// TETRA basic service for an ambience-listening call. Experimental: the
/// brew-server only relays the private-call setup (it never reads this byte),
/// so ambience listening happens only if the destination radio/basestation
/// supports it. The exact value a given fleet expects may differ; adjust for
/// the deployment.
const SERVICE_AMBIENCE_LISTENING: u8 = 9;
const CAUSE_CALLED_PARTY_BUSY: u8 = 2;
const CAUSE_NOT_REACHABLE: u8 = 3;

/// Ended / failed calls stay on screen this long.
const TERMINAL_HOLD: Duration = Duration::from_secs(5);
/// A received group call with no voice or signalling for this long is dropped.
const RX_STALE: Duration = Duration::from_secs(10);
/// A second PTT within this window transmits over a busy talkgroup.
const PREEMPT_WINDOW: Duration = Duration::from_secs(3);
/// No answer within this long ends an outgoing private call.
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);
const LOG_CAP: usize = 1000;
const SDS_CAP: usize = 1000;

#[derive(Debug)]
pub enum Event {
    /// Brew session established; frames for the brew-server go to this sender.
    LinkUp(mpsc::Sender<Vec<u8>>),
    LinkDown(String),
    Brew(Vec<u8>),
    /// A console connected from this address (no accounts configured).
    ClientJoined(ClientId, String),
    /// A console connected from this address, logged in as this account.
    ClientLoggedIn(ClientId, String, String),
    ClientLeft(ClientId),
    Ui(ClientId, UiCmd),
    /// Microphone PCM16 @ 8 kHz from a console.
    UlPcm(ClientId, Vec<i16>),
    /// A recording was played or downloaded over HTTP, from this address.
    RecordingAccess { file: String, action: &'static str, from: String },
    /// An admin added or changed an account: apply it live.
    OperatorSet { name: String, account: OperatorConfig, by: String },
    /// An admin removed an account: drop its seat and log its consoles out.
    OperatorRemoved { name: String, by: String },
    Tick,
}

#[derive(Debug, Clone)]
pub enum UiCmd {
    Claim,
    Release,
    SetGroups { listen: Vec<u32>, tx: u32 },
    Ptt { down: bool },
    Call { issi: u32, duplex: bool },
    Answer,
    Hangup,
    Sds { dest: u32, text: String },
    /// Ambience-listening call to a radio (indicated on the radio). `password`
    /// is checked against `dispatch.ambience_password` when one is configured.
    AmbienceListen { issi: u32, password: String },
    /// Request a position report. `period_s` 0 = once; otherwise poll every
    /// `period_s` seconds. A new period for the same ISSI replaces the old.
    LocationRequest { issi: u32, period_s: u32 },
    /// Stop polling a radio's position.
    LocationStop { issi: u32 },
    /// Block (or unblock) an ISSI on the connected brew-server's blacklist.
    Block { issi: u32, block: bool },
    /// Start a server-side recording; `name` is the user's file name.
    RecordStart { name: String },
    RecordStop,
    RecordDelete { file: String },
}

#[derive(Debug, Clone)]
pub enum UiOut {
    /// JSON text for every console.
    Text(String),
    /// JSON text for one console.
    TextTo(ClientId, String),
    /// Received audio (PCM16LE @ 8 kHz) for the console holding a seat.
    Pcm(ClientId, Vec<u8>),
    /// Close one console's WebSocket (its account was removed).
    Close(ClientId),
}

struct RxCall {
    gssi: u32,
    source: u32,
    started: Instant,
    last: Instant,
    /// Emergency call (priority 15). brew-server pushes these to every console,
    /// whatever groups it listens to.
    emergency: bool,
    /// Decoder state for this call's voice (the codec is stateful per stream).
    codec: Codec,
    /// A seat of this console is the talker. brew-server never echoes a
    /// connection's own transmission, so the microphone audio is handed to
    /// the other seats listening to the group here.
    local: bool,
}

/// Call priority of an emergency call (ETSI EN 300 392-2 clause 14.8).
const EMERGENCY_PRIORITY: u8 = 15;

struct TxCall {
    uuid: Uuid,
    gssi: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Outgoing: SETUP_REQUEST sent.
    Dialing,
    /// Outgoing: the called radio is alerted.
    Alerting,
    /// Incoming: ringing on the console.
    Ringing,
    /// Incoming: CONNECT_REQUEST sent, waiting for CONNECT_CONFIRM.
    Answering,
    Connected,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Phase::Dialing => "dialing",
            Phase::Alerting => "alerting",
            Phase::Ringing => "ringing",
            Phase::Answering => "answering",
            Phase::Connected => "connected",
        }
    }
}

struct Private {
    uuid: Uuid,
    peer: u32,
    duplex: bool,
    inbound: bool,
    phase: Phase,
    /// The call as the caller sent it (incoming), echoed in CONNECT_REQUEST.
    call: Option<BrewCircularCall>,
    started: Instant,
    connected_at: Option<Instant>,
    /// Simplex: the console holds the floor.
    talking: bool,
    /// Simplex: the radio holds the floor.
    peer_talking: bool,
    /// Ambience-listening call: the radio is the only talker; the console
    /// never keys. The radio indicates the call like any other (not covert).
    ambience: bool,
    /// Decoder state for the peer's voice.
    codec: Codec,
}

impl Private {
    fn new(uuid: Uuid, peer: u32, duplex: bool, inbound: bool, phase: Phase, call: Option<BrewCircularCall>, ambience: bool) -> Self {
        Self {
            uuid,
            peer,
            duplex,
            inbound,
            phase,
            call,
            started: Instant::now(),
            connected_at: None,
            talking: false,
            peer_talking: false,
            ambience,
            codec: Codec::new(),
        }
    }
}

/// What the call panel shows for a few seconds after a private call ends.
struct Ended {
    peer: u32,
    duplex: bool,
    failed: bool,
    cause: u8,
    until: Instant,
}

/// Where a position poll goes and who asked (the ISSI the requests come from).
struct Poll {
    period: Duration,
    next: Instant,
    from: u32,
}

/// One operator position. With accounts, one per account (a viewer's seat
/// has no ISSI and cannot be taken); without, the single legacy position.
struct Seat {
    /// Account name; empty without accounts.
    name: String,
    role: Role,
    /// This seat's ISSI on the network (0 = none, a viewer).
    issi: u32,
    listen: BTreeSet<u32>,
    tx_group: u32,
    /// Groups currently affiliated on the brew-server under this ISSI.
    affiliated: BTreeSet<u32>,
    /// The received group call being played on this seat.
    playing: Option<Uuid>,
    tx: Option<TxCall>,
    preempt_until: Option<Instant>,
    private: Option<Private>,
    ended: Option<Ended>,
    ptt_down: bool,
    /// The console that holds the seat; its account's other consoles watch.
    owner: Option<ClientId>,
    /// Encoder state for this seat's microphone.
    codec: Codec,
    /// Server-side voice recording in progress, and when it began.
    recorder: Option<(Recorder, Instant)>,
    last_error: Option<String>,
}

impl Seat {
    fn new(name: &str, role: Role, issi: u32, groups: &[u32], tx_group: u32) -> Self {
        let listen: BTreeSet<u32> = groups.iter().copied().collect();
        let tx_group = if tx_group != 0 { tx_group } else { listen.iter().next().copied().unwrap_or(0) };
        Self {
            name: name.to_string(),
            role,
            issi: if role.has_seat() { issi } else { 0 },
            listen,
            tx_group,
            affiliated: BTreeSet::new(),
            playing: None,
            tx: None,
            preempt_until: None,
            private: None,
            ended: None,
            ptt_down: false,
            owner: None,
            codec: Codec::new(),
            recorder: None,
            last_error: None,
        }
    }

    /// The seat of an account: its own groups, or the console defaults.
    fn from_account(name: &str, o: &OperatorConfig, cfg: &DispatchConfig) -> Self {
        match &o.groups {
            Some(groups) => Seat::new(name, o.role, o.issi, groups, o.tx_group),
            None => Seat::new(name, o.role, o.issi, &cfg.groups, if o.tx_group != 0 { o.tx_group } else { cfg.tx_group }),
        }
    }

    /// Whether this seat has an ISSI to register on the network.
    fn registers(&self) -> bool {
        self.role.has_seat() && self.issi != 0
    }

    /// Talkgroups that should be affiliated: the listen list plus the TX group.
    fn wanted_groups(&self) -> BTreeSet<u32> {
        let mut g = self.listen.clone();
        if self.tx_group != 0 {
            g.insert(self.tx_group);
        }
        g
    }

    /// Whether a call on `gssi` is this seat's business.
    fn wants(&self, gssi: u32) -> bool {
        self.listen.contains(&gssi) || (self.tx_group != 0 && self.tx_group == gssi)
    }
}

struct Client {
    from: String,
    /// Account name, when accounts are configured.
    account: Option<String>,
}

pub struct Dispatcher {
    cfg: DispatchConfig,
    ui: broadcast::Sender<UiOut>,
    link: Option<mpsc::Sender<Vec<u8>>>,
    link_error: Option<String>,
    /// Accounts are configured: consoles log in, and each account has a seat.
    multi: bool,
    seats: Vec<Seat>,
    /// Received group calls, shared by every seat.
    rx: HashMap<Uuid, RxCall>,
    clients: HashMap<ClientId, Client>,
    log: VecDeque<Value>,
    sds: VecDeque<Value>,
    /// Last LIP position per ISSI.
    positions: BTreeMap<u32, Value>,
    /// Radios being polled for position.
    polls: HashMap<u32, Poll>,
    sds_ref: u8,
    /// SHORT_TRANSFER headers waiting for their SDS_TRANSFER frame.
    sds_pending: HashMap<Uuid, (u32, u32)>,
    /// Emergencies brew-server last reported (ISSI → called group), and when. brew-server
    /// re-sends the list every few seconds while any is active, so a list that goes quiet
    /// (link trouble) expires after `ALARMS_STALE`.
    alarms: BTreeMap<u32, Option<u32>>,
    alarms_at: Option<Instant>,
    /// brew-server's ISSI blacklist as last pushed, and whether this console may edit it.
    blacklist: BTreeSet<u32>,
    can_block: bool,
}

/// How long a list from brew-server stays valid without a refresh.
const ALARMS_STALE: Duration = Duration::from_secs(15);
/// `CLASS_SERVICE` type brew-server pushes: the active emergencies.
const SERVICE_EMERGENCY: u8 = 0x11;
/// `CLASS_SERVICE` type brew-server pushes: the ISSI blacklist (or a refused edit).
const SERVICE_BLACKLIST: u8 = 0x13;

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn epoch_ms_at(t: Instant) -> u64 {
    epoch_ms().saturating_sub(t.elapsed().as_millis() as u64)
}

/// Length-independent constant-time byte comparison for secret checks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn pcm_bytes(pcm: &[i16]) -> Vec<u8> {
    pcm.iter().flat_map(|s| s.to_le_bytes()).collect()
}

impl Dispatcher {
    pub fn new(cfg: DispatchConfig, ui: broadcast::Sender<UiOut>) -> Self {
        let multi = !cfg.operators.is_empty();
        let seats = if multi {
            cfg.operators.iter().map(|(name, o)| Seat::from_account(name, o, &cfg)).collect()
        } else {
            vec![Seat::new("", Role::Operator, cfg.operator_issi, &cfg.groups, cfg.tx_group)]
        };
        Self {
            cfg,
            ui,
            link: None,
            link_error: None,
            multi,
            seats,
            rx: HashMap::new(),
            clients: HashMap::new(),
            log: VecDeque::new(),
            sds: VecDeque::new(),
            positions: BTreeMap::new(),
            polls: HashMap::new(),
            sds_ref: 0,
            sds_pending: HashMap::new(),
            alarms: BTreeMap::new(),
            alarms_at: None,
            blacklist: BTreeSet::new(),
            can_block: false,
        }
    }

    pub async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        while let Some(ev) = events.recv().await {
            let push = !matches!(ev, Event::Tick | Event::UlPcm(..));
            let changed = self.handle(ev);
            if push || changed {
                self.push_status();
            }
        }
    }

    /// Returns true when a periodic event changed what consoles show.
    fn handle(&mut self, ev: Event) -> bool {
        match ev {
            Event::LinkUp(tx) => self.on_link_up(tx),
            Event::LinkDown(reason) => self.on_link_down(reason),
            Event::Brew(data) => match parse_brew_message(&data) {
                Ok(msg) => self.on_brew(msg),
                Err(e) => debug!("brew: unparsable frame: {e}"),
            },
            Event::ClientJoined(id, from) => self.on_client_joined(id, from, None),
            Event::ClientLoggedIn(id, from, account) => self.on_client_joined(id, from, Some(account)),
            Event::ClientLeft(id) => {
                self.clients.remove(&id);
                if let Some(si) = self.seat_owned_by(id) {
                    self.release_position(si);
                }
            }
            Event::Ui(id, cmd) => self.on_ui(id, cmd),
            Event::UlPcm(id, pcm) => {
                if let Some(si) = self.seat_owned_by(id) {
                    self.on_ul_pcm(si, &pcm);
                }
            }
            Event::RecordingAccess { file, action, from } => {
                self.note("recording", json!({"event": action, "file": file, "by": from}));
            }
            Event::OperatorSet { name, account, by } => self.set_operator(&name, &account, &by),
            Event::OperatorRemoved { name, by } => self.remove_operator(&name, &by),
            Event::Tick => return self.on_tick(),
        }
        true
    }

    fn on_client_joined(&mut self, id: ClientId, from: String, account: Option<String>) {
        self.clients.insert(id, Client { from, account });
        self.send_to(id, json!({"type": "history", "log": self.log, "sds": self.sds, "positions": self.positions.values().collect::<Vec<_>>()}));
    }

    // ── Seats ────────────────────────────────────────────────────────

    /// The seat a console is bound to: its account's, or the only one.
    fn seat_for_client(&self, id: ClientId) -> Option<usize> {
        let c = self.clients.get(&id)?;
        if self.multi {
            self.seat_by_name(c.account.as_deref()?)
        } else {
            (!self.seats.is_empty()).then_some(0)
        }
    }

    fn seat_owned_by(&self, id: ClientId) -> Option<usize> {
        self.seats.iter().position(|s| s.owner == Some(id))
    }

    fn seat_by_name(&self, name: &str) -> Option<usize> {
        self.seats.iter().position(|s| s.name == name)
    }

    fn seat_by_issi(&self, issi: u32) -> Option<usize> {
        self.seats.iter().position(|s| s.issi != 0 && s.issi == issi)
    }

    fn seat_with_private(&self, uuid: Uuid) -> Option<usize> {
        self.seats.iter().position(|s| s.private.as_ref().is_some_and(|p| p.uuid == uuid))
    }

    fn seat_with_tx(&self, uuid: Uuid) -> Option<usize> {
        self.seats.iter().position(|s| s.tx.as_ref().is_some_and(|t| t.uuid == uuid))
    }

    /// How a console is named in the log: its account, or its address.
    fn label(&self, id: ClientId) -> String {
        self.clients.get(&id).map(|c| c.account.clone().unwrap_or_else(|| c.from.clone())).unwrap_or_default()
    }

    /// The console's own ISSI (`[dispatch] operator_issi`). It is the single
    /// seat's identity without accounts; with them it stays registered as a
    /// shared identity (SDS to it reaches everyone) unless a seat owns it.
    fn console_issi(&self) -> u32 {
        self.cfg.operator_issi
    }

    /// Whether `issi` stays registered after a seat gives it up.
    fn issi_still_used(&self, issi: u32) -> bool {
        issi == self.console_issi() || self.seat_by_issi(issi).is_some()
    }

    /// Adds or updates an account's seat and registers it on the network.
    fn set_operator(&mut self, name: &str, o: &OperatorConfig, by: &str) {
        let event = match self.seat_by_name(name) {
            Some(si) => {
                let new_issi = if o.role.has_seat() { o.issi } else { 0 };
                let old_issi = self.seats[si].issi;
                if old_issi != new_issi {
                    self.release_position(si);
                    self.seats[si].issi = new_issi;
                    if old_issi != 0 && !self.issi_still_used(old_issi) {
                        let had: Vec<u32> = self.seats[si].affiliated.iter().copied().collect();
                        if !had.is_empty() {
                            self.send(build_subscriber_deaffiliate(old_issi, &had));
                        }
                        self.send(build_subscriber_deregister(old_issi));
                    }
                    self.seats[si].affiliated.clear();
                    if self.link.is_some() && self.seats[si].registers() {
                        self.send(build_subscriber_register(new_issi, &[]));
                    }
                }
                if let Some(groups) = &o.groups {
                    let s = &mut self.seats[si];
                    s.listen = groups.iter().copied().filter(|g| valid_ssi(*g)).collect();
                    s.tx_group = if o.tx_group != 0 { o.tx_group } else { s.listen.iter().next().copied().unwrap_or(0) };
                } else if o.tx_group != 0 {
                    self.seats[si].tx_group = o.tx_group;
                }
                self.seats[si].role = o.role;
                if !o.role.has_seat() {
                    self.release_position(si);
                }
                self.sync_affiliations(si);
                self.prune_rx();
                "updated"
            }
            None => {
                let seat = Seat::from_account(name, o, &self.cfg);
                self.seats.push(seat);
                let si = self.seats.len() - 1;
                if self.link.is_some() && self.seats[si].registers() {
                    self.send(build_subscriber_register(self.seats[si].issi, &[]));
                    self.sync_affiliations(si);
                }
                "added"
            }
        };
        info!(name, issi = o.issi, role = o.role.as_str(), by, "account {event}");
        self.note("account", json!({"event": event, "name": name, "issi": o.issi, "role": o.role.as_str(), "operator": by}));
    }

    /// Drops an account's seat, deregisters its ISSI and closes its consoles.
    fn remove_operator(&mut self, name: &str, by: &str) {
        let Some(si) = self.seat_by_name(name) else { return };
        self.release_position(si);
        let issi = self.seats[si].issi;
        let seat = self.seats.remove(si);
        if issi != 0 && self.link.is_some() && !self.issi_still_used(issi) {
            let had: Vec<u32> = seat.affiliated.iter().copied().collect();
            if !had.is_empty() {
                self.send(build_subscriber_deaffiliate(issi, &had));
            }
            self.send(build_subscriber_deregister(issi));
        }
        let gone: Vec<ClientId> = self.clients.iter().filter(|(_, c)| c.account.as_deref() == Some(name)).map(|(&id, _)| id).collect();
        for id in gone {
            self.clients.remove(&id);
            let _ = self.ui.send(UiOut::Close(id));
        }
        self.prune_rx();
        info!(name, issi, by, "account removed");
        self.note("account", json!({"event": "removed", "name": name, "issi": issi, "operator": by}));
    }

    // ── Brew link ────────────────────────────────────────────────────

    fn send(&self, frame: Vec<u8>) {
        if let Some(link) = &self.link {
            if link.try_send(frame).is_err() {
                warn!("brew: outbound queue full, frame dropped");
            }
        }
    }

    fn on_link_up(&mut self, tx: mpsc::Sender<Vec<u8>>) {
        self.link = Some(tx);
        self.link_error = None;
        let console = self.console_issi();
        if self.seat_by_issi(console).is_none() {
            self.send(build_subscriber_register(console, &[]));
        }
        for si in 0..self.seats.len() {
            self.seats[si].affiliated.clear();
            if self.seats[si].registers() {
                self.send(build_subscriber_register(self.seats[si].issi, &[]));
                self.sync_affiliations(si);
                info!(seat = %self.seats[si].name, issi = self.seats[si].issi, groups = ?self.seats[si].affiliated, "registered on brew-server");
            }
        }
        self.note("link", json!({"up": true}));
    }

    fn on_link_down(&mut self, reason: String) {
        let was_up = self.link.take().is_some();
        self.link_error = Some(reason.clone());
        self.rx.clear();
        for si in 0..self.seats.len() {
            let s = &mut self.seats[si];
            s.affiliated.clear();
            s.playing = None;
            s.tx = None;
            if let Some(p) = s.private.take() {
                self.end_private(si, &p, true, CAUSE_NOT_REACHABLE);
            }
        }
        if was_up {
            self.note("link", json!({"up": false, "reason": reason}));
        }
    }

    fn sync_affiliations(&mut self, si: usize) {
        if self.link.is_none() || !self.seats[si].registers() {
            return;
        }
        let s = &self.seats[si];
        let wanted = s.wanted_groups();
        let drop: Vec<u32> = s.affiliated.difference(&wanted).copied().collect();
        let add: Vec<u32> = wanted.difference(&s.affiliated).copied().collect();
        let issi = s.issi;
        if !drop.is_empty() {
            self.send(build_subscriber_deaffiliate(issi, &drop));
        }
        if !add.is_empty() {
            self.send(build_subscriber_affiliate(issi, &add));
        }
        self.seats[si].affiliated = wanted;
    }

    fn on_brew(&mut self, msg: BrewMessage) {
        match msg {
            BrewMessage::CallControl(cc) => self.on_call_control(cc),
            BrewMessage::Frame(f) => self.on_frame(f),
            BrewMessage::Error(e) => {
                warn!("brew-server error type={} data={:02x?}", e.error_type, e.data);
                self.error_all(format!("brew-server error {}", e.error_type));
            }
            BrewMessage::Subscriber(s) => debug!("brew: subscriber msg type={} issi={}", s.msg_type, s.number),
            BrewMessage::Service(s) if s.service_type == SERVICE_EMERGENCY => self.on_emergencies(&s.json_data),
            BrewMessage::Service(s) if s.service_type == SERVICE_BLACKLIST => self.on_blacklist(&s.json_data),
            BrewMessage::Service(s) => debug!("brew: service type={} {}", s.service_type, s.json_data),
        }
    }

    /// An error that is nobody's in particular: every seat sees it until its
    /// next successful action.
    fn error_all(&mut self, msg: String) {
        for s in &mut self.seats {
            s.last_error = Some(msg.clone());
        }
    }

    /// brew-server's list of active emergencies: `{"emergencies":[{"issi":N,"dest":G|null}]}`.
    fn on_emergencies(&mut self, json: &str) {
        let Ok(v) = serde_json::from_str::<Value>(json) else {
            debug!("brew: malformed emergency list");
            return;
        };
        let list: BTreeMap<u32, Option<u32>> = v["emergencies"].as_array().into_iter().flatten()
            .filter_map(|e| Some((u32::try_from(e["issi"].as_u64()?).ok()?, e["dest"].as_u64().and_then(|d| u32::try_from(d).ok()))))
            .collect();
        let was_active = !self.alarms.is_empty();
        for (issi, dest) in &list {
            if !self.alarms.contains_key(issi) {
                self.note("emergency", json!({"issi": issi, "gssi": dest}));
            }
        }
        let changed = list != self.alarms;
        self.alarms = list;
        self.alarms_at = (!self.alarms.is_empty()).then(Instant::now);
        if changed || was_active != !self.alarms.is_empty() {
            self.push_status();
        }
    }

    /// brew-server's blacklist: `{"blacklist":[N,...],"can_edit":bool}`, or
    /// `{"error":"..."}` when it refused an edit.
    fn on_blacklist(&mut self, json: &str) {
        let Ok(v) = serde_json::from_str::<Value>(json) else {
            debug!("brew: malformed blacklist message");
            return;
        };
        if let Some(e) = v["error"].as_str() {
            warn!("brew-server refused a blacklist edit: {e}");
            self.error_all(format!("brew-server: {e}"));
            self.push_status();
            return;
        }
        let list: BTreeSet<u32> = v["blacklist"].as_array().into_iter().flatten()
            .filter_map(|i| i.as_u64().and_then(|i| u32::try_from(i).ok())).collect();
        let can = v["can_edit"].as_bool().unwrap_or(false);
        if list != self.blacklist || can != self.can_block {
            self.blacklist = list;
            self.can_block = can;
            self.push_status();
        }
    }

    /// Ask brew-server to block or unblock an ISSI. It decides whether this
    /// console may (`[blacklist] console_users`) and answers with the new list.
    fn block_issi(&mut self, si: usize, issi: u32, block: bool) {
        if !valid_ssi(issi) {
            self.seats[si].last_error = Some("invalid ISSI".into());
            return;
        }
        if self.link.is_none() {
            self.seats[si].last_error = Some("not connected to brew-server".into());
            return;
        }
        if !self.can_block {
            self.seats[si].last_error = Some("this console may not edit the blacklist on this brew-server".into());
            return;
        }
        let json = json!({"action": if block { "block" } else { "unblock" }, "issi": issi}).to_string();
        self.send(build_service(BREW_SERVICE_BLACKLIST_CMD, &json));
        self.note_by(si, "block", json!({"issi": issi, "block": block}));
    }

    fn on_call_control(&mut self, cc: BrewCallControlMessage) {
        let uuid = cc.identifier;
        match (cc.call_state, cc.payload) {
            (CALL_STATE_GROUP_TX, BrewCallPayload::GroupTransmission(gt)) => self.on_group_tx(uuid, gt),
            (CALL_STATE_GROUP_IDLE, payload) => {
                let cause = if let BrewCallPayload::Cause(c) = payload { c } else { 0 };
                self.on_group_idle(uuid, cause);
            }
            (CALL_STATE_SETUP_REQUEST, BrewCallPayload::CircularCall(call)) => self.on_setup_request(uuid, call),
            (CALL_STATE_SETUP_ACCEPT, _) => {
                if let Some(p) = self.private_mut(uuid) {
                    debug!("private {}: setup accepted", p.peer);
                }
            }
            (CALL_STATE_CALL_ALERT, _) => {
                if let Some(p) = self.private_mut(uuid).filter(|p| p.phase == Phase::Dialing) {
                    p.phase = Phase::Alerting;
                }
            }
            (CALL_STATE_SETUP_REJECT, payload) | (CALL_STATE_CALL_RELEASE, payload) => {
                let cause = if let BrewCallPayload::Cause(c) = payload { c } else { 0 };
                if let Some(si) = self.seat_with_private(uuid) {
                    let p = self.seats[si].private.take().expect("seat has this call");
                    let failed = p.phase != Phase::Connected;
                    self.end_private(si, &p, failed, cause);
                }
            }
            (CALL_STATE_CONNECT_REQUEST, _) => {
                // The called radio answered our call.
                let Some(si) = self.seat_with_private(uuid) else { return };
                let Some(p) = self.seats[si].private.as_mut().filter(|p| !p.inbound) else { return };
                p.phase = Phase::Connected;
                p.connected_at = Some(Instant::now());
                let (peer, duplex) = (p.peer, p.duplex);
                self.send(build_connect_confirm(&uuid, 0, 0));
                self.seats[si].codec.reset();
                self.note_by(si, "private", json!({"peer": peer, "duplex": duplex, "dir": "out", "event": "connected"}));
            }
            (CALL_STATE_CONNECT_CONFIRM, _) => {
                let Some(p) = self.private_mut(uuid).filter(|p| p.inbound) else { return };
                if p.phase != Phase::Connected {
                    p.phase = Phase::Connected;
                    p.connected_at.get_or_insert_with(Instant::now);
                }
            }
            (CALL_STATE_SIMPLEX_GRANTED, _) => {
                if let Some(p) = self.private_mut(uuid) {
                    p.peer_talking = true;
                    p.talking = false;
                }
            }
            (CALL_STATE_SIMPLEX_IDLE, _) => {
                if let Some(p) = self.private_mut(uuid) {
                    p.peer_talking = false;
                }
            }
            (CALL_STATE_SHORT_TRANSFER, BrewCallPayload::ShortTransfer { source, destination }) => {
                self.sds_pending.insert(uuid, (source, destination));
            }
            (state, _) => debug!("brew: call state {state} uuid={uuid} ignored"),
        }
    }

    fn private_mut(&mut self, uuid: Uuid) -> Option<&mut Private> {
        self.seats.iter_mut().find_map(|s| s.private.as_mut().filter(|p| p.uuid == uuid))
    }

    fn on_group_tx(&mut self, uuid: Uuid, gt: BrewGroupTransmission) {
        if self.seat_with_tx(uuid).is_some() {
            return;
        }
        let emergency = gt.priority >= EMERGENCY_PRIORITY;
        if !emergency && !self.seats.iter().any(|s| s.affiliated.contains(&gt.destination)) {
            // brew-server may broadcast unaffiliated groups (fallback routing). An emergency
            // call is always taken: brew-server pushes it to every console.
            return;
        }
        let now = Instant::now();
        let fresh = !self.rx.contains_key(&uuid);
        let call = self.rx.entry(uuid).or_insert_with(|| RxCall { gssi: gt.destination, source: gt.source, started: now, last: now, emergency, codec: Codec::new(), local: false });
        call.emergency |= emergency;
        // A GROUP_TX on a running call is a talker change.
        let talker_changed = call.source != gt.source;
        call.source = gt.source;
        call.last = now;
        if fresh || talker_changed {
            self.note("group_rx", json!({"gssi": gt.destination, "source": gt.source, "emergency": emergency}));
        }
        self.offer_rx(uuid);
    }

    /// Puts a received call on the speaker of every seat it concerns and that
    /// is free. An emergency call takes the speaker over from an ordinary
    /// group call (not from a private call in progress, nor from another
    /// emergency call already playing).
    fn offer_rx(&mut self, uuid: Uuid) {
        let Some(c) = self.rx.get(&uuid) else { return };
        let (gssi, emergency) = (c.gssi, c.emergency);
        for s in &mut self.seats {
            if s.tx.as_ref().is_some_and(|t| t.uuid == uuid) || !(emergency || s.wants(gssi)) {
                continue;
            }
            let playing_emergency = s.playing.and_then(|u| self.rx.get(&u)).is_some_and(|c| c.emergency);
            let takes_over = emergency && s.private.is_none() && !playing_emergency && s.playing != Some(uuid);
            if (s.playing.is_none() && s.private.is_none()) || takes_over {
                s.playing = Some(uuid);
            }
        }
    }

    fn on_group_idle(&mut self, uuid: Uuid, cause: u8) {
        if let Some(si) = self.seat_with_tx(uuid) {
            // brew-server ended our transmission (pre-empted or refused).
            let t = self.seats[si].tx.take().expect("seat has this transmission");
            info!(gssi = t.gssi, cause, "group TX ended by brew-server");
            self.seats[si].last_error = Some(format!("transmission on {} ended by the network (cause {cause})", t.gssi));
            self.drop_rx(uuid);
            return;
        }
        self.drop_rx(uuid);
    }

    /// Forgets a received call and moves the seats that were playing it on.
    fn drop_rx(&mut self, uuid: Uuid) {
        if self.rx.remove(&uuid).is_none() {
            return;
        }
        for si in 0..self.seats.len() {
            if self.seats[si].playing == Some(uuid) {
                self.seats[si].playing = None;
                self.pick_next_rx(si);
            }
        }
    }

    /// Drops received calls no seat wants any more, and re-picks what each
    /// seat plays.
    fn prune_rx(&mut self) {
        let seats = &self.seats;
        self.rx.retain(|_, c| c.emergency || c.local || seats.iter().any(|s| s.wants(c.gssi)));
        for si in 0..self.seats.len() {
            let s = &self.seats[si];
            let keep = s.playing.is_some_and(|u| self.rx.get(&u).is_some_and(|c| c.emergency || s.wants(c.gssi)));
            if !keep {
                // Including a call already running on a group the seat just added.
                self.seats[si].playing = None;
                self.pick_next_rx(si);
            }
        }
    }

    fn pick_next_rx(&mut self, si: usize) {
        let s = &self.seats[si];
        if s.private.is_some() {
            return;
        }
        let own = s.tx.as_ref().map(|t| t.uuid);
        self.seats[si].playing = self.rx.iter()
            .filter(|(u, c)| Some(**u) != own && (c.emergency || s.wants(c.gssi)))
            .min_by_key(|(_, c)| c.started)
            .map(|(u, _)| *u);
    }

    fn on_frame(&mut self, f: BrewFrameMessage) {
        match f.frame_type {
            FRAME_TYPE_TRAFFIC_CHANNEL => self.on_voice(f.identifier, &f.data),
            FRAME_TYPE_SDS_TRANSFER => self.on_sds(f.identifier, f.length_bits, &f.data),
            FRAME_TYPE_SDS_REPORT => {
                if let Some(m) = self.sds.iter_mut().rev().find(|m| m["uuid"] == f.identifier.to_string()) {
                    m["delivered"] = json!(true);
                }
            }
            _ => {}
        }
    }

    fn on_voice(&mut self, uuid: Uuid, data: &[u8]) {
        if let Some(si) = self.seat_with_private(uuid) {
            let pcm = self.seats[si].private.as_mut().expect("seat has this call").codec.decode_ste(data);
            if let Some(pcm) = pcm {
                self.deliver(si, &pcm);
            }
            return;
        }
        let Some(c) = self.rx.get_mut(&uuid) else { return };
        c.last = Instant::now();
        if c.local {
            return;
        }
        let Some(pcm) = c.codec.decode_ste(data) else { return };
        for si in 0..self.seats.len() {
            let s = &self.seats[si];
            if s.playing == Some(uuid) && s.tx.is_none() {
                self.deliver(si, &pcm);
            }
        }
    }

    /// Plays audio on a seat: to the console that holds it, and to its recording.
    fn deliver(&mut self, si: usize, pcm: &[i16]) {
        let Some(owner) = self.seats[si].owner else { return };
        self.record(si, pcm);
        let _ = self.ui.send(UiOut::Pcm(owner, pcm_bytes(pcm)));
    }

    fn on_sds(&mut self, uuid: Uuid, length_bits: u16, data: &[u8]) {
        let Some((source, destination)) = self.sds_pending.remove(&uuid) else {
            debug!("brew: SDS frame {uuid} without header");
            return;
        };
        let len = (length_bits as usize).div_ceil(8).min(data.len());
        let data = &data[..len];
        self.send(build_sds_report(&uuid, 0));
        let seat = self.seat_by_issi(destination);
        // Answer from the identity the message came to, when it is ours.
        let ours = if seat.is_some() || destination == self.console_issi() { destination } else { self.console_issi() };
        if let Some(info) = sds::tl_info(data).filter(|i| i.report_requested) {
            self.send_sds_raw(ours, source, &sds::build_received_report(&info));
        }
        if let Some(p) = sds::decode_lip(data) {
            debug!("LIP from {source}: {:.5}, {:.5}", p.lat, p.lon);
            let pos = json!({
                "issi": source, "lat": p.lat, "lon": p.lon, "speed": p.speed, "heading": p.heading, "ts": epoch_ms(),
            });
            self.positions.insert(source, pos.clone());
            let _ = self.ui.send(UiOut::Text(json!({"type": "position", "pos": pos}).to_string()));
            return;
        }
        let Some(text) = sds::decode_text(data) else {
            debug!("SDS from {source} to {destination}: not text ({} bytes)", data.len());
            return;
        };
        info!("SDS from {source} to {destination}: {text}");
        let mut m = json!({
            "dir": "in", "from": source, "to": destination, "text": text,
            "uuid": uuid.to_string(), "ts": epoch_ms(),
        });
        if let Some(si) = seat.filter(|_| self.multi) {
            m["operator"] = json!(self.seats[si].name);
        }
        self.push_sds(m);
    }

    fn send_sds_raw(&mut self, from: u32, dest: u32, payload: &[u8]) -> Uuid {
        let uuid = Uuid::new_v4();
        self.send(build_short_transfer(&uuid, from, dest));
        self.send(build_sds_frame(&uuid, (payload.len() * 8) as u16, payload));
        uuid
    }

    // ── Private calls ────────────────────────────────────────────────

    fn on_setup_request(&mut self, uuid: Uuid, call: BrewCircularCall) {
        let Some(si) = self.seat_by_issi(call.destination) else {
            self.send(build_setup_reject(&uuid, CAUSE_NOT_REACHABLE));
            return;
        };
        let s = &self.seats[si];
        if s.private.is_some() || s.owner.is_none() {
            let cause = if s.private.is_some() { CAUSE_CALLED_PARTY_BUSY } else { CAUSE_NOT_REACHABLE };
            self.send(build_setup_reject(&uuid, cause));
            self.note_by(si, "private", json!({"peer": call.source, "dir": "in", "event": "rejected", "cause": cause}));
            return;
        }
        let duplex = call.duplex != 0;
        info!(peer = call.source, duplex, seat = %s.name, "incoming private call");
        let s = &mut self.seats[si];
        s.private = Some(Private::new(uuid, call.source, duplex, true, Phase::Ringing, Some(call), false));
        s.ended = None;
        s.playing = None;
        self.send(build_setup_accept(&uuid));
        self.send(build_call_alert(&uuid));
    }

    fn start_private(&mut self, si: usize, dest: u32, duplex: bool) {
        self.start_call(si, dest, duplex, false);
    }

    /// Start an ambience-listening (AL) call after checking the configured
    /// authorization password. Every attempt — allowed or denied — is recorded
    /// in the activity log.
    fn ambience_listen(&mut self, si: usize, id: ClientId, issi: u32, password: &str) {
        let required = &self.cfg.ambience_password;
        if !required.is_empty() && !constant_time_eq(password.as_bytes(), required.as_bytes()) {
            self.note_by(si, "ambience", json!({"issi": issi, "authorized": false}));
            self.seats[si].last_error = Some("ambient-listen authorization failed".into());
            self.send_to(id, json!({"type": "error", "error": "al_denied"}));
            warn!(issi, "ambient-listen denied: wrong authorization password");
            return;
        }
        self.note_by(si, "ambience", json!({"issi": issi, "authorized": true}));
        self.start_call(si, issi, false, true);
    }

    /// Set up an outgoing individual call. `ambience` requests a TETRA
    /// ambience-listening call: a simplex call where only the radio talks and
    /// the console never keys. The radio shows the call like any normal call;
    /// this is not a covert feature.
    fn start_call(&mut self, si: usize, dest: u32, duplex: bool, ambience: bool) {
        let issi = self.seats[si].issi;
        if !valid_ssi(dest) || dest == issi {
            self.seats[si].last_error = Some("invalid ISSI".into());
            return;
        }
        if self.seat_by_issi(dest).is_some() {
            self.seats[si].last_error = Some(format!("{dest} is another operator's ISSI"));
            return;
        }
        if self.link.is_none() {
            self.seats[si].last_error = Some("not connected to brew-server".into());
            return;
        }
        if let Some(p) = self.seats[si].private.take() {
            self.send(build_call_release(&p.uuid, CAUSE_USER_REQUESTED));
        }
        self.end_group_tx(si);
        let uuid = Uuid::new_v4();
        let call = BrewCircularCall {
            source: issi,
            destination: dest,
            number: String::new(),
            priority: self.cfg.priority,
            // Ambience listening is a distinct TETRA service; a normal call
            // otherwise. The exact service code depends on the brew-server and
            // radios and may need adjusting for a given fleet.
            service: if ambience { SERVICE_AMBIENCE_LISTENING } else { 0 },
            mode: 0,
            duplex: duplex as u8,
            // Hook signalling for a phone-style duplex call, direct for simplex.
            method: duplex as u8,
            communication: 0,
            grant: 0,
            permission: 0,
            timeout: 0,
            ownership: 0,
            queued: 0,
            mnemonic: None,
        };
        self.send(build_setup_request(&uuid, &call));
        let s = &mut self.seats[si];
        s.private = Some(Private::new(uuid, dest, duplex, false, Phase::Dialing, None, ambience));
        s.ended = None;
        s.playing = None;
        s.last_error = None;
        info!(dest, duplex, ambience, seat = %s.name, "calling");
    }

    fn answer(&mut self, si: usize) {
        let issi = self.seats[si].issi;
        let Some(p) = self.seats[si].private.as_mut().filter(|p| p.inbound && p.phase == Phase::Ringing) else { return };
        let mut call = p.call.clone().expect("incoming call keeps its setup");
        call.destination = issi;
        call.grant = 0;
        call.permission = 0;
        p.phase = Phase::Answering;
        p.connected_at = Some(Instant::now());
        let (uuid, peer, duplex) = (p.uuid, p.peer, p.duplex);
        self.send(build_connect_request(&uuid, &call));
        self.seats[si].codec.reset();
        self.note_by(si, "private", json!({"peer": peer, "duplex": duplex, "dir": "in", "event": "connected"}));
    }

    fn hangup(&mut self, si: usize) {
        if let Some(p) = self.seats[si].private.take() {
            let cause = CAUSE_USER_REQUESTED;
            if p.inbound && p.phase == Phase::Ringing {
                self.send(build_setup_reject(&p.uuid, CAUSE_CALLED_PARTY_BUSY));
            } else {
                self.send(build_call_release(&p.uuid, cause));
            }
            self.end_private(si, &p, false, cause);
        }
    }

    fn end_private(&mut self, si: usize, p: &Private, failed: bool, cause: u8) {
        self.seats[si].ended = Some(Ended { peer: p.peer, duplex: p.duplex, failed, cause, until: Instant::now() + TERMINAL_HOLD });
        let secs = p.connected_at.map(|t| t.elapsed().as_secs());
        self.note_by(si, "private", json!({
            "peer": p.peer, "duplex": p.duplex, "dir": if p.inbound { "in" } else { "out" },
            "event": if failed { "failed" } else { "ended" }, "cause": cause, "secs": secs,
        }));
        self.pick_next_rx(si);
    }

    // ── Console ──────────────────────────────────────────────────────

    fn on_ui(&mut self, id: ClientId, cmd: UiCmd) {
        let seat = self.seat_for_client(id);
        match cmd {
            UiCmd::Claim => {
                let Some(si) = seat.filter(|&si| self.seats[si].role.has_seat()) else {
                    self.send_to(id, json!({"type": "error", "error": if self.multi { "viewer" } else { "not_operator" }}));
                    return;
                };
                let by = self.label(id);
                let s = &mut self.seats[si];
                if s.owner.is_none() || s.owner == Some(id) {
                    s.owner = Some(id);
                    s.last_error = None;
                    info!(seat = %s.name, "operator position taken by {by}");
                } else {
                    self.send_to(id, json!({"type": "error", "error": "busy"}));
                }
                return;
            }
            UiCmd::Release => {
                if let Some(si) = seat.filter(|&si| self.seats[si].owner == Some(id)) {
                    self.release_position(si);
                }
                return;
            }
            _ => {}
        }
        let Some(si) = seat.filter(|&si| self.seats[si].owner == Some(id)) else {
            self.send_to(id, json!({"type": "error", "error": "not_operator"}));
            return;
        };
        match cmd {
            UiCmd::SetGroups { listen, tx } => {
                let s = &mut self.seats[si];
                s.listen = listen.into_iter().filter(|g| valid_ssi(*g)).take(64).collect();
                s.tx_group = if valid_ssi(tx) { tx } else { 0 };
                if s.tx.as_ref().is_some_and(|t| t.gssi != s.tx_group) {
                    self.end_group_tx(si);
                }
                self.sync_affiliations(si);
                self.prune_rx();
            }
            UiCmd::Ptt { down } => self.set_ptt(si, down),
            UiCmd::Call { issi, duplex } => self.start_private(si, issi, duplex),
            UiCmd::Answer => self.answer(si),
            UiCmd::Hangup => self.hangup(si),
            UiCmd::Sds { dest, text } => self.send_text(si, dest, text),
            UiCmd::AmbienceListen { issi, password } => self.ambience_listen(si, id, issi, &password),
            UiCmd::Block { issi, block } => self.block_issi(si, issi, block),
            UiCmd::LocationRequest { issi, period_s } => self.request_location(si, issi, period_s),
            UiCmd::RecordStart { name } => {
                let by = self.label(id);
                self.start_recording(si, &name, &by);
            }
            UiCmd::RecordStop => self.stop_recording(si),
            UiCmd::RecordDelete { file } => {
                let by = self.label(id);
                self.delete_recording(si, &file, &by);
            }
            UiCmd::LocationStop { issi } => {
                self.polls.remove(&issi);
                self.push_status();
            }
            UiCmd::Claim | UiCmd::Release => unreachable!(),
        }
    }

    fn start_recording(&mut self, si: usize, name: &str, by: &str) {
        if self.seats[si].recorder.is_some() {
            return;
        }
        let Some(stem) = recording::sanitize(name) else {
            self.seats[si].last_error = Some("invalid recording name".into());
            return;
        };
        match Recorder::start(&self.cfg.recordings_dir, &stem) {
            Ok(r) => {
                info!(file = r.file_name(), "recording started");
                self.note_by(si, "recording", json!({"event": "start", "file": r.file_name(), "by": by}));
                self.seats[si].recorder = Some((r, Instant::now()));
                self.seats[si].last_error = None;
            }
            Err(e) => {
                warn!("recording: {e}");
                self.seats[si].last_error = Some(format!("recording: {e}"));
            }
        }
    }

    fn stop_recording(&mut self, si: usize) {
        if let Some((r, _)) = self.seats[si].recorder.take() {
            match r.finish() {
                Ok(f) => {
                    info!(file = f, "recording saved");
                    self.note_by(si, "recording", json!({"event": "stop", "file": f}));
                    self.recordings_changed();
                }
                Err(e) => {
                    warn!("recording: {e}");
                    self.seats[si].last_error = Some(format!("recording: {e}"));
                }
            }
        }
    }

    fn recordings_changed(&self) {
        let _ = self.ui.send(UiOut::Text(json!({"type": "recordings_changed"}).to_string()));
    }

    fn delete_recording(&mut self, si: usize, file: &str, by: &str) {
        // A recording any seat is still writing cannot be deleted.
        let active = self.seats.iter().any(|s| s.recorder.as_ref().is_some_and(|(r, _)| r.file_name() == file));
        let Some(path) = recording::path_of(&self.cfg.recordings_dir, file).filter(|_| !active) else {
            self.seats[si].last_error = Some("cannot delete this recording".into());
            return;
        };
        match std::fs::remove_file(&path) {
            Ok(()) => {
                info!(file, "recording deleted");
                self.note_by(si, "recording", json!({"event": "delete", "file": file, "by": by}));
                self.recordings_changed();
            }
            Err(e) => {
                warn!("recording: {e}");
                self.seats[si].last_error = Some(format!("recording: {e}"));
            }
        }
    }

    fn record(&mut self, si: usize, pcm: &[i16]) {
        if let Some((r, _)) = self.seats[si].recorder.as_mut() {
            if let Err(e) = r.write(pcm) {
                warn!("recording: {e}");
                self.seats[si].last_error = Some(format!("recording: {e}"));
                self.stop_recording(si);
            }
        }
    }

    fn release_position(&mut self, si: usize) {
        self.stop_recording(si);
        self.set_ptt(si, false);
        self.hangup(si);
        self.seats[si].owner = None;
        self.seats[si].ended = None;
    }

    fn send_to(&self, id: ClientId, v: Value) {
        let _ = self.ui.send(UiOut::TextTo(id, v.to_string()));
    }

    fn set_ptt(&mut self, si: usize, down: bool) {
        let was_down = std::mem::replace(&mut self.seats[si].ptt_down, down);
        if let Some(p) = self.seats[si].private.as_mut() {
            // Ambience listening is receive-only: the console never keys.
            if p.duplex || p.ambience || p.phase != Phase::Connected {
                return;
            }
            if down && !p.talking {
                p.talking = true;
                p.peer_talking = false;
                let uuid = p.uuid;
                self.send(build_simplex_granted(&uuid, 0, 0));
            } else if !down && p.talking {
                p.talking = false;
                let uuid = p.uuid;
                self.flush_ul(si, uuid);
                self.send(build_simplex_idle(&uuid, 0, 0));
            }
            return;
        }
        if !down {
            self.end_group_tx(si);
            return;
        }
        if was_down || self.seats[si].tx.is_some() {
            return;
        }
        let gssi = self.seats[si].tx_group;
        if gssi == 0 {
            self.seats[si].last_error = Some("no talkgroup selected for transmit".into());
            return;
        }
        if self.link.is_none() {
            self.seats[si].last_error = Some("not connected to brew-server".into());
            return;
        }
        let busy = self.rx.values().any(|c| c.gssi == gssi);
        if busy && !self.seats[si].preempt_until.is_some_and(|t| Instant::now() < t) {
            self.seats[si].preempt_until = Some(Instant::now() + PREEMPT_WINDOW);
            self.seats[si].last_error = Some(format!("talkgroup {gssi} is busy — press PTT again to transmit over it"));
            return;
        }
        let issi = self.seats[si].issi;
        let uuid = Uuid::new_v4();
        self.send(build_group_tx(&uuid, issi, gssi, self.cfg.priority, 0, None));
        let s = &mut self.seats[si];
        s.preempt_until = None;
        s.tx = Some(TxCall { uuid, gssi });
        s.last_error = None;
        s.codec.reset();
        if self.seats.len() > 1 {
            // The other seats listening to the group hear this transmission
            // from here: brew-server does not echo it to this connection.
            let now = Instant::now();
            self.rx.insert(uuid, RxCall { gssi, source: issi, started: now, last: now, emergency: false, codec: Codec::new(), local: true });
            self.offer_rx(uuid);
        }
        self.note_by(si, "group_tx", json!({"gssi": gssi, "source": issi}));
    }

    fn end_group_tx(&mut self, si: usize) {
        if let Some(t) = self.seats[si].tx.take() {
            self.flush_ul(si, t.uuid);
            self.send(build_group_idle(&t.uuid, 0));
            self.drop_rx(t.uuid);
        }
    }

    fn flush_ul(&mut self, si: usize, uuid: Uuid) {
        for block in self.seats[si].codec.flush() {
            self.send(build_voice_frame(&uuid, (block.len() * 8) as u16, &block));
        }
    }

    fn on_ul_pcm(&mut self, si: usize, pcm: &[i16]) {
        let s = &self.seats[si];
        let (uuid, group) = match (&s.private, &s.tx) {
            (Some(p), _) if p.phase == Phase::Connected || p.phase == Phase::Answering => {
                if !(p.duplex || p.talking) {
                    return;
                }
                (p.uuid, false)
            }
            (Some(_), _) => return,
            (None, Some(t)) => (t.uuid, true),
            (None, None) => return,
        };
        self.record(si, pcm);
        for block in self.seats[si].codec.encode_pcm(pcm) {
            self.send(build_voice_frame(&uuid, (block.len() * 8) as u16, &block));
        }
        if group {
            self.relay_local(si, uuid, pcm);
        }
    }

    /// Hands a seat's group transmission to the other seats playing it.
    fn relay_local(&mut self, from: usize, uuid: Uuid, pcm: &[i16]) {
        let Some(c) = self.rx.get_mut(&uuid) else { return };
        c.last = Instant::now();
        for si in 0..self.seats.len() {
            let s = &self.seats[si];
            if si != from && s.playing == Some(uuid) && s.tx.is_none() {
                self.deliver(si, pcm);
            }
        }
    }

    /// Ask a radio for a position report. `period_s` 0 sends one request;
    /// otherwise the console re-sends every `period_s` seconds until stopped.
    fn request_location(&mut self, si: usize, issi: u32, period_s: u32) {
        if !valid_ssi(issi) {
            self.seats[si].last_error = Some("invalid ISSI".into());
            return;
        }
        if self.link.is_none() {
            self.seats[si].last_error = Some("not connected to brew-server".into());
            return;
        }
        let from = self.seats[si].issi;
        self.send_sds_raw(from, issi, &sds::encode_location_request());
        let period_s = period_s.clamp(0, 86_400);
        if period_s == 0 {
            self.polls.remove(&issi);
        } else {
            let period = Duration::from_secs(period_s as u64);
            self.polls.insert(issi, Poll { period, next: Instant::now() + period, from });
        }
        self.note_by(si, "location_req", json!({"issi": issi, "period_s": period_s}));
        self.push_status();
    }

    fn send_text(&mut self, si: usize, dest: u32, text: String) {
        let text = text.trim().to_string();
        if !valid_ssi(dest) || text.is_empty() {
            self.seats[si].last_error = Some("SDS needs a destination and text".into());
            return;
        }
        if self.link.is_none() {
            self.seats[si].last_error = Some("not connected to brew-server".into());
            return;
        }
        self.sds_ref = self.sds_ref.wrapping_add(1);
        let payload = sds::encode_text(&text, self.sds_ref);
        let from = self.seats[si].issi;
        let uuid = self.send_sds_raw(from, dest, &payload);
        let mut m = json!({
            "dir": "out", "from": from, "to": dest, "text": text,
            "uuid": uuid.to_string(), "ts": epoch_ms(), "delivered": false,
        });
        if self.multi {
            m["operator"] = json!(self.seats[si].name);
        }
        self.push_sds(m);
    }

    fn push_sds(&mut self, m: Value) {
        if self.sds.len() >= SDS_CAP {
            self.sds.pop_front();
        }
        self.sds.push_back(m.clone());
        let _ = self.ui.send(UiOut::Text(json!({"type": "sds", "msg": m}).to_string()));
    }

    fn note(&mut self, kind: &str, mut v: Value) {
        v["kind"] = json!(kind);
        v["ts"] = json!(epoch_ms());
        if self.log.len() >= LOG_CAP {
            self.log.pop_front();
        }
        self.log.push_back(v.clone());
        let _ = self.ui.send(UiOut::Text(json!({"type": "log", "entry": v}).to_string()));
    }

    /// An activity log entry caused by a seat: names its operator, with accounts.
    fn note_by(&mut self, si: usize, kind: &str, mut v: Value) {
        if self.multi {
            v["operator"] = json!(self.seats[si].name);
        }
        self.note(kind, v);
    }

    fn on_tick(&mut self) -> bool {
        let mut changed = false;
        let now = Instant::now();
        let stale: Vec<Uuid> = self.rx.iter().filter(|(_, c)| c.last.elapsed() > RX_STALE).map(|(u, _)| *u).collect();
        for uuid in stale {
            self.on_group_idle(uuid, 0);
            changed = true;
        }
        for si in 0..self.seats.len() {
            let s = &mut self.seats[si];
            if s.ended.as_ref().is_some_and(|e| now >= e.until) {
                s.ended = None;
                changed = true;
            }
            if s.preempt_until.is_some_and(|t| now >= t) {
                s.preempt_until = None;
                changed = true;
            }
            if s.private.as_ref().is_some_and(|p| !p.inbound && p.phase != Phase::Connected && p.started.elapsed() > SETUP_TIMEOUT) {
                let p = s.private.take().expect("checked above");
                self.send(build_call_release(&p.uuid, CAUSE_NOT_REACHABLE));
                self.end_private(si, &p, true, CAUSE_NOT_REACHABLE);
                changed = true;
            }
        }
        if self.alarms_at.is_some_and(|t| t.elapsed() > ALARMS_STALE) {
            self.alarms.clear();
            self.alarms_at = None;
            changed = true;
        }
        // Re-send location requests that have come due.
        let due: Vec<(u32, u32)> = self.polls.iter().filter(|(_, p)| now >= p.next).map(|(&i, p)| (i, p.from)).collect();
        for (issi, from) in due {
            if self.link.is_some() {
                // From the seat that asked, if it still exists; else the console's own ISSI.
                let from = if self.seat_by_issi(from).is_some() { from } else { self.console_issi() };
                self.send_sds_raw(from, issi, &sds::encode_location_request());
            }
            if let Some(p) = self.polls.get_mut(&issi) {
                p.next = now + p.period;
            }
        }
        changed
    }

    /// What every console sees: link, calls on the air, emergencies, blacklist,
    /// polls and (with accounts) the operators.
    fn shared_status(&self) -> Value {
        let active: Vec<Value> = self.rx.values().map(|c| json!({"gssi": c.gssi, "source": c.source, "emergency": c.emergency})).collect();
        // Every emergency the console knows: calls it hears at priority 15 and the list
        // brew-server pushes (alarms, and calls on groups relayed from other servers).
        let mut emergencies: BTreeMap<u32, Option<u32>> = self.alarms.clone();
        for c in self.rx.values().filter(|c| c.emergency) {
            emergencies.insert(c.source, Some(c.gssi));
        }
        let emergencies: Vec<Value> = emergencies.iter().map(|(&issi, &gssi)| json!({"issi": issi, "gssi": gssi})).collect();
        let mut s = json!({
            "type": "status",
            "connected": self.link.is_some(),
            "link_error": self.link_error,
            "active": active,
            "emergencies": emergencies,
            "blacklist": self.blacklist,
            "can_block": self.can_block,
            "polls": self.polls.iter().map(|(&i, p)| json!({"issi": i, "period_s": p.period.as_secs()})).collect::<Vec<_>>(),
            "ambience_auth": !self.cfg.ambience_password.is_empty(),
        });
        if self.multi {
            s["operators"] = json!(self.seats.iter().map(|seat| json!({
                "name": seat.name,
                "issi": (seat.issi != 0).then_some(seat.issi),
                "role": seat.role.as_str(),
                "online": self.clients.values().any(|c| c.account.as_deref() == Some(seat.name.as_str())),
                "seat_held_by": seat.owner.and_then(|id| self.clients.get(&id)).map(|c| &c.from),
            })).collect::<Vec<_>>());
        }
        s
    }

    /// What a seat's consoles see of it; `None` for a console with no seat.
    fn seat_status(&self, seat: Option<&Seat>) -> Value {
        let Some(s) = seat else {
            return json!({
                "issi": null, "listen": [], "tx_group": 0, "transmitting": null, "preempt_offer": false,
                "rx": null, "recording": null, "private": null, "operator": null, "last_error": null,
            });
        };
        let rx = s.playing.and_then(|u| self.rx.get(&u)).map(|c| json!({"gssi": c.gssi, "source": c.source}));
        let private = if let Some(p) = &s.private {
            json!({
                "peer": p.peer, "duplex": p.duplex, "inbound": p.inbound, "phase": p.phase.name(),
                "talking": p.talking, "peer_talking": p.peer_talking, "ambience": p.ambience,
                "connected_ms": p.connected_at.map(epoch_ms_at),
            })
        } else if let Some(e) = &s.ended {
            json!({
                "peer": e.peer, "duplex": e.duplex, "inbound": false,
                "phase": if e.failed { "failed" } else { "ended" }, "cause": e.cause,
            })
        } else {
            Value::Null
        };
        json!({
            "issi": (s.issi != 0).then_some(s.issi),
            "listen": s.listen,
            "tx_group": s.tx_group,
            "transmitting": s.tx.as_ref().map(|t| t.gssi),
            "preempt_offer": s.preempt_until.is_some(),
            "rx": rx,
            "recording": s.recorder.as_ref().map(|(r, at)| json!({"file": r.file_name(), "started_ms": epoch_ms_at(*at)})),
            "private": private,
            "operator": s.owner.map(|id| self.label(id)),
            "last_error": s.last_error,
        })
    }

    fn merge(into: &mut Value, from: Value) {
        if let (Value::Object(a), Value::Object(b)) = (into, from) {
            a.extend(b);
        }
    }

    /// The status as the first seat's consoles see it (the only seat, without accounts).
    #[cfg(test)]
    fn status(&self) -> Value {
        let mut s = self.shared_status();
        Self::merge(&mut s, self.seat_status(self.seats.first()));
        s
    }

    fn push_status(&self) {
        let shared = self.shared_status();
        // Each console sees its own seat and learns whether it is the operator.
        for (&id, c) in &self.clients {
            let si = self.seat_for_client(id);
            let seat = si.map(|i| &self.seats[i]);
            let mut s = shared.clone();
            Self::merge(&mut s, self.seat_status(seat));
            let holds = seat.is_some_and(|s| s.owner == Some(id));
            s["you_operate"] = json!(holds);
            if self.multi {
                s["me"] = json!({
                    "name": c.account,
                    "role": seat.map(|s| s.role.as_str()),
                    "issi": seat.and_then(|s| (s.issi != 0).then_some(s.issi)),
                    "holds_seat": holds,
                });
            }
            let _ = self.ui.send(UiOut::TextTo(id, s.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OP: u32 = 9_990_001;

    fn setup() -> (Dispatcher, broadcast::Receiver<UiOut>, mpsc::Receiver<Vec<u8>>) {
        setup_with(DispatchConfig { operator_issi: OP, groups: vec![91, 92], tx_group: 91, priority: 0, ambience_password: String::new(), ..Default::default() })
    }

    fn setup_with(cfg: DispatchConfig) -> (Dispatcher, broadcast::Receiver<UiOut>, mpsc::Receiver<Vec<u8>>) {
        let (ui, ui_rx) = broadcast::channel(256);
        let mut d = Dispatcher::new(cfg, ui);
        let (tx, rx) = mpsc::channel(64);
        d.handle(Event::LinkUp(tx));
        d.handle(Event::ClientJoined(1, "a".into()));
        d.handle(Event::Ui(1, UiCmd::Claim));
        (d, ui_rx, rx)
    }

    fn drain(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<BrewMessage> {
        let mut out = Vec::new();
        while let Ok(f) = rx.try_recv() {
            out.push(parse_brew_message(&f).unwrap());
        }
        out
    }

    fn call_states(msgs: &[BrewMessage]) -> Vec<u8> {
        msgs.iter()
            .filter_map(|m| if let BrewMessage::CallControl(c) = m { Some(c.call_state) } else { None })
            .collect()
    }

    #[test]
    fn registers_and_affiliates_on_link_up() {
        let (_d, _ui, mut rx) = setup();
        let msgs = drain(&mut rx);
        let subs: Vec<(u8, Vec<u32>)> = msgs.iter()
            .filter_map(|m| if let BrewMessage::Subscriber(s) = m { Some((s.msg_type, s.groups.clone())) } else { None })
            .collect();
        assert_eq!(subs, vec![(BREW_SUBSCRIBER_REGISTER, vec![]), (BREW_SUBSCRIBER_AFFILIATE, vec![91, 92])]);
    }

    #[test]
    fn changing_groups_affiliates_the_difference() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::SetGroups { listen: vec![92, 93], tx: 93 }));
        let subs: Vec<(u8, Vec<u32>)> = drain(&mut rx).iter()
            .filter_map(|m| if let BrewMessage::Subscriber(s) = m { Some((s.msg_type, s.groups.clone())) } else { None })
            .collect();
        assert_eq!(subs, vec![(BREW_SUBSCRIBER_DEAFFILIATE, vec![91]), (BREW_SUBSCRIBER_AFFILIATE, vec![93])]);
    }

    #[test]
    fn ptt_sends_group_tx_voice_and_idle() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::Ptt { down: true }));
        d.handle(Event::UlPcm(1, vec![0; 480 * 2 + 10]));
        d.handle(Event::Ui(1, UiCmd::Ptt { down: false }));
        let msgs = drain(&mut rx);
        assert_eq!(call_states(&msgs), vec![CALL_STATE_GROUP_TX, CALL_STATE_GROUP_IDLE]);
        let voice = msgs.iter().filter(|m| matches!(m, BrewMessage::Frame(f) if f.frame_type == FRAME_TYPE_TRAFFIC_CHANNEL && f.data.len() == 36)).count();
        assert_eq!(voice, 3, "two full blocks and the padded tail");
    }

    #[test]
    fn busy_group_needs_a_second_ptt() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        let uuid = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&uuid, 1234, 91, 0, 0, None)));
        d.handle(Event::Ui(1, UiCmd::Ptt { down: true }));
        d.handle(Event::Ui(1, UiCmd::Ptt { down: false }));
        assert!(call_states(&drain(&mut rx)).is_empty());
        d.handle(Event::Ui(1, UiCmd::Ptt { down: true }));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_GROUP_TX]);
    }

    #[test]
    fn emergency_group_call_is_taken_even_when_not_listened_to_and_takes_the_speaker() {
        let (mut d, mut ui, _rx) = setup();
        // Group 99 is not listened to: an ordinary call there is ignored ...
        let normal = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&normal, 1234, 99, 0, 0, None)));
        assert!(d.status()["active"].as_array().unwrap().is_empty());
        // ... but an emergency call there is taken, flagged, and played.
        let em = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&em, 4013, 99, EMERGENCY_PRIORITY, 0, None)));
        let st = d.status();
        assert_eq!(st["active"][0]["emergency"], true);
        assert_eq!(st["rx"]["gssi"], 99);
        d.handle(Event::Brew(build_voice_frame(&em, 288, &[0u8; 36])));
        assert!(std::iter::from_fn(|| ui.try_recv().ok()).any(|m| matches!(m, UiOut::Pcm(1, _))));
        // It takes the speaker over from an ordinary call already playing.
        d.handle(Event::Brew(build_group_idle(&em, 0)));
        let ordinary = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&ordinary, 1234, 92, 0, 0, None)));
        assert_eq!(d.status()["rx"]["gssi"], 92);
        let em2 = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&em2, 4013, 91, EMERGENCY_PRIORITY, 0, None)));
        assert_eq!(d.status()["rx"]["gssi"], 91);
        d.handle(Event::Brew(build_group_idle(&em2, 0)));
        assert!(d.status()["active"].as_array().unwrap().iter().all(|a| a["emergency"] == false));
    }

    fn service(t: u8, json: &str) -> Vec<u8> {
        let mut m = vec![0xf4, t];
        m.extend_from_slice(json.as_bytes());
        m.push(0);
        m
    }

    fn raw(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<Vec<u8>> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    #[test]
    fn the_operator_can_block_an_issi_when_brew_server_allows_it() {
        let (mut d, _ui, mut rx) = setup();
        d.handle(Event::Brew(service(0x13, r#"{"blacklist":[7],"can_edit":false}"#)));
        assert_eq!(d.status()["blacklist"], json!([7]));
        assert_eq!(d.status()["can_block"], false);
        // Not allowed here: nothing is sent, the operator is told.
        d.handle(Event::Ui(1, UiCmd::Block { issi: 4013, block: true }));
        assert!(raw(&mut rx).iter().all(|m| m.get(..2) != Some(&[0xf4, 0x12][..])));
        assert!(d.status()["last_error"].as_str().unwrap().contains("may not edit"));

        d.handle(Event::Brew(service(0x13, r#"{"blacklist":[7],"can_edit":true}"#)));
        assert_eq!(d.status()["can_block"], true);
        d.handle(Event::Ui(1, UiCmd::Block { issi: 4013, block: true }));
        let sent: Vec<Vec<u8>> = raw(&mut rx).into_iter().filter(|m| m.get(..2) == Some(&[0xf4, 0x12][..])).collect();
        assert_eq!(sent.len(), 1);
        assert_eq!(&sent[0][2..sent[0].len() - 1], br#"{"action":"block","issi":4013}"#);
        d.handle(Event::Ui(1, UiCmd::Block { issi: 4013, block: false }));
        let sent: Vec<Vec<u8>> = raw(&mut rx).into_iter().filter(|m| m.get(..2) == Some(&[0xf4, 0x12][..])).collect();
        assert_eq!(&sent[0][2..sent[0].len() - 1], br#"{"action":"unblock","issi":4013}"#);
        // brew-server's refusal is shown.
        d.handle(Event::Brew(service(0x13, r#"{"error":"not authorised to edit the blacklist"}"#)));
        assert!(d.status()["last_error"].as_str().unwrap().contains("not authorised"));
        // Only the operator can send it.
        d.handle(Event::Ui(2, UiCmd::Block { issi: 5, block: true }));
        assert!(raw(&mut rx).iter().all(|m| m.get(..2) != Some(&[0xf4, 0x12][..])));
    }

    #[test]
    fn brew_servers_emergency_list_is_shown_and_expires() {
        let (mut d, _ui, _rx) = setup();
        let msg = |json: &str| {
            let mut m = vec![0xf4, 0x11];
            m.extend_from_slice(json.as_bytes());
            m.push(0);
            m
        };
        d.handle(Event::Brew(msg(r#"{"emergencies":[{"issi":4013,"dest":91},{"issi":4020,"dest":null}]}"#)));
        let e = d.status()["emergencies"].clone();
        assert_eq!(e, json!([{"issi": 4013, "gssi": 91}, {"issi": 4020, "gssi": null}]));
        // A call heard at priority 15 is listed too, merged by ISSI.
        let em = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&em, 4020, 99, EMERGENCY_PRIORITY, 0, None)));
        assert_eq!(d.status()["emergencies"][1], json!({"issi": 4020, "gssi": 99}));
        // An empty list clears the alarms; stale ones expire on their own.
        d.handle(Event::Brew(msg(r#"{"emergencies":[]}"#)));
        assert_eq!(d.status()["emergencies"], json!([{"issi": 4020, "gssi": 99}]), "only the live call remains");
        d.handle(Event::Brew(msg(r#"{"emergencies":[{"issi":4013,"dest":null}]}"#)));
        d.alarms_at = Some(Instant::now() - ALARMS_STALE - Duration::from_secs(1));
        d.on_tick();
        assert!(d.alarms.is_empty());
        d.handle(Event::Brew(msg("not json")));
    }

    #[test]
    fn received_group_voice_reaches_the_operator() {
        let (mut d, mut ui, _rx) = setup();
        let uuid = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&uuid, 1234, 92, 0, 0, None)));
        d.handle(Event::Brew(build_voice_frame(&uuid, 288, &[0u8; 36])));
        let mut got = false;
        while let Ok(m) = ui.try_recv() {
            if let UiOut::Pcm(1, b) = m {
                assert_eq!(b.len(), 960);
                got = true;
            }
        }
        assert!(got);
        assert_eq!(d.status()["rx"]["source"], 1234);
        d.handle(Event::Brew(build_group_idle(&uuid, 0)));
        assert!(d.status()["rx"].is_null());
    }

    #[test]
    fn outgoing_private_call_flow() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::Call { issi: 2001, duplex: true }));
        let msgs = drain(&mut rx);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!() };
        assert_eq!(cc.call_state, CALL_STATE_SETUP_REQUEST);
        let uuid = cc.identifier;
        let BrewCallPayload::CircularCall(call) = &cc.payload else { panic!() };
        assert_eq!((call.source, call.destination, call.duplex), (OP, 2001, 1));

        d.handle(Event::Brew(build_call_alert(&uuid)));
        assert_eq!(d.status()["private"]["phase"], "alerting");
        let mut answer = call.clone();
        answer.source = 2001;
        d.handle(Event::Brew(build_connect_request(&uuid, &answer)));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_CONNECT_CONFIRM]);
        assert_eq!(d.status()["private"]["phase"], "connected");

        d.handle(Event::UlPcm(1, vec![0; 480]));
        assert_eq!(drain(&mut rx).len(), 1, "duplex sends voice without PTT");

        d.handle(Event::Brew(build_call_release(&uuid, 1)));
        assert_eq!(d.status()["private"]["phase"], "ended");
    }

    #[test]
    fn incoming_private_call_is_answered() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        let uuid = Uuid::new_v4();
        let call = BrewCircularCall {
            source: 2002, destination: OP, number: String::new(), priority: 0, service: 0, mode: 0,
            duplex: 0, method: 0, communication: 0, grant: 0, permission: 0, timeout: 0, ownership: 0,
            queued: 0, mnemonic: None,
        };
        d.handle(Event::Brew(build_setup_request(&uuid, &call)));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_ACCEPT, CALL_STATE_CALL_ALERT]);
        assert_eq!(d.status()["private"]["phase"], "ringing");
        d.handle(Event::Ui(1, UiCmd::Answer));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_CONNECT_REQUEST]);
        d.handle(Event::Brew(build_connect_confirm(&uuid, 0, 0)));
        assert_eq!(d.status()["private"]["phase"], "connected");

        // Simplex: voice only while the console holds the floor.
        d.handle(Event::UlPcm(1, vec![0; 480]));
        assert!(drain(&mut rx).is_empty());
        d.handle(Event::Ui(1, UiCmd::Ptt { down: true }));
        d.handle(Event::UlPcm(1, vec![0; 480]));
        d.handle(Event::Ui(1, UiCmd::Ptt { down: false }));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SIMPLEX_GRANTED, CALL_STATE_SIMPLEX_IDLE]);

        d.handle(Event::Ui(1, UiCmd::Hangup));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_CALL_RELEASE]);
    }

    #[test]
    fn incoming_call_without_operator_is_rejected() {
        let (mut d, _ui, mut rx) = setup();
        d.handle(Event::Ui(1, UiCmd::Release));
        drain(&mut rx);
        let uuid = Uuid::new_v4();
        let call = BrewCircularCall {
            source: 2002, destination: OP, number: String::new(), priority: 0, service: 0, mode: 0,
            duplex: 1, method: 1, communication: 0, grant: 0, permission: 0, timeout: 0, ownership: 0,
            queued: 0, mnemonic: None,
        };
        d.handle(Event::Brew(build_setup_request(&uuid, &call)));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_REJECT]);
    }

    #[test]
    fn sds_in_and_out() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::Sds { dest: 2003, text: "hola".into() }));
        let msgs = drain(&mut rx);
        assert_eq!(call_states(&msgs), vec![CALL_STATE_SHORT_TRANSFER]);
        let BrewMessage::Frame(f) = &msgs[1] else { panic!() };
        assert_eq!(sds::decode_text(&f.data).as_deref(), Some("hola"));

        let uuid = Uuid::new_v4();
        let data = sds::encode_text("recibido", 4);
        d.handle(Event::Brew(build_short_transfer(&uuid, 2003, OP)));
        d.handle(Event::Brew(build_sds_frame(&uuid, (data.len() * 8) as u16, &data)));
        assert_eq!(d.sds.back().unwrap()["text"], "recibido");
        assert!(drain(&mut rx).iter().any(|m| matches!(m, BrewMessage::Frame(f) if f.frame_type == FRAME_TYPE_SDS_REPORT)));
    }

    #[test]
    fn non_operator_cannot_command() {
        let (mut d, mut ui, mut rx) = setup();
        d.handle(Event::ClientJoined(2, "b".into()));
        drain(&mut rx);
        d.handle(Event::Ui(2, UiCmd::Ptt { down: true }));
        assert!(drain(&mut rx).is_empty());
        let mut refused = false;
        while let Ok(m) = ui.try_recv() {
            if let UiOut::TextTo(2, t) = m {
                refused |= t.contains("not_operator");
            }
        }
        assert!(refused);
    }

    fn setup_al(password: &str) -> (Dispatcher, broadcast::Receiver<UiOut>, mpsc::Receiver<Vec<u8>>) {
        setup_with(DispatchConfig {
            operator_issi: OP, groups: vec![91, 92], tx_group: 91,
            ambience_password: password.to_string(),
            ..Default::default()
        })
    }

    fn last_log(d: &Dispatcher) -> (Option<&str>, Option<bool>) {
        let e = d.log.back().unwrap();
        (e["kind"].as_str(), e["authorized"].as_bool())
    }

    #[test]
    fn ambience_without_password_starts_and_logs() {
        let (mut d, _ui, mut rx) = setup();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::AmbienceListen { issi: 2001, password: String::new() }));
        let msgs = drain(&mut rx);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!("no setup request") };
        assert_eq!(cc.call_state, CALL_STATE_SETUP_REQUEST);
        let BrewCallPayload::CircularCall(call) = &cc.payload else { panic!() };
        assert_eq!(call.service, SERVICE_AMBIENCE_LISTENING);
        assert_eq!(last_log(&d), (Some("ambience"), Some(true)));
        assert!(d.status()["ambience_auth"] == false);
    }

    #[test]
    fn ambience_wrong_password_is_denied_and_logged() {
        let (mut d, _ui, mut rx) = setup_al("secret");
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::AmbienceListen { issi: 2001, password: "wrong".into() }));
        assert!(call_states(&drain(&mut rx)).is_empty(), "no call on a failed authorization");
        assert!(d.seats[0].private.is_none());
        assert_eq!(last_log(&d), (Some("ambience"), Some(false)));
        assert!(d.status()["ambience_auth"] == true);
    }

    #[test]
    fn ambience_right_password_starts() {
        let (mut d, _ui, mut rx) = setup_al("secret");
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::AmbienceListen { issi: 2001, password: "secret".into() }));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_REQUEST]);
        assert!(d.seats[0].private.as_ref().is_some_and(|p| p.ambience));
        assert_eq!(last_log(&d), (Some("ambience"), Some(true)));
    }

    // ── Several operators ────────────────────────────────────────────

    const A: u32 = 9_990_011;
    const B: u32 = 9_990_012;

    fn account(issi: u32, role: Role, groups: Option<Vec<u32>>, tx_group: u32) -> OperatorConfig {
        OperatorConfig { password: "pw".into(), issi, role, groups, tx_group }
    }

    /// Two seats (ann on 91+92 transmitting on 91, bob on 92+93 transmitting on
    /// 93) and a viewer. Console 1 is ann, 2 is bob, 3 is the viewer; ann and
    /// bob have taken their seats.
    fn setup_multi() -> (Dispatcher, broadcast::Receiver<UiOut>, mpsc::Receiver<Vec<u8>>) {
        let mut operators = BTreeMap::new();
        operators.insert("ann".to_string(), account(A, Role::Admin, Some(vec![91, 92]), 91));
        operators.insert("bob".to_string(), account(B, Role::Operator, Some(vec![92, 93]), 93));
        operators.insert("vic".to_string(), account(0, Role::Viewer, None, 0));
        let cfg = DispatchConfig { operator_issi: OP, groups: vec![91], tx_group: 91, operators, ..Default::default() };
        let (ui, ui_rx) = broadcast::channel(256);
        let mut d = Dispatcher::new(cfg, ui);
        let (tx, rx) = mpsc::channel(64);
        d.handle(Event::LinkUp(tx));
        d.handle(Event::ClientLoggedIn(1, "a".into(), "ann".into()));
        d.handle(Event::ClientLoggedIn(2, "b".into(), "bob".into()));
        d.handle(Event::ClientLoggedIn(3, "v".into(), "vic".into()));
        d.handle(Event::Ui(1, UiCmd::Claim));
        d.handle(Event::Ui(2, UiCmd::Claim));
        (d, ui_rx, rx)
    }

    fn subscriber_msgs(msgs: &[BrewMessage]) -> Vec<(u8, u32, Vec<u32>)> {
        msgs.iter()
            .filter_map(|m| if let BrewMessage::Subscriber(s) = m { Some((s.msg_type, s.number, s.groups.clone())) } else { None })
            .collect()
    }

    /// Everything the consoles were sent (skipping over a lag).
    fn ui_out(ui: &mut broadcast::Receiver<UiOut>) -> Vec<UiOut> {
        let mut out = Vec::new();
        loop {
            match ui.try_recv() {
                Ok(m) => out.push(m),
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
        out
    }

    /// Texts sent to one console, parsed.
    fn texts_to(ui: &mut broadcast::Receiver<UiOut>, id: ClientId) -> Vec<Value> {
        ui_out(ui).into_iter()
            .filter_map(|m| match m {
                UiOut::TextTo(to, t) if to == id => serde_json::from_str(&t).ok(),
                _ => None,
            })
            .collect()
    }

    /// The last status one console was sent.
    fn status_of(out: &[UiOut], id: ClientId) -> Value {
        out.iter().rev()
            .find_map(|m| match m {
                UiOut::TextTo(to, t) if *to == id => serde_json::from_str::<Value>(t).ok().filter(|v| v["type"] == "status"),
                _ => None,
            })
            .expect("a status was sent")
    }

    /// Who was sent audio.
    fn pcm_to(ui: &mut broadcast::Receiver<UiOut>) -> Vec<ClientId> {
        ui_out(ui).into_iter()
            .filter_map(|m| if let UiOut::Pcm(to, _) = m { Some(to) } else { None })
            .collect()
    }

    #[test]
    fn every_seat_registers_and_affiliates_its_own_groups_on_link_up() {
        let (_d, _ui, mut rx) = setup_multi();
        let subs = subscriber_msgs(&drain(&mut rx));
        assert_eq!(subs, vec![
            // The console's own ISSI stays registered as a shared identity ...
            (BREW_SUBSCRIBER_REGISTER, OP, vec![]),
            // ... and every seat registers under its own ISSI with its own groups (the viewer has none).
            (BREW_SUBSCRIBER_REGISTER, A, vec![]),
            (BREW_SUBSCRIBER_AFFILIATE, A, vec![91, 92]),
            (BREW_SUBSCRIBER_REGISTER, B, vec![]),
            (BREW_SUBSCRIBER_AFFILIATE, B, vec![92, 93]),
        ]);
    }

    #[test]
    fn a_group_call_is_played_only_to_the_seats_listening_to_it() {
        let (mut d, mut ui, _rx) = setup_multi();
        let _ = pcm_to(&mut ui);
        // Group 91: ann only.
        let g91 = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&g91, 1234, 91, 0, 0, None)));
        d.handle(Event::Brew(build_voice_frame(&g91, 288, &[0u8; 36])));
        assert_eq!(pcm_to(&mut ui), vec![1]);
        // Group 93 at the same time: bob only, each seat plays its own call.
        let g93 = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&g93, 1235, 93, 0, 0, None)));
        d.handle(Event::Brew(build_voice_frame(&g93, 288, &[0u8; 36])));
        assert_eq!(pcm_to(&mut ui), vec![2]);
        d.push_status();
        let out = ui_out(&mut ui);
        let ann = status_of(&out, 1);
        assert_eq!(ann["rx"]["gssi"], 91);
        assert_eq!(ann["me"]["name"], "ann");
        assert_eq!(ann["me"]["issi"], A);
        assert_eq!(ann["you_operate"], true);
        assert_eq!(ann["operators"].as_array().unwrap().len(), 3);
        let bob = status_of(&out, 2);
        assert_eq!(bob["rx"]["gssi"], 93);
        assert_eq!(bob["issi"], B);
        // Group 92 is shared: once 91 and 93 end, both move on to it.
        let g92 = Uuid::new_v4();
        d.handle(Event::Brew(build_group_tx(&g92, 1236, 92, 0, 0, None)));
        d.handle(Event::Brew(build_group_idle(&g91, 0)));
        d.handle(Event::Brew(build_group_idle(&g93, 0)));
        let _ = pcm_to(&mut ui);
        d.handle(Event::Brew(build_voice_frame(&g92, 288, &[0u8; 36])));
        let mut heard = pcm_to(&mut ui);
        heard.sort_unstable();
        assert_eq!(heard, vec![1, 2]);
        // The viewer never gets audio and has no seat.
        d.push_status();
        let vic = status_of(&ui_out(&mut ui), 3);
        assert!(vic["issi"].is_null());
        assert_eq!(vic["me"]["role"], "viewer");
        assert_eq!(vic["you_operate"], false);
    }

    fn setup_call(dest: u32) -> BrewCircularCall {
        BrewCircularCall {
            source: 2002, destination: dest, number: String::new(), priority: 0, service: 0, mode: 0,
            duplex: 1, method: 1, communication: 0, grant: 0, permission: 0, timeout: 0, ownership: 0,
            queued: 0, mnemonic: None,
        }
    }

    #[test]
    fn a_private_call_reaches_the_seat_owning_the_issi() {
        let (mut d, _ui, mut rx) = setup_multi();
        drain(&mut rx);
        let uuid = Uuid::new_v4();
        d.handle(Event::Brew(build_setup_request(&uuid, &setup_call(B))));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_ACCEPT, CALL_STATE_CALL_ALERT]);
        assert!(d.seats[0].private.is_none(), "ann's seat is untouched");
        assert_eq!(d.seats[1].private.as_ref().map(|p| p.peer), Some(2002));
        // Ann cannot answer bob's call; bob can, from his own ISSI.
        d.handle(Event::Ui(1, UiCmd::Answer));
        assert!(drain(&mut rx).is_empty());
        d.handle(Event::Ui(2, UiCmd::Answer));
        let msgs = drain(&mut rx);
        assert_eq!(call_states(&msgs), vec![CALL_STATE_CONNECT_REQUEST]);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!() };
        let BrewCallPayload::CircularCall(call) = &cc.payload else { panic!() };
        assert_eq!(call.destination, B);
        assert_eq!(d.log.back().unwrap()["operator"], "bob");
        d.handle(Event::Ui(2, UiCmd::Hangup));
        drain(&mut rx);
        // A call to a seat nobody holds is rejected; so is one to an unknown ISSI.
        d.handle(Event::Ui(2, UiCmd::Release));
        let uuid = Uuid::new_v4();
        d.handle(Event::Brew(build_setup_request(&uuid, &setup_call(B))));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_REJECT]);
        assert_eq!(d.log.back().unwrap()["event"], "rejected");
        d.handle(Event::Brew(build_setup_request(&Uuid::new_v4(), &setup_call(7777))));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_REJECT]);
    }

    #[test]
    fn ptt_from_a_seat_transmits_with_its_own_issi_and_the_other_seats_hear_it() {
        let (mut d, mut ui, mut rx) = setup_multi();
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::Ptt { down: true }));
        let msgs = drain(&mut rx);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!() };
        assert_eq!(cc.call_state, CALL_STATE_GROUP_TX);
        let BrewCallPayload::GroupTransmission(gt) = &cc.payload else { panic!() };
        assert_eq!((gt.source, gt.destination), (A, 91));
        assert_eq!(d.log.back().unwrap()["operator"], "ann");
        // Bob transmits on 93 at the same time, as himself.
        d.handle(Event::Ui(2, UiCmd::Ptt { down: true }));
        let msgs = drain(&mut rx);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!() };
        let BrewCallPayload::GroupTransmission(gt) = &cc.payload else { panic!() };
        assert_eq!((gt.source, gt.destination), (B, 93));
        d.handle(Event::Ui(2, UiCmd::Ptt { down: false }));
        drain(&mut rx);
        let _ = pcm_to(&mut ui);
        // Bob listens to 92, not 91: he does not hear ann. Once he does listen, he hears
        // her microphone relayed here (brew-server does not echo our own transmission).
        d.handle(Event::UlPcm(1, vec![0; 480]));
        assert!(pcm_to(&mut ui).is_empty());
        d.handle(Event::Ui(2, UiCmd::SetGroups { listen: vec![91, 93], tx: 93 }));
        d.handle(Event::UlPcm(1, vec![0; 480]));
        assert_eq!(pcm_to(&mut ui), vec![2]);
        // Bob now finds 91 busy and needs a second press to transmit over ann.
        d.handle(Event::Ui(2, UiCmd::SetGroups { listen: vec![91, 93], tx: 91 }));
        drain(&mut rx);
        d.handle(Event::Ui(2, UiCmd::Ptt { down: true }));
        d.handle(Event::Ui(2, UiCmd::Ptt { down: false }));
        assert!(call_states(&drain(&mut rx)).is_empty());
        d.handle(Event::Ui(2, UiCmd::Ptt { down: true }));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_GROUP_TX]);
        d.handle(Event::Ui(2, UiCmd::Ptt { down: false }));
        d.handle(Event::Ui(1, UiCmd::Ptt { down: false }));
        assert!(d.rx.is_empty(), "local transmissions are forgotten when they end");
    }

    #[test]
    fn sds_from_a_seat_uses_its_issi_and_incoming_sds_names_the_seat() {
        let (mut d, _ui, mut rx) = setup_multi();
        drain(&mut rx);
        d.handle(Event::Ui(2, UiCmd::Sds { dest: 2003, text: "hi".into() }));
        let msgs = drain(&mut rx);
        let BrewMessage::CallControl(cc) = &msgs[0] else { panic!() };
        let BrewCallPayload::ShortTransfer { source, destination } = cc.payload else { panic!() };
        assert_eq!((source, destination), (B, 2003));
        assert_eq!(d.sds.back().unwrap()["operator"], "bob");
        // Addressed to ann's ISSI: shown as hers. Addressed to the console's: shared.
        for (dest, who) in [(A, json!("ann")), (OP, Value::Null)] {
            let uuid = Uuid::new_v4();
            let data = sds::encode_text("ok", 4);
            d.handle(Event::Brew(build_short_transfer(&uuid, 2003, dest)));
            d.handle(Event::Brew(build_sds_frame(&uuid, (data.len() * 8) as u16, &data)));
            let m = d.sds.back().unwrap();
            assert_eq!(m["to"], dest);
            assert_eq!(m["operator"], who);
        }
    }

    #[test]
    fn a_viewer_cannot_take_a_seat_or_command() {
        let (mut d, mut ui, mut rx) = setup_multi();
        drain(&mut rx);
        let _ = texts_to(&mut ui, 3);
        d.handle(Event::Ui(3, UiCmd::Claim));
        assert!(texts_to(&mut ui, 3).iter().any(|m| m["error"] == "viewer"));
        d.handle(Event::Ui(3, UiCmd::Ptt { down: true }));
        d.handle(Event::Ui(3, UiCmd::Sds { dest: 2003, text: "x".into() }));
        assert!(drain(&mut rx).is_empty());
        assert!(texts_to(&mut ui, 3).iter().any(|m| m["error"] == "not_operator"));
        // A second console of the same account watches the seat and cannot command it.
        d.handle(Event::ClientLoggedIn(4, "a2".into(), "ann".into()));
        d.handle(Event::Ui(4, UiCmd::Claim));
        assert!(texts_to(&mut ui, 4).iter().any(|m| m["error"] == "busy"));
        d.handle(Event::Ui(4, UiCmd::Ptt { down: true }));
        assert!(drain(&mut rx).is_empty());
        // When ann's console goes, the seat is free and the other console may take it.
        d.handle(Event::ClientLeft(1));
        d.handle(Event::Ui(4, UiCmd::Claim));
        assert_eq!(d.seats[0].owner, Some(4));
    }

    #[test]
    fn accounts_change_live() {
        let (mut d, mut ui, mut rx) = setup_multi();
        drain(&mut rx);
        // A new operator registers and affiliates at once.
        d.handle(Event::OperatorSet { name: "cat".into(), account: account(9_990_013, Role::Operator, Some(vec![95]), 0), by: "ann".into() });
        assert_eq!(subscriber_msgs(&drain(&mut rx)), vec![(BREW_SUBSCRIBER_REGISTER, 9_990_013, vec![]), (BREW_SUBSCRIBER_AFFILIATE, 9_990_013, vec![95])]);
        assert_eq!(d.log.back().unwrap()["operator"], "ann");
        // Changing bob's ISSI moves his registration.
        d.handle(Event::OperatorSet { name: "bob".into(), account: account(9_990_022, Role::Operator, None, 0), by: "ann".into() });
        assert_eq!(subscriber_msgs(&drain(&mut rx)), vec![
            (BREW_SUBSCRIBER_DEAFFILIATE, B, vec![92, 93]),
            (BREW_SUBSCRIBER_DEREGISTER, B, vec![]),
            (BREW_SUBSCRIBER_REGISTER, 9_990_022, vec![]),
            (BREW_SUBSCRIBER_AFFILIATE, 9_990_022, vec![92, 93]),
        ]);
        assert_eq!(d.seats[1].owner, None, "bob lost the seat with his old ISSI");
        // Removing cat deregisters her and closes her consoles.
        d.handle(Event::ClientLoggedIn(5, "c".into(), "cat".into()));
        d.handle(Event::OperatorRemoved { name: "cat".into(), by: "ann".into() });
        assert_eq!(subscriber_msgs(&drain(&mut rx)), vec![(BREW_SUBSCRIBER_DEAFFILIATE, 9_990_013, vec![95]), (BREW_SUBSCRIBER_DEREGISTER, 9_990_013, vec![])]);
        assert!(ui_out(&mut ui).iter().any(|m| matches!(m, UiOut::Close(5))));
        assert!(d.seat_by_name("cat").is_none());
        assert!(!d.clients.contains_key(&5));
    }
}
