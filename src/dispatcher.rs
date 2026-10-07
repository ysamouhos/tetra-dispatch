//! Dispatcher state machine: one task owns the Brew session state (registration,
//! affiliations, group and private calls, SDS) and the console audio.
//!
//! Inputs arrive as [`Event`]s from the Brew link and the web consoles;
//! outputs go to the brew-server as raw Brew frames and to the consoles as
//! [`UiOut`] broadcasts. One console at a time holds the operator position
//! ("claim"); the others watch.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::codec::Codec;
use crate::config::{valid_ssi, DispatchConfig};
use crate::protocol::*;
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
    ClientJoined(ClientId, String),
    ClientLeft(ClientId),
    Ui(ClientId, UiCmd),
    /// Microphone PCM16 @ 8 kHz from a console.
    UlPcm(ClientId, Vec<i16>),
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
}

#[derive(Debug, Clone)]
pub enum UiOut {
    /// JSON text for every console.
    Text(String),
    /// JSON text for one console.
    TextTo(ClientId, String),
    /// Received audio (PCM16LE @ 8 kHz) for the operator console.
    Pcm(ClientId, Vec<u8>),
}

struct RxCall {
    gssi: u32,
    source: u32,
    started: Instant,
    last: Instant,
    /// Emergency call (priority 15). brew-server pushes these to every console,
    /// whatever groups it listens to.
    emergency: bool,
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
}

/// What the call panel shows for a few seconds after a private call ends.
struct Ended {
    peer: u32,
    duplex: bool,
    failed: bool,
    cause: u8,
    until: Instant,
}

pub struct Dispatcher {
    cfg: DispatchConfig,
    ui: broadcast::Sender<UiOut>,
    link: Option<mpsc::Sender<Vec<u8>>>,
    link_error: Option<String>,
    issi: u32,
    listen: BTreeSet<u32>,
    tx_group: u32,
    /// Groups currently affiliated on the brew-server.
    affiliated: BTreeSet<u32>,
    rx: HashMap<Uuid, RxCall>,
    /// The received group call being played on the console.
    playing: Option<Uuid>,
    tx: Option<TxCall>,
    preempt_until: Option<Instant>,
    private: Option<Private>,
    ended: Option<Ended>,
    codec: Codec,
    clients: HashMap<ClientId, String>,
    owner: Option<ClientId>,
    ptt_down: bool,
    last_error: Option<String>,
    log: VecDeque<Value>,
    sds: VecDeque<Value>,
    /// Last LIP position per ISSI.
    positions: BTreeMap<u32, Value>,
    /// Radios being polled for position: ISSI → (period, next due).
    polls: HashMap<u32, (Duration, Instant)>,
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

impl Dispatcher {
    pub fn new(cfg: DispatchConfig, ui: broadcast::Sender<UiOut>) -> Self {
        let listen: BTreeSet<u32> = cfg.groups.iter().copied().collect();
        let tx_group = if cfg.tx_group != 0 { cfg.tx_group } else { listen.iter().next().copied().unwrap_or(0) };
        Self {
            issi: cfg.operator_issi,
            cfg,
            ui,
            link: None,
            link_error: None,
            listen,
            tx_group,
            affiliated: BTreeSet::new(),
            rx: HashMap::new(),
            playing: None,
            tx: None,
            preempt_until: None,
            private: None,
            ended: None,
            codec: Codec::new(),
            clients: HashMap::new(),
            owner: None,
            ptt_down: false,
            last_error: None,
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
            Event::ClientJoined(id, label) => {
                self.clients.insert(id, label);
                self.send_to(id, json!({"type": "history", "log": self.log, "sds": self.sds, "positions": self.positions.values().collect::<Vec<_>>()}));
            }
            Event::ClientLeft(id) => {
                self.clients.remove(&id);
                if self.owner == Some(id) {
                    self.release_position();
                }
            }
            Event::Ui(id, cmd) => self.on_ui(id, cmd),
            Event::UlPcm(id, pcm) => {
                if self.owner == Some(id) {
                    self.on_ul_pcm(&pcm);
                }
            }
            Event::Tick => return self.on_tick(),
        }
        true
    }

    // ── Brew link ────────────────────────────────────────────────────

    fn send(&mut self, frame: Vec<u8>) {
        if let Some(link) = &self.link {
            if link.try_send(frame).is_err() {
                warn!("brew: outbound queue full, frame dropped");
            }
        }
    }

    fn on_link_up(&mut self, tx: mpsc::Sender<Vec<u8>>) {
        self.link = Some(tx);
        self.link_error = None;
        self.affiliated.clear();
        self.send(build_subscriber_register(self.issi, &[]));
        self.sync_affiliations();
        self.note("link", json!({"up": true}));
        info!(issi = self.issi, groups = ?self.affiliated, "registered on brew-server");
    }

    fn on_link_down(&mut self, reason: String) {
        let was_up = self.link.take().is_some();
        self.link_error = Some(reason.clone());
        self.affiliated.clear();
        self.rx.clear();
        self.playing = None;
        self.tx = None;
        if let Some(p) = self.private.take() {
            self.end_private(&p, true, CAUSE_NOT_REACHABLE);
        }
        if was_up {
            self.note("link", json!({"up": false, "reason": reason}));
        }
    }

    /// Talkgroups that should be affiliated: the listen list plus the TX group.
    fn wanted_groups(&self) -> BTreeSet<u32> {
        let mut g = self.listen.clone();
        if self.tx_group != 0 {
            g.insert(self.tx_group);
        }
        g
    }

    fn sync_affiliations(&mut self) {
        if self.link.is_none() {
            return;
        }
        let wanted = self.wanted_groups();
        let drop: Vec<u32> = self.affiliated.difference(&wanted).copied().collect();
        let add: Vec<u32> = wanted.difference(&self.affiliated).copied().collect();
        if !drop.is_empty() {
            self.send(build_subscriber_deaffiliate(self.issi, &drop));
        }
        if !add.is_empty() {
            self.send(build_subscriber_affiliate(self.issi, &add));
        }
        self.affiliated = wanted;
    }

    fn on_brew(&mut self, msg: BrewMessage) {
        match msg {
            BrewMessage::CallControl(cc) => self.on_call_control(cc),
            BrewMessage::Frame(f) => self.on_frame(f),
            BrewMessage::Error(e) => {
                warn!("brew-server error type={} data={:02x?}", e.error_type, e.data);
                self.last_error = Some(format!("brew-server error {}", e.error_type));
            }
            BrewMessage::Subscriber(s) => debug!("brew: subscriber msg type={} issi={}", s.msg_type, s.number),
            BrewMessage::Service(s) if s.service_type == SERVICE_EMERGENCY => self.on_emergencies(&s.json_data),
            BrewMessage::Service(s) if s.service_type == SERVICE_BLACKLIST => self.on_blacklist(&s.json_data),
            BrewMessage::Service(s) => debug!("brew: service type={} {}", s.service_type, s.json_data),
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
            self.last_error = Some(format!("brew-server: {e}"));
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
    fn block_issi(&mut self, issi: u32, block: bool) {
        if !valid_ssi(issi) {
            self.last_error = Some("invalid ISSI".into());
            return;
        }
        if self.link.is_none() {
            self.last_error = Some("not connected to brew-server".into());
            return;
        }
        if !self.can_block {
            self.last_error = Some("this console may not edit the blacklist on this brew-server".into());
            return;
        }
        let json = json!({"action": if block { "block" } else { "unblock" }, "issi": issi}).to_string();
        self.send(build_service(BREW_SERVICE_BLACKLIST_CMD, &json));
        self.note("block", json!({"issi": issi, "block": block}));
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
                if let Some(p) = self.private.take_if(|p| p.uuid == uuid) {
                    let failed = p.phase != Phase::Connected;
                    self.end_private(&p, failed, cause);
                }
            }
            (CALL_STATE_CONNECT_REQUEST, _) => {
                // The called radio answered our call.
                let Some(p) = self.private_mut(uuid).filter(|p| !p.inbound) else { return };
                p.phase = Phase::Connected;
                p.connected_at = Some(Instant::now());
                let (peer, duplex) = (p.peer, p.duplex);
                self.send(build_connect_confirm(&uuid, 0, 0));
                self.codec.reset();
                self.note("private", json!({"peer": peer, "duplex": duplex, "dir": "out", "event": "connected"}));
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
        self.private.as_mut().filter(|p| p.uuid == uuid)
    }

    fn on_group_tx(&mut self, uuid: Uuid, gt: BrewGroupTransmission) {
        if self.tx.as_ref().is_some_and(|t| t.uuid == uuid) {
            return;
        }
        let emergency = gt.priority >= EMERGENCY_PRIORITY;
        if !self.affiliated.contains(&gt.destination) && !emergency {
            // brew-server may broadcast unaffiliated groups (fallback routing). An emergency
            // call is always taken: brew-server pushes it to every console.
            return;
        }
        let now = Instant::now();
        let fresh = !self.rx.contains_key(&uuid);
        let call = self.rx.entry(uuid).or_insert(RxCall { gssi: gt.destination, source: gt.source, started: now, last: now, emergency });
        call.emergency |= emergency;
        // A GROUP_TX on a running call is a talker change.
        let talker_changed = call.source != gt.source;
        call.source = gt.source;
        call.last = now;
        if fresh || talker_changed {
            self.note("group_rx", json!({"gssi": gt.destination, "source": gt.source, "emergency": emergency}));
        }
        // An emergency call takes the speaker over from an ordinary group call (not from a
        // private call in progress, nor from another emergency call already playing).
        let playing_emergency = self.playing.and_then(|u| self.rx.get(&u)).is_some_and(|c| c.emergency);
        let takes_over = emergency && self.private.is_none() && !playing_emergency && self.playing != Some(uuid);
        if (self.playing.is_none() && self.private.is_none()) || takes_over {
            self.playing = Some(uuid);
            self.codec.reset();
        }
    }

    fn on_group_idle(&mut self, uuid: Uuid, cause: u8) {
        if let Some(t) = self.tx.take_if(|t| t.uuid == uuid) {
            // brew-server ended our transmission (pre-empted or refused).
            info!(gssi = t.gssi, cause, "group TX ended by brew-server");
            self.last_error = Some(format!("transmission on {} ended by the network (cause {cause})", t.gssi));
            return;
        }
        if self.rx.remove(&uuid).is_some() && self.playing == Some(uuid) {
            self.playing = None;
            self.pick_next_rx();
        }
    }

    fn pick_next_rx(&mut self) {
        if self.private.is_some() {
            return;
        }
        self.playing = self.rx.iter().min_by_key(|(_, c)| c.started).map(|(u, _)| *u);
        if self.playing.is_some() {
            self.codec.reset();
        }
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
        let audible = if self.private.as_ref().is_some_and(|p| p.uuid == uuid) {
            true
        } else if let Some(c) = self.rx.get_mut(&uuid) {
            c.last = Instant::now();
            self.playing == Some(uuid) && self.tx.is_none()
        } else {
            false
        };
        let Some(owner) = self.owner.filter(|_| audible) else { return };
        if let Some(pcm) = self.codec.decode_ste(data) {
            let bytes = pcm.iter().flat_map(|s| s.to_le_bytes()).collect();
            let _ = self.ui.send(UiOut::Pcm(owner, bytes));
        }
    }

    fn on_sds(&mut self, uuid: Uuid, length_bits: u16, data: &[u8]) {
        let Some((source, destination)) = self.sds_pending.remove(&uuid) else {
            debug!("brew: SDS frame {uuid} without header");
            return;
        };
        let len = (length_bits as usize).div_ceil(8).min(data.len());
        let data = &data[..len];
        self.send(build_sds_report(&uuid, 0));
        if let Some(info) = sds::tl_info(data).filter(|i| i.report_requested) {
            self.send_sds_raw(source, &sds::build_received_report(&info));
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
        self.push_sds(json!({
            "dir": "in", "from": source, "to": destination, "text": text,
            "uuid": uuid.to_string(), "ts": epoch_ms(),
        }));
    }

    fn send_sds_raw(&mut self, dest: u32, payload: &[u8]) -> Uuid {
        let uuid = Uuid::new_v4();
        self.send(build_short_transfer(&uuid, self.issi, dest));
        self.send(build_sds_frame(&uuid, (payload.len() * 8) as u16, payload));
        uuid
    }

    // ── Private calls ────────────────────────────────────────────────

    fn on_setup_request(&mut self, uuid: Uuid, call: BrewCircularCall) {
        if call.destination != self.issi {
            self.send(build_setup_reject(&uuid, CAUSE_NOT_REACHABLE));
            return;
        }
        if self.private.is_some() || self.owner.is_none() {
            let cause = if self.private.is_some() { CAUSE_CALLED_PARTY_BUSY } else { CAUSE_NOT_REACHABLE };
            self.send(build_setup_reject(&uuid, cause));
            self.note("private", json!({"peer": call.source, "dir": "in", "event": "rejected", "cause": cause}));
            return;
        }
        let duplex = call.duplex != 0;
        info!(peer = call.source, duplex, "incoming private call");
        self.private = Some(Private {
            uuid,
            peer: call.source,
            duplex,
            inbound: true,
            phase: Phase::Ringing,
            call: Some(call),
            started: Instant::now(),
            connected_at: None,
            talking: false,
            peer_talking: false,
            ambience: false,
        });
        self.ended = None;
        self.playing = None;
        self.send(build_setup_accept(&uuid));
        self.send(build_call_alert(&uuid));
    }

    fn start_private(&mut self, dest: u32, duplex: bool) {
        self.start_call(dest, duplex, false);
    }

    /// Start an ambience-listening (AL) call after checking the configured
    /// authorization password. Every attempt — allowed or denied — is recorded
    /// in the activity log.
    fn ambience_listen(&mut self, id: ClientId, issi: u32, password: &str) {
        let required = &self.cfg.ambience_password;
        if !required.is_empty() && !constant_time_eq(password.as_bytes(), required.as_bytes()) {
            self.note("ambience", json!({"issi": issi, "authorized": false}));
            self.last_error = Some("ambient-listen authorization failed".into());
            self.send_to(id, json!({"type": "error", "error": "al_denied"}));
            warn!(issi, "ambient-listen denied: wrong authorization password");
            return;
        }
        self.note("ambience", json!({"issi": issi, "authorized": true}));
        self.start_call(issi, false, true);
    }

    /// Set up an outgoing individual call. `ambience` requests a TETRA
    /// ambience-listening call: a simplex call where only the radio talks and
    /// the console never keys. The radio shows the call like any normal call;
    /// this is not a covert feature.
    fn start_call(&mut self, dest: u32, duplex: bool, ambience: bool) {
        if !valid_ssi(dest) || dest == self.issi {
            self.last_error = Some("invalid ISSI".into());
            return;
        }
        if self.link.is_none() {
            self.last_error = Some("not connected to brew-server".into());
            return;
        }
        if let Some(p) = self.private.take() {
            self.send(build_call_release(&p.uuid, CAUSE_USER_REQUESTED));
        }
        self.end_group_tx();
        let uuid = Uuid::new_v4();
        let call = BrewCircularCall {
            source: self.issi,
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
        self.private = Some(Private {
            uuid,
            peer: dest,
            duplex,
            inbound: false,
            phase: Phase::Dialing,
            call: None,
            started: Instant::now(),
            connected_at: None,
            talking: false,
            peer_talking: false,
            ambience,
        });
        self.ended = None;
        self.playing = None;
        self.last_error = None;
        info!(dest, duplex, ambience, "calling");
    }

    fn answer(&mut self) {
        let issi = self.issi;
        let Some(p) = self.private.as_mut().filter(|p| p.inbound && p.phase == Phase::Ringing) else { return };
        let mut call = p.call.clone().expect("incoming call keeps its setup");
        call.destination = issi;
        call.grant = 0;
        call.permission = 0;
        p.phase = Phase::Answering;
        p.connected_at = Some(Instant::now());
        let (uuid, peer, duplex) = (p.uuid, p.peer, p.duplex);
        self.send(build_connect_request(&uuid, &call));
        self.codec.reset();
        self.note("private", json!({"peer": peer, "duplex": duplex, "dir": "in", "event": "connected"}));
    }

    fn hangup(&mut self) {
        if let Some(p) = self.private.take() {
            let cause = CAUSE_USER_REQUESTED;
            if p.inbound && p.phase == Phase::Ringing {
                self.send(build_setup_reject(&p.uuid, CAUSE_CALLED_PARTY_BUSY));
            } else {
                self.send(build_call_release(&p.uuid, cause));
            }
            self.end_private(&p, false, cause);
        }
    }

    fn end_private(&mut self, p: &Private, failed: bool, cause: u8) {
        self.ended = Some(Ended { peer: p.peer, duplex: p.duplex, failed, cause, until: Instant::now() + TERMINAL_HOLD });
        let secs = p.connected_at.map(|t| t.elapsed().as_secs());
        self.note("private", json!({
            "peer": p.peer, "duplex": p.duplex, "dir": if p.inbound { "in" } else { "out" },
            "event": if failed { "failed" } else { "ended" }, "cause": cause, "secs": secs,
        }));
        self.pick_next_rx();
    }

    // ── Console ──────────────────────────────────────────────────────

    fn on_ui(&mut self, id: ClientId, cmd: UiCmd) {
        match cmd {
            UiCmd::Claim => {
                if self.owner.is_none() || self.owner == Some(id) {
                    self.owner = Some(id);
                    self.last_error = None;
                    info!("operator position taken by {}", self.clients.get(&id).map(String::as_str).unwrap_or("?"));
                } else {
                    self.send_to(id, json!({"type": "error", "error": "busy"}));
                }
                return;
            }
            UiCmd::Release => {
                if self.owner == Some(id) {
                    self.release_position();
                }
                return;
            }
            _ => {}
        }
        if self.owner != Some(id) {
            self.send_to(id, json!({"type": "error", "error": "not_operator"}));
            return;
        }
        match cmd {
            UiCmd::SetGroups { listen, tx } => {
                self.listen = listen.into_iter().filter(|g| valid_ssi(*g)).take(64).collect();
                self.tx_group = if valid_ssi(tx) { tx } else { 0 };
                if self.tx.as_ref().is_some_and(|t| t.gssi != self.tx_group) {
                    self.end_group_tx();
                }
                self.sync_affiliations();
                let wanted = self.wanted_groups();
                self.rx.retain(|_, c| wanted.contains(&c.gssi));
                if self.playing.is_some_and(|u| !self.rx.contains_key(&u)) {
                    self.playing = None;
                    self.pick_next_rx();
                }
            }
            UiCmd::Ptt { down } => self.set_ptt(down),
            UiCmd::Call { issi, duplex } => self.start_private(issi, duplex),
            UiCmd::Answer => self.answer(),
            UiCmd::Hangup => self.hangup(),
            UiCmd::Sds { dest, text } => self.send_text(dest, text),
            UiCmd::AmbienceListen { issi, password } => self.ambience_listen(id, issi, &password),
            UiCmd::Block { issi, block } => self.block_issi(issi, block),
            UiCmd::LocationRequest { issi, period_s } => self.request_location(issi, period_s),
            UiCmd::LocationStop { issi } => {
                self.polls.remove(&issi);
                self.push_status();
            }
            UiCmd::Claim | UiCmd::Release => unreachable!(),
        }
    }

    fn release_position(&mut self) {
        self.set_ptt(false);
        self.hangup();
        self.owner = None;
        self.ended = None;
    }

    fn send_to(&self, id: ClientId, v: Value) {
        let _ = self.ui.send(UiOut::TextTo(id, v.to_string()));
    }

    fn set_ptt(&mut self, down: bool) {
        let was_down = std::mem::replace(&mut self.ptt_down, down);
        if let Some(p) = self.private.as_mut() {
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
                self.flush_ul(uuid);
                self.send(build_simplex_idle(&uuid, 0, 0));
            }
            return;
        }
        if !down {
            self.end_group_tx();
            return;
        }
        if was_down || self.tx.is_some() {
            return;
        }
        let gssi = self.tx_group;
        if gssi == 0 {
            self.last_error = Some("no talkgroup selected for transmit".into());
            return;
        }
        if self.link.is_none() {
            self.last_error = Some("not connected to brew-server".into());
            return;
        }
        let busy = self.rx.values().any(|c| c.gssi == gssi);
        if busy && !self.preempt_until.is_some_and(|t| Instant::now() < t) {
            self.preempt_until = Some(Instant::now() + PREEMPT_WINDOW);
            self.last_error = Some(format!("talkgroup {gssi} is busy — press PTT again to transmit over it"));
            return;
        }
        self.preempt_until = None;
        let uuid = Uuid::new_v4();
        self.send(build_group_tx(&uuid, self.issi, gssi, self.cfg.priority, 0, None));
        self.tx = Some(TxCall { uuid, gssi });
        self.last_error = None;
        self.codec.reset();
        self.note("group_tx", json!({"gssi": gssi, "source": self.issi}));
    }

    fn end_group_tx(&mut self) {
        if let Some(t) = self.tx.take() {
            self.flush_ul(t.uuid);
            self.send(build_group_idle(&t.uuid, 0));
        }
    }

    fn flush_ul(&mut self, uuid: Uuid) {
        for block in self.codec.flush() {
            self.send(build_voice_frame(&uuid, (block.len() * 8) as u16, &block));
        }
    }

    fn on_ul_pcm(&mut self, pcm: &[i16]) {
        let uuid = match (&self.private, &self.tx) {
            (Some(p), _) if p.phase == Phase::Connected || p.phase == Phase::Answering => {
                if !(p.duplex || p.talking) {
                    return;
                }
                p.uuid
            }
            (Some(_), _) => return,
            (None, Some(t)) => t.uuid,
            (None, None) => return,
        };
        for block in self.codec.encode_pcm(pcm) {
            self.send(build_voice_frame(&uuid, (block.len() * 8) as u16, &block));
        }
    }

    /// Ask a radio for a position report. `period_s` 0 sends one request;
    /// otherwise the console re-sends every `period_s` seconds until stopped.
    fn request_location(&mut self, issi: u32, period_s: u32) {
        if !valid_ssi(issi) {
            self.last_error = Some("invalid ISSI".into());
            return;
        }
        if self.link.is_none() {
            self.last_error = Some("not connected to brew-server".into());
            return;
        }
        self.send_sds_raw(issi, &sds::encode_location_request());
        let period_s = period_s.clamp(0, 86_400);
        if period_s == 0 {
            self.polls.remove(&issi);
        } else {
            let period = Duration::from_secs(period_s as u64);
            self.polls.insert(issi, (period, Instant::now() + period));
        }
        self.note("location_req", json!({"issi": issi, "period_s": period_s}));
        self.push_status();
    }

    fn send_text(&mut self, dest: u32, text: String) {
        let text = text.trim().to_string();
        if !valid_ssi(dest) || text.is_empty() {
            self.last_error = Some("SDS needs a destination and text".into());
            return;
        }
        if self.link.is_none() {
            self.last_error = Some("not connected to brew-server".into());
            return;
        }
        self.sds_ref = self.sds_ref.wrapping_add(1);
        let payload = sds::encode_text(&text, self.sds_ref);
        let uuid = self.send_sds_raw(dest, &payload);
        self.push_sds(json!({
            "dir": "out", "from": self.issi, "to": dest, "text": text,
            "uuid": uuid.to_string(), "ts": epoch_ms(), "delivered": false,
        }));
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

    fn on_tick(&mut self) -> bool {
        let mut changed = false;
        let now = Instant::now();
        if self.ended.as_ref().is_some_and(|e| now >= e.until) {
            self.ended = None;
            changed = true;
        }
        let stale: Vec<Uuid> = self.rx.iter().filter(|(_, c)| c.last.elapsed() > RX_STALE).map(|(u, _)| *u).collect();
        for uuid in stale {
            self.on_group_idle(uuid, 0);
            changed = true;
        }
        if let Some(p) = self.private.as_ref() {
            if !p.inbound && p.phase != Phase::Connected && p.started.elapsed() > SETUP_TIMEOUT {
                let p = self.private.take().unwrap();
                self.send(build_call_release(&p.uuid, CAUSE_NOT_REACHABLE));
                self.end_private(&p, true, CAUSE_NOT_REACHABLE);
                changed = true;
            }
        }
        if self.preempt_until.is_some_and(|t| now >= t) {
            self.preempt_until = None;
            changed = true;
        }
        if self.alarms_at.is_some_and(|t| t.elapsed() > ALARMS_STALE) {
            self.alarms.clear();
            self.alarms_at = None;
            changed = true;
        }
        // Re-send location requests that have come due.
        let due: Vec<u32> = self.polls.iter().filter(|(_, (_, next))| now >= *next).map(|(&i, _)| i).collect();
        for issi in due {
            if self.link.is_some() {
                self.send_sds_raw(issi, &sds::encode_location_request());
            }
            if let Some((period, next)) = self.polls.get_mut(&issi) {
                *next = now + *period;
            }
        }
        changed
    }

    fn status(&self) -> Value {
        let rx = self.playing.and_then(|u| self.rx.get(&u)).map(|c| json!({"gssi": c.gssi, "source": c.source}));
        let active: Vec<Value> = self.rx.values().map(|c| json!({"gssi": c.gssi, "source": c.source, "emergency": c.emergency})).collect();
        // Every emergency the console knows: calls it hears at priority 15 and the list
        // brew-server pushes (alarms, and calls on groups relayed from other servers).
        let mut emergencies: BTreeMap<u32, Option<u32>> = self.alarms.clone();
        for c in self.rx.values().filter(|c| c.emergency) {
            emergencies.insert(c.source, Some(c.gssi));
        }
        let emergencies: Vec<Value> = emergencies.iter().map(|(&issi, &gssi)| json!({"issi": issi, "gssi": gssi})).collect();
        let private = if let Some(p) = &self.private {
            json!({
                "peer": p.peer, "duplex": p.duplex, "inbound": p.inbound, "phase": p.phase.name(),
                "talking": p.talking, "peer_talking": p.peer_talking, "ambience": p.ambience,
                "connected_ms": p.connected_at.map(epoch_ms_at),
            })
        } else if let Some(e) = &self.ended {
            json!({
                "peer": e.peer, "duplex": e.duplex, "inbound": false,
                "phase": if e.failed { "failed" } else { "ended" }, "cause": e.cause,
            })
        } else {
            Value::Null
        };
        json!({
            "type": "status",
            "connected": self.link.is_some(),
            "link_error": self.link_error,
            "issi": self.issi,
            "listen": self.listen,
            "tx_group": self.tx_group,
            "transmitting": self.tx.as_ref().map(|t| t.gssi),
            "preempt_offer": self.preempt_until.is_some(),
            "rx": rx,
            "active": active,
            "emergencies": emergencies,
            "blacklist": self.blacklist,
            "can_block": self.can_block,
            "private": private,
            "polls": self.polls.iter().map(|(&i, (p, _))| json!({"issi": i, "period_s": p.as_secs()})).collect::<Vec<_>>(),
            "operator": self.owner.and_then(|id| self.clients.get(&id)),
            "ambience_auth": !self.cfg.ambience_password.is_empty(),
            "last_error": self.last_error,
        })
    }

    fn push_status(&self) {
        let mut s = self.status();
        // Each console learns whether it is the operator.
        for &id in self.clients.keys() {
            s["you_operate"] = json!(self.owner == Some(id));
            let _ = self.ui.send(UiOut::TextTo(id, s.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OP: u32 = 9_990_001;

    fn setup() -> (Dispatcher, broadcast::Receiver<UiOut>, mpsc::Receiver<Vec<u8>>) {
        setup_with(DispatchConfig { operator_issi: OP, groups: vec![91, 92], tx_group: 91, priority: 0, ambience_password: String::new() })
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
            operator_issi: OP, groups: vec![91, 92], tx_group: 91, priority: 0,
            ambience_password: password.to_string(),
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
        assert!(d.private.is_none());
        assert_eq!(last_log(&d), (Some("ambience"), Some(false)));
        assert!(d.status()["ambience_auth"] == true);
    }

    #[test]
    fn ambience_right_password_starts() {
        let (mut d, _ui, mut rx) = setup_al("secret");
        drain(&mut rx);
        d.handle(Event::Ui(1, UiCmd::AmbienceListen { issi: 2001, password: "secret".into() }));
        assert_eq!(call_states(&drain(&mut rx)), vec![CALL_STATE_SETUP_REQUEST]);
        assert!(d.private.as_ref().is_some_and(|p| p.ambience));
        assert_eq!(last_log(&d), (Some("ambience"), Some(true)));
    }
}
