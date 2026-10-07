# Changelog

All notable changes to Tetra Dispatch, newest first.

## Unreleased

- **Emergency list from brew-server.** The red ribbon now also covers an
  emergency alarm with no call running, from the list brew-server pushes
  (service message `0x11`); it merges with the emergency calls the console
  hears. The list expires after 15 s without a refresh.
- **Red radio on the map.** A radio in an emergency is drawn red and pulsing
  with an EMERGENCY popup; an emergency radio with no position yet is named in
  the map line.
- **Emergency calls.** An emergency group call (priority 15) is now taken and
  played even when the console is not listening to that group, takes the
  speaker over from an ordinary group call, and shows a red "EMERGENCY ACTIVE"
  ribbon with the calling ISSI and group while it lasts. Needs a brew-server
  that pushes emergency calls to consoles (brew-server after 1.16.0).

## 1.0.0

First release. Tetra Dispatch is a browser dispatch console that connects
directly to a brew-server. It started from the LST Dispatch console in
bost-flowstation, which is still there for cell-local dispatch.

- **Direct brew-server link.** Logs in the way a Basestation does: Brew
  discovery, Digest auth and a WebSocket upgrade. TLS is optional, checked
  against a CA bundle or a pinned self-signed certificate. The link sends
  pings and redials on its own after a drop.
- **Own operator ISSI.** Registers `[dispatch] operator_issi` on the network
  and registers again after every reconnect.
- **Talkgroups.** Set a listen list and a TX group from the console. The
  console affiliates and deaffiliates only the groups that changed.
- **Group calls.** You hear the active call on any listened group. PTT works
  on screen or with the Space bar. If the TX group is busy, a second press
  within 3 s transmits over it.
- **Private calls.** Outgoing and incoming calls, duplex or simplex. For
  simplex calls the floor is handled with SIMPLEX_GRANTED/IDLE. An unanswered
  call ends after 60 s. Incoming calls are rejected when no operator holds the
  position.
- **SDS.** Text messages go both ways (SDS-TL, protocol 0x82). The console
  answers delivery reports and shows which sent messages were delivered.
- **Audio.** Voice uses the ETSI ACELP reference codec in the 36-byte STE
  traffic format that Basestations use. The browser sends and receives PCM at
  8 kHz over the console WebSocket.
- **Console.** One browser holds the operator position at a time and others
  watch. It has an activity log and an SDS log, Spanish and English, light and
  dark themes, and an optional HTTP Basic password.
- **HTTPS.** `[web] tls` with `tls_cert_path` / `tls_key_path` serves the page
  and WebSocket over TLS, so browsers allow the microphone without a proxy.
- **Default port 8443** (`[web] listen = "0.0.0.0:8443"`).
