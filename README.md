# Tetra Dispatch

- **What's new:** see [CHANGELOG.md](CHANGELOG.md)

Browser TETRA dispatch console that connects **directly to a
[brew-server](https://github.com/ysamouhos/brew-server)**. It logs in the
same way a Basestation does (Brew discovery, Digest auth, WebSocket), then
registers its own operator ISSI on the network. You don't need a base station
or SDR.

It grew out of the LST Dispatch console in
[bost-flowstation](https://github.com/Aitorrio/bost-flowstation). That console
stays where it is, for cell-local dispatch without a Brew network.

## What it does

- **Talkgroups**: picks which groups to listen to and which one to transmit
  on. The console handles the AFFILIATE/DEAFFILIATE messages itself.
- **Group calls**: you hear the active call on any listened group. PTT works
  on screen or with the Space bar. If a group is busy, pressing PTT a second
  time within 3 s transmits over the current talker.
- **Private calls**: you can make and answer duplex or simplex calls to any
  ISSI the brew-server can reach.
- **SDS**: sends and receives text messages (SDS-TL, protocol 0x82).
- **HTTPS console**: optional built-in TLS (`[web] tls`), so browsers allow the
  microphone without a reverse proxy.
- **One operator at a time**: one browser takes the operator position. Other
  browsers can watch the same status, activity and SDS log.

Audio is ACELP. The reference ETSI codec is vendored in
`third_party/tetra-codec/` and compiled by `build.rs`. The browser sends and
receives PCM at 8 kHz over the console WebSocket.

## Build and run

You need a Rust toolchain and a C compiler.

```bash
cargo build --release
./target/release/tetra-dispatch sample/tetra-dispatch.toml   # the example; copy and edit it for your own
```

Then open `http://<host>:8443/` (or `https://` with `tls = true`). The console
listens on port 8443 by default (`[web] listen`).

Browsers only allow microphone access on `https://` or `http://localhost`.
For use from other machines, serve the console over HTTPS. Set `tls = true`
in `[web]` and point `tls_cert_path` / `tls_key_path` at a PEM certificate
and key, then open `https://<host>:8443/`. A self-signed pair works; each
browser has to accept it once:

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=tetra-dispatch" -addext "subjectAltName=IP:192.168.1.10,DNS:dispatch.local" \
  -keyout key.pem -out cert.pem
```

A TLS reverse proxy (nginx, Caddy, …) forwarding `/` and the `/ws`
WebSocket works too.

## brew-server side

Add a Brew user for the console to the brew-server's `[auth.users]`. The name
must be numeric, up to 7 digits:

```toml
[auth.users]
"1000009" = "a-long-secret"
```

Then put the same credentials in `[brew]` in `tetra-dispatch.toml`. The
operator ISSI (`[dispatch] operator_issi`) is the identity radios see and call.
Choose one that no radio uses.

If the brew-server uses TLS with a self-signed certificate, set `tls = true`
and `tls_pinned_cert_path` to a copy of its `server.crt`.

## Layout

| File | Role |
|---|---|
| `src/brew_link.rs` | Discovery, Digest auth, WebSocket, keepalive and redial |
| `src/protocol.rs` | Brew wire format (shared with bost-flowstation's `net_brew`) |
| `src/dispatcher.rs` | Registration, affiliations, group/private calls, SDS, operator position |
| `src/codec.rs` | PCM ↔ ACELP and the 36-byte STE traffic payload |
| `src/sds.rs` | SDS text encode/decode |
| `src/web.rs` | Console HTTP and WebSocket (JSON commands, binary PCM) |
| `static/index.html` | The console (ES/EN, light/dark) |
| `sample/tetra-dispatch.toml` | Example configuration |

```bash
cargo test
```
