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
- **Voice recording**: records on the server (the operator's traffic, saved
  as WAV in `[dispatch] recordings_dir`) or on the browser's device. Both ask
  for a file name, which always starts with `YYYYMMDD-HHMMSS`. Server
  recordings are listed in the console to play, download or delete, and
  recording start/stop, play, download and delete go to the activity log.
- **HTTPS console**: optional built-in TLS (`[web] tls`), so browsers allow the
  microphone without a reverse proxy.
- **One operator, or several**: by default one browser takes the operator
  position and the others watch. With operator accounts configured, each
  operator logs in and works their own seat (own ISSI, talkgroups, calls and
  PTT) at the same time over the one brew-server link. See
  [Several operators](#several-operators).

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

## Several operators

Without accounts the console is a single operator position: `[dispatch]
operator_issi` is the one identity on the network, `[web] password` (HTTP
Basic) guards the page, and the first browser to take the position holds it.

Add accounts under `[dispatch.operators]` and the console becomes
multi-seat:

```toml
[dispatch.operators.dave]
password = "secret"
issi = 9990001          # this operator's ISSI on the network; unique per operator
role = "admin"          # admin | operator | viewer   (default operator)
groups = [91, 92]       # optional: initial listen list (default: [dispatch] groups)
tx_group = 91           # optional (default: first of groups)

[dispatch.operators.sue]
password = "another"
issi = 9990002
```

- The page asks for a name and password. A session cookie (`HttpOnly`,
  `SameSite=Strict`, kept in memory, so a restart logs everyone out) binds
  the browser to its account; there is a Log out button. `[web] password` is
  ignored, with a warning at start.
- Every account with a seat registers its own ISSI on the brew-server when
  the link comes up (and again after a reconnect) and affiliates its own
  talkgroups under it. Radios call and message an operator by their ISSI.
- A received group call is played on every seat that listens to that group;
  emergency calls reach every seat. A private call rings on the seat that owns
  the called ISSI, and is rejected with "not reachable" when nobody holds it.
  PTT transmits with the seat's ISSI on the seat's TX group: two operators
  may talk on different groups at once; on the same group brew-server's
  one-talker rule applies and the press-again-to-pre-empt logic works per
  seat. Since brew-server does not echo a connection's own transmission, an
  operator's voice is handed to the other seats listening to that group by
  the console itself.
- SDS is sent from the seat's ISSI; SDS to an operator's ISSI is shown with
  their name. `[dispatch] operator_issi` stays registered as a shared console
  identity (unless an account uses it), so SDS sent to the old single ISSI
  still arrives.
- Within one account several browsers may be open: the first to take the seat
  holds it, the others watch it, as before. Activity and SDS log entries say
  which operator caused them, and server recordings are per seat.
- `viewer` watches everything (status, activity, SDS, map, recordings) but
  has no seat and cannot command. `admin` additionally manages accounts from
  the Operators card: an added account registers on the network at once, a
  removed one is deregistered and its browsers are logged out, and the change
  is written back to the config file (only the `dispatch.operators` table is
  rewritten; the file is replaced atomically). The last admin cannot be
  removed or demoted, nor can an admin remove their own account.

The same is available over HTTP for scripts, with the session cookie:
`GET /api/operators` (no passwords), `POST /api/operators/<name>` with
`{"password", "issi", "role", "groups", "tx_group"}` (password optional when
updating), `DELETE /api/operators/<name>`.

## Layout

| File | Role |
|---|---|
| `src/brew_link.rs` | Discovery, Digest auth, WebSocket, keepalive and redial |
| `src/protocol.rs` | Brew wire format (shared with bost-flowstation's `net_brew`) |
| `src/dispatcher.rs` | Registration, affiliations, group/private calls, SDS, operator seats |
| `src/accounts.rs` | Operator accounts: login sessions, runtime add/remove, config file rewrite |
| `src/codec.rs` | PCM ↔ ACELP and the 36-byte STE traffic payload |
| `src/sds.rs` | SDS text encode/decode |
| `src/recording.rs` | Server-side WAV recordings: file names, writer, listing |
| `src/web.rs` | Console HTTP and WebSocket (JSON commands, binary PCM), login, account API, recordings download |
| `static/index.html` | The console (ES/EN, light/dark) |
| `sample/tetra-dispatch.toml` | Example configuration |

```bash
cargo test
```

## Credits

- David Harcus MM7FDM
