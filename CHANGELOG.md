# Changelog

## [Unreleased]

Two-way audio (talk), ported from the neolink talk client:
- `bairelay talk <camera> --file <wav> [--volume <gain>]` plays a 16-bit PCM WAV (up to 60 s) through the camera speaker. Pure-Rust IMA ADPCM (DVI4 block layout) encoder; no GStreamer.
- MQTT `<prefix>/<cam>/control/talk` (payload: absolute host path to a WAV) with progress on `<prefix>/<cam>/status/talk` (`playing`, `done`, `error: <code>`).
- `[cameras.talk_config]` pins the `TalkConfig` (duplex, stream mode, ADPCM rate and block size). Required for battery doorbells, which neither answer `TalkAbility` nor advertise `two_way_audio`.
- Top-level `talk_dir` enables MQTT talk and confines its paths to one directory; without it `control/talk` replies `error: disabled`.

### Changes

- talk: `talk` one-shot and `control/talk` topic
- talk: three-tier `TalkConfig` resolver (config table, camera `TalkAbility`, Reolink defaults)
- talk: WAV reader and linear resampler; DVI4 IMA ADPCM encoder in `bairelay-rtsp` next to the decoder
- config: `[cameras.talk_config]` table and `talk_dir` (absolute, existing directory), validated by `check-config`
- core: `talk` and `talk_stop` no longer gate on `two_way_audio` (neolink parity; the binary checks it unless `[cameras.talk_config]` is set); fix underflow on a short last talk payload and a panic on `sample_rate = 0`
- core: pad serialized ADPCM frames on `(data.len() + 4) % 8` like the camera; talk sent 4 stray bytes after every block, and `--dump-bcmedia` captures with ADPCM desynced on replay
- core: remove the unused `BcCamera::talk_stream` and the `crossbeam-channel` dependency

Per-camera `stream_latency_ms` (default 1500): the pacer startup buffer, previously fixed at 1.5 s. Steady wired cameras can lower it to cut stream latency; `0` disables pacing.

### Toolchain and dependencies

- Rust 1.100, edition 2024 (was 1.94.1 / 2021)
- nom 8, rand 0.10, reqwest 0.13 (cloud HTTPS now verifies against the OS trust store), toml 1, aes/cfb-mode on cipher 0.5, x25519-dalek 3, rcgen 0.14, criterion 0.8, rtsp-types/rtp-types 0.2, sdp-types 0.3
- replace unmaintained `get_if_addrs` with `if-addrs` and `rustls-pemfile` with `rustls-pki-types`; drop `lazy_static`, `regex` and `crossbeam-channel`

### Protocol fixes

- UDP: ACKs report the measured receive rate in bytes/s (the field the camera's congestion control reads, see neolink #428); we sent ACK spacing in µs, which throttled high-bitrate streams on UID/P2P connections
- UDP: reorder buffer can no longer deadlock when full; wrap-aware sequence handling; skip a gap stalled for 500 ms; 10 s timeout resets only on in-order progress
- UDP: ACK truth table capped at 200 bytes; undecodable or foreign datagrams are dropped instead of ending the session; D2C_DISC only ends the session when it matches our cid/did
- UDP: answer post-connect D2C_T with C2D_A; heartbeats keep the discovery tid; remote discovery no longer fails when R2C_C_R has no `dev`
- FullAes: keep the clear tail of binary payloads past `encryptLen`
- TCP: a clean EOF fails subscribers immediately; a stalled subscriber no longer blocks routing for the whole connection
- BcMedia: cap frame size fields, resync on the next magic instead of clearing the buffer, reject AAC frames with mismatched size fields
- an XML payload that fails to parse no longer ends the connection; legacy frames consume their body; binary mode keyed by (msg_id, msg_num)
- an empty ability map (account cameras) no longer fails every gated command
- snapshot subscribes before sending the request; PTZ zoom no longer panics on an inverted range

### RTSP fixes

- demux client interleaved data (RTCP RR over TCP) and pipelined requests instead of replying 400 and closing
- PLAY response is written before media; RTP-Info carries the track URL and the SETUP-chosen seq
- TCP writes time out after 5 s; one write per interleaved packet
- Digest accepts the RFC 2069 no-qop form (VLC, openRTSP); constant-time credential compares; Basic is only offered over TLS
- 501 for unsupported methods, 454 for SETUP on an unknown session, SET_PARAMETER keepalive
- SDP uses `IN IP6` for IPv6; AAC track omitted without a config; H.264 FU-A keeps the F bit; no marker on every PCMU packet; CRC-protected ADTS headers stripped correctly
- remove the unused P-frame buffer, RTCP sender reports and session state machine

### Camera lifecycle fixes

- a watchdog disconnect request can no longer kill the next session
- dead stream sources are replaced and their RTSP sessions closed; pruned readers are awaited
- login failures retry on a 5–30 min backoff instead of stopping the camera; the daemon only exits on shutdown
- session setup requests time out; keepalive drops a dead connection on the first probe; connect backoff resets only after a stable session
- video RTP timestamps stay monotonic across the camera's 71.6 min µs wrap; codec seeded from the BcMedia tag
- grace period requires the full idle window; motion wake hold released 60 s after the last Start; motion/AI cleared on teardown

### MQTT, HA and services

- HA availability follows the bridge LWT as well as the camera; `{prefix}/status` is set `online` on every connect
- discovery configs stay retained on shutdown, so HA keeps entities and their customisations across restarts
- publishes never block on a dead broker; retained control messages are ignored
- entity names carry only the feature; HA adds the device name (upstream #13)
- wake server only serves configured UIDs, matches exactly (or plus the 4-character firmware suffix) and runs one wake burst per camera
- push listener matches cameras by address without the wake server (upstream #9); backs off on accept errors; handles v4-mapped peers
- webhook alarm takes the wake hold before publishing
- one-shots load the MFA trust token; `users` prompts before connecting; CLI and MQTT PTZ share one move/stop path; timeouts exit 4
- config: durations above one day are rejected; parse errors keep line and column; trust token file created 0600

## [1.1.2] — 2026-06-08

Maintenance release:
- Renamed crates to avoid clashing namespaces
- Published to crates.io
- Updated CI to fix code review issues
- Added automattic crates.io publishing

### Changes

- codeql: build-mode none + v4 actions
- ci: explicit workflow permissions + codeql advanced setup
- ci: auto-publish to crates.io on release publish
- bairelay: fix cargo exclude patterns with leading slash
- bairelay-neolink-core: pin env_logger dev-dep for crates.io
- crates: exclude unneeded files.
- crates: update documentation, add publish-crates.sh
- crates: workspace.dependencies, polish, release.sh bump
- rename: bairelay-* namespace for crates.io publishing

## [1.1.1] — 2026-05-15

HAOS App fixes and improvements:
- separate `address` / `uid` / `username` / `idle_disconnect` fields per camera (was a single `host_or_uid` blob).
- `/config/bairelay/config.toml` is now the authoritative editable config — bootstrapped from an annotated template on first start, merged with the form on every restart. The file no longer needs to restate `username` for partial-overrride camera entries.
- `/config/bairelay/effective.toml` written every start (read-only diagnostic of the merged result, with defaults stripped).
- HA MQTT discovery auto-enabled at default topic `homeassistant`; entities appear in HA without manual `[mqtt.discovery]` configuration.
- Configuration form labels and inline descriptions localised via `translations/en.yaml`.
       
### Changes

- release: mirror CHANGELOG section to hassio/bairelay/CHANGELOG.md
- hassio: config.toml authoritative; auto-discovery; UI labels
- hassio: split address/uid/username; add idle_disconnect (default true)
- hassio: pass --mqtt-ssl as flag; silence bashio Service-not-enabled noise
- docs: HA UI is Settings -> Apps -> Install app (renamed in 2026.2)
- tests: ip-monitor-route daemon kills Colima's re-added eth0 route
- release: arm64 case match; build+release skip on release:published
- ci: force rustup default stable after dtolnay; print rustup show
- Updated CHANGELOG.md and fixed release script.
- ci: pin toolchain via rust-toolchain.toml

## [1.1.0] — 2026-05-15

Added Dockerfile and initial Home Assistant Add-On support under `hassio/` then updated documentation accordingly.

### Changes

- hassio: rename map to homeassistant_config; drop default boot:auto
- hassio: set SHELL ash -o pipefail so sha256sum pipe propagates
- ci: pin hadolint-action to v3.3.0
- tests: persistent fix for Colima VM networking (route + DNS)
- hassio: real icon + logo from docs/logos/ source masters
- hassio: pin base image to ghcr.io/hassio-addons/base:20.1.1
- hassio: field-merge MqttServerConfig — preserve base topic_prefix
- docs: README install-as-add-on section + CHANGELOG 1.1.0 entry
- ha-verify: list --bairelay-as-container in --help; drop dead id file
- ci: hadolint + HA add-on YAML lint on every push
- testing: document HA add-on verification surfaces
- ha-verify: --bairelay-as-container runs the add-on image
- release.sh: replay block covers hassio manifest too
- release.sh: bump hassio/bairelay/config.yaml version in lockstep
- release.yml: docker job pushes per-arch images to GHCR on publish
- release.yml: publish SHA256SUMS alongside tarballs
- hassio: boot:auto + entrypoint /tmp comment
- hassio: treat --mqtt-port 0 as unset sentinel
- hassio: CHANGELOG + README stubs for the add-on directory
- hassio: DOCS.md — install, options form, TOML overlay, debug
- hassio: placeholder transparent icon + logo (replace later)
- hassio: s6 longrun service runs render-hassio-config + bairelay
- hassio: Dockerfile pulls SHA-verified tarball from GH release
- hassio: build.yaml pins hassio-addons/base 15.0.10 per arch
- hassio: config.yaml — slug, options, services, network
- hassio: repository.yaml at repo root for HA scanner
- hassio: rustfmt fixup for cmd.rs
- hassio: end-to-end CLI test for render-hassio-config
- hassio: wire render-hassio-config subcommand into CLI
- hassio: tighten merge — pin wake_server.enable, flag mqtt invariant
- hassio: top-level merge() ties top-level + per-camera passes
- hassio: merge_cameras combines HA-options + overlay by name
- hassio: merge_top_level overlays bairelay-wide knobs
- hassio: parse_overlay reads operator TOML
- hassio: pin build_base_config field mappings; cover half-set creds
- hassio: cover empty-camera + no-mqtt-injection + ssl paths
- hassio: build_base_config maps HassioOptions to Config
- hassio: add MqttServiceFlags struct for Supervisor injection
- hassio: parse Supervisor options.json into HassioOptions
- hassio: scaffold module tree for render-hassio-config
- build: target reproducible releases; bump workspace to 1.0.0
- readme: bump stated rust toolchain floor to 1.93

## [1.0.0] — 2026-05-03

First public binary release.

### Changes

- set_time: read DST and compensate, fix +1h drift
- mqtt: cache + republish status on broker ConnAck for HA recovery
- mqtt: title-case preserves caps + splits on `-`; fix Floodlight Tasks

## [0.9.0] — 2026-05-02

Initial repo published.

Bairelay is a pure-Rust replacement for [Neolink](https://github.com/QuantumEntangledAndy/neolink) for Reolink battery cameras (Argus class). Relevant Baichuan-protocol code from Neolink is vendored in `neolink_core`; everything else is a clean rewrite.

### Highlights vs Neolink
- Pure Rust, no GStreamer — single static binary, no native deps.
- Stable jitter-free RTSP / RTSPS with audio + video pacers, gap bridging and proper battery-camera lifecycle.
- Tighter Home Assistant integration via MQTT including image previews with overlays, reading camera presets, PTZ and more.
- HEVC handling that survives Home Assistant's `ffmpeg:` re-publish and go2rtc's snapshot transcoder.
- Local replacements for Reolink's cloud wake server `p2p.reolink.com` and motion-push listener `pushx.reolink.com` (experimental).
- Improved one-shot CLI subcommands and a `check-config` validator with a coarse exit-code table.
- TLS (`rtsps://`) parallel listener with optional client-cert mTLS.
- Drop-in compatible with Neolink configs via `mqtt.topic_prefix = "neolink"`.
- >2000 workspace tests, fuzz harness, live-verify rigs against real Argus hardware.

### Deferred
- Two-way audio (`talk`).

See `README.md` for full rationale and acknowledgements.
