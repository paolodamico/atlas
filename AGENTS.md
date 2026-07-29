# AGENTS.md

Reusable context for working on atlas. Keep it short; update when the shape changes.

## Crates

- `core` (`atlas-core`) — the engine. Automerge-backed vault of notes, storage
  (`FileStore`), local merge, and the relay sync protocol (`remote.rs`). No async, no network.
- `relay` (`atlas-relay`) — dumb append-only log server (axum). Stores opaque
  blobs per graph, orders them, fans them out. Never reads blob contents.
- `client` (`atlas-client`) — async SDK over `core`. Background websocket sync
  (`sync.rs`) + an event stream (`Event`) the host renders.
- `cli` (`atlas-cli`, binary `atlas`) — command-line client. `sync` uses the HTTP
  transport; `live` uses the websocket client.

## Sync model

- A **graph** is a shared log name on the relay. The relay is a per-graph `Vec`
  of opaque blobs plus a broadcast channel (`relay/src/store.rs`).
- Each blob is an `Envelope { doc_id, change }` (CBOR): one Automerge change for
  one doc. `doc_id == "root"` is the vault root (note list); otherwise a note id.
- A **cursor** `{ epoch, seq }` is a position in the log. `epoch` is for future
  snapshot generations; today it is always 0.
- Persisted sync progress lives in the vault store under `sync-{key}`
  (`SyncState { cursor, pushed }`). `pushed` = heads already on the relay, so we
  only send newer changes. A change is marked pushed only when the relay **echoes**
  it back (via `apply_remote`), so an unacked send is retried.
- The websocket client keys sync progress by **relay + graph** (`Target::scope`
  in `sync.rs`), so pointing a graph at a different relay does not reuse stale
  progress. The HTTP `atlas sync` path is still keyed by graph name alone.

## Wire protocol (CBOR both ways)

- Client → relay: `Hello { since: Option<Cursor> }`, then `Push { changes }`.
- Relay → client: `ServerMsg { changes, cursor }` — a catch-up from the cursor,
  then each fanned-out batch. The relay also sends websocket Pings every 15s; the
  client reaps a connection silent past 40s.
- The `ClientMsg` / `ServerMsg` shapes are duplicated in `client/src/sync.rs` and
  `relay/src/routes/ws.rs`; keep them in sync. HTTP shapes live in
  `relay/src/routes/sync.rs` and `cli/src/relay_transport.rs`.

## Conventions

- Workspace lints (root `Cargo.toml`) `deny` `unwrap_used`/`panic`/etc **including
  in tests**. Test modules opt out with
  `#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]`.
- Shared deps go in `[workspace.dependencies]`; crates reference `{ workspace = true }`
  and add features locally.
- Store keys become filenames (`<id>.atlas`), so anything used as a key must be
  filesystem-safe (see `relay_tag`, which hashes the relay URL).

## Commands

- Build/lint/test: `cargo build --workspace`,
  `cargo clippy --workspace --all-targets --all-features`, `cargo test --workspace`.
- Supply chain: `cargo deny check`.
- Run relay: `cargo run -p atlas-relay` (binds `RELAY_ADDR`, default `127.0.0.1:4000`).
- Try live sync: `atlas live <note> --relay ws://127.0.0.1:4000 --graph demo` in two terminals.
