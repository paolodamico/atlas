//! Background websocket sync: applies remote changes into the vault and pushes
//! local ones, reconnecting with backoff. It feeds the same event stream as
//! local edits, so the host renders remote and local changes identically.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use atlas_core::{Cursor, Vault};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::{Event, SyncStatus, emit_applied, guard};

/// How often to push accumulated local changes.
const PUSH_INTERVAL: Duration = Duration::from_millis(200);
const BACKOFF_START: Duration = Duration::from_millis(250);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// A session must stay up this long before we treat it as healthy and reset the
/// backoff. Otherwise a relay that accepts then immediately drops the socket
/// would be retried several times a second forever.
const HEALTHY_SESSION: Duration = Duration::from_secs(5);
/// Drop a connection that has produced no frame (not even a server ping) within
/// this window. MUST exceed the relay's ping interval.
const READ_TIMEOUT: Duration = Duration::from_secs(40);
/// How often to check the read timeout.
const LIVENESS_CHECK: Duration = Duration::from_secs(10);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Serialize)]
enum ClientMsg {
    Hello { since: Option<Cursor> },
    Push { changes: Vec<Vec<u8>> },
}

#[derive(Deserialize)]
struct ServerMsg {
    changes: Vec<Vec<u8>>,
    cursor: Cursor,
}

/// The connection target.
struct Target {
    /// The websocket URL to dial.
    ws_url: String,
    /// The sync-state key: scoped by relay so pointing a graph at a different
    /// relay does not reuse stale cursor/pushed state.
    scope: String,
}

impl Target {
    /// Builds the relay target URL
    fn parse(base: &str, graph: &str) -> Option<Self> {
        let base = base.trim_end_matches('/');
        if base.is_empty()
            || graph.is_empty()
            || graph.chars().all(|c| {
                !c.is_control() && !c.is_whitespace() && !matches!(c, '/' | '?' | '#' | '%')
            })
        {
            return None;
        }
        Some(Self {
            ws_url: format!("{base}/graphs/{graph}/ws"),
            scope: format!("{}-{graph}", relay_tag(base)),
        })
    }
}

/// A short, filesystem-safe token for a relay URL, used to scope persisted sync
/// progress to the relay.
///
/// FNV-1a, not [`std::hash::DefaultHasher`], because this is an on-disk key: its
/// algorithm is fixed here, so a rebuilt client never re-hashes the same relay
/// URL to a new scope and loses its cursor and pushed heads.
///
/// Reference: <https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function>
fn relay_tag(base: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in base.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Drives background sync for one graph: owns the shared context and reconnects
/// forever, running a session per connection.
pub(crate) struct Syncer {
    vault: Arc<Mutex<Vault>>,
    events: broadcast::Sender<Event>,
    graph: String,
}

impl Syncer {
    pub(crate) fn new(
        vault: Arc<Mutex<Vault>>,
        events: broadcast::Sender<Event>,
        graph: String,
    ) -> Self {
        Self {
            vault,
            events,
            graph,
        }
    }

    /// Connects, runs a session until the socket drops, then backs off and
    /// retries. Reports connection state as [`Event::Status`].
    ///
    /// The backoff is reset only after a session stays up long enough to be
    /// healthy, so a relay that flaps (accepts then immediately closes) is
    /// backed off instead of hammered.
    pub(crate) async fn run(self, url: String) {
        let Some(target) = Target::parse(&url, &self.graph) else {
            self.set_status(SyncStatus::Offline);
            return;
        };
        let mut backoff = BACKOFF_START;
        loop {
            self.set_status(SyncStatus::Connecting);
            if let Ok((socket, _)) = connect_async(target.ws_url.as_str()).await {
                self.set_status(SyncStatus::Live);
                let started = Instant::now();
                let _ = self.session(socket, &target.scope).await;
                if started.elapsed() >= HEALTHY_SESSION {
                    backoff = BACKOFF_START;
                }
            }
            self.set_status(SyncStatus::Offline);
            sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    /// Greets the relay with the local cursor, then continuously applies
    /// incoming batches and pushes local changes until the socket drops or goes
    /// silent past [`READ_TIMEOUT`].
    async fn session(&self, mut socket: Socket, scope: &str) -> Result<(), Disconnected> {
        let Ok(since) = guard(&self.vault).sync_cursor(scope) else {
            return Ok(());
        };
        self.send(&mut socket, &ClientMsg::Hello { since }).await?;

        let mut push = interval(PUSH_INTERVAL);
        let mut liveness = interval(LIVENESS_CHECK);
        let mut in_flight: HashSet<Vec<u8>> = HashSet::new();
        let mut last_seen = Instant::now();
        loop {
            tokio::select! {
                incoming = socket.next() => match incoming {
                    Some(Ok(Message::Binary(bytes))) => {
                        last_seen = Instant::now();
                        self.apply(scope, &bytes, &mut in_flight)?;
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    // Pings, pongs, and other frames still prove the peer is alive.
                    Some(Ok(_)) => last_seen = Instant::now(),
                },
                _ = push.tick() => self.push(&mut socket, scope, &mut in_flight).await?,
                _ = liveness.tick() => {
                    if last_seen.elapsed() >= READ_TIMEOUT {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Applies one remote batch and emits events for what changed. A malformed
    /// frame is skipped, but a failed apply is surfaced so the session drops and
    /// reconnects from the unchanged cursor rather than skipping those changes.
    ///
    /// Echoed blobs are cleared from `in_flight` first: the relay has now
    /// acknowledged them, so they are no longer pending a resend.
    fn apply(
        &self,
        scope: &str,
        bytes: &[u8],
        in_flight: &mut HashSet<Vec<u8>>,
    ) -> Result<(), Disconnected> {
        let Ok(ServerMsg { changes, cursor }) = ciborium::from_reader(bytes) else {
            return Ok(());
        };
        for blob in &changes {
            in_flight.remove(blob);
        }
        let applied = guard(&self.vault)
            .apply_remote(scope, changes, cursor)
            .map_err(|e| Disconnected::Apply(e.to_string()))?;
        emit_applied(&self.vault, &self.events, &applied);
        Ok(())
    }

    /// Sends local changes not yet pushed, skipping any already in flight (sent
    /// but not yet echoed) so a slow echo does not make us resend duplicates.
    async fn push(
        &self,
        socket: &mut Socket,
        scope: &str,
        in_flight: &mut HashSet<Vec<u8>>,
    ) -> Result<(), Disconnected> {
        let Ok(changes) = guard(&self.vault).collect_outgoing(scope) else {
            return Ok(());
        };
        let fresh: Vec<Vec<u8>> = changes
            .into_iter()
            .filter(|blob| !in_flight.contains(blob))
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        for blob in &fresh {
            in_flight.insert(blob.clone());
        }
        self.send(socket, &ClientMsg::Push { changes: fresh }).await
    }

    async fn send(&self, socket: &mut Socket, msg: &ClientMsg) -> Result<(), Disconnected> {
        let mut body = Vec::new();
        ciborium::into_writer(msg, &mut body).map_err(|e| Disconnected::Encode(e.to_string()))?;
        socket.send(Message::binary(body)).await?;
        Ok(())
    }

    fn set_status(&self, status: SyncStatus) {
        let _ = self.events.send(Event::Status(status));
    }
}

/// The websocket is finished; the caller should back off and reconnect.
#[derive(Debug, thiserror::Error)]
enum Disconnected {
    #[error("encoding message: {0}")]
    Encode(String),
    #[error("applying remote changes: {0}")]
    Apply(String),
    #[error("websocket: {0}")]
    Socket(#[from] tungstenite::Error),
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::{Target, relay_tag};

    #[test]
    fn trailing_slash_in_base_url_is_normalized() {
        let target = Target::parse("ws://host:4000/", "g").unwrap();
        assert_eq!(target.ws_url, "ws://host:4000/graphs/g/ws");
    }

    #[test]
    fn ordinary_graph_names_are_accepted() {
        for good in ["demo", "g", "notes-2026", "a.b_c"] {
            assert!(Target::parse("ws://host", good).is_some());
        }
    }

    #[test]
    fn graph_names_that_would_corrupt_the_path_are_rejected() {
        assert!(Target::parse("ws://host", "").is_none());

        for bad in [
            "a/b", "a?b", "a#b", "a%20b", "a b", "a\tb", "a\u{0}b", "a<b", "a>b", "a`b", "café",
        ] {
            assert!(Target::parse("ws://host", bad).is_none());
        }
    }

    #[test]
    fn empty_base_url_is_rejected() {
        assert!(Target::parse("", "g").is_none());
        assert!(Target::parse("/", "g").is_none());
    }

    #[test]
    fn scope_tracks_the_relay_not_just_the_graph() {
        let one = Target::parse("ws://host-a:4000", "g").unwrap();
        let two = Target::parse("ws://host-b:4000", "g").unwrap();
        assert_ne!(
            one.scope, two.scope,
            "different relays must not share state"
        );
        assert!(one.scope.ends_with("-g"));
    }

    #[test]
    fn relay_tag_is_stable_and_filesystem_safe() {
        assert_eq!(relay_tag("ws://host:4000"), relay_tag("ws://host:4000"));
        assert!(
            relay_tag("ws://host:4000/a/b")
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        );
    }
}
