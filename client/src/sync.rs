//! Background websocket sync: applies remote changes into the vault and pushes
//! local ones, reconnecting with backoff. It feeds the same event stream as
//! local edits, so the host renders remote and local changes identically.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use atlas_core::{Cursor, Vault};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
/// Max change payload per push frame, so a big backlog goes out as several
/// bounded messages the relay can accept instead of one it may reject.
const PUSH_BYTES: usize = 256 * 1024;

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
    /// Builds the target, or `None` if the URL or graph name can't form one
    fn parse(base: &str, graph: &str) -> Option<Self> {
        let base = base.trim_end_matches('/');
        let graph_ok = !graph.is_empty()
            && graph
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~'));
        if base.is_empty() || !graph_ok {
            return None;
        }
        Some(Self {
            ws_url: format!("{base}/graphs/{graph}/ws"),
            scope: format!("{}-{graph}", relay_tag(base)),
        })
    }
}

/// A short, filesystem-safe token for a relay URL, so sync state is scoped per
/// relay.
fn relay_tag(base: &str) -> String {
    let digest = Sha256::digest(base.as_bytes());
    let mut tag = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(tag, "{byte:02x}");
    }
    tag
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
    /// retries, reporting state as [`Event::Status`]. Backoff resets only after a
    /// session stays up long enough to be healthy, so a flapping relay (accept
    /// then immediately close) isn't hammered.
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

    /// Applies one remote batch and emits what changed. A malformed frame is
    /// skipped; a failed apply is surfaced so the session drops and reconnects
    /// from the unchanged cursor instead of losing those changes. Echoed blobs
    /// clear from `in_flight` first, since the relay has now acknowledged them.
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
    /// but not yet echoed) so a slow echo doesn't make us resend duplicates. A
    /// large backlog goes out as several [`PUSH_BYTES`]-bounded frames rather than
    /// one the relay might reject, which would otherwise never converge.
    async fn push(
        &self,
        socket: &mut Socket,
        scope: &str,
        in_flight: &mut HashSet<Vec<u8>>,
    ) -> Result<(), Disconnected> {
        let Ok(changes) = guard(&self.vault).collect_outgoing(scope) else {
            return Ok(());
        };
        let mut batch: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0;
        for blob in changes {
            if in_flight.contains(&blob) {
                continue;
            }
            if !batch.is_empty() && batch_bytes + blob.len() > PUSH_BYTES {
                self.send_push(socket, in_flight, std::mem::take(&mut batch))
                    .await?;
                batch_bytes = 0;
            }
            batch_bytes += blob.len();
            batch.push(blob);
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.send_push(socket, in_flight, batch).await
    }

    /// Marks `batch` in flight and sends it as one `Push` frame.
    async fn send_push(
        &self,
        socket: &mut Socket,
        in_flight: &mut HashSet<Vec<u8>>,
        batch: Vec<Vec<u8>>,
    ) -> Result<(), Disconnected> {
        for blob in &batch {
            in_flight.insert(blob.clone());
        }
        self.send(socket, &ClientMsg::Push { changes: batch }).await
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
            assert!(Target::parse("ws://host", bad).is_none(), "{bad:?}");
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
