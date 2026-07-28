//! Background websocket sync: applies remote changes into the vault and pushes
//! local ones, reconnecting with backoff. It feeds the same event stream as
//! local edits, so the host renders remote and local changes identically.

use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    pub(crate) async fn run(self, url: String) {
        let ws_url = format!("{url}/graphs/{}/ws", self.graph);
        let mut backoff = BACKOFF_START;
        loop {
            self.set_status(SyncStatus::Connecting);
            if let Ok((socket, _)) = connect_async(ws_url.as_str()).await {
                backoff = BACKOFF_START;
                self.set_status(SyncStatus::Live);
                let _ = self.session(socket).await;
            }
            self.set_status(SyncStatus::Offline);
            sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    /// Greets the relay with the local cursor, then continuously pushes
    /// incoming batches and periodic pushes until the socket drops.
    async fn session(&self, mut socket: Socket) -> Result<(), Disconnected> {
        let Ok(since) = guard(&self.vault).sync_cursor(&self.graph) else {
            return Ok(());
        };
        self.send(&mut socket, &ClientMsg::Hello { since }).await?;

        let mut push = interval(PUSH_INTERVAL);
        loop {
            tokio::select! {
                incoming = socket.next() => match incoming {
                    Some(Ok(Message::Binary(bytes))) => self.apply(&bytes)?,
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(_)) => {}
                },
                _ = push.tick() => self.push(&mut socket).await?,
            }
        }
    }

    /// Applies one remote batch and emits events for what changed. A malformed
    /// frame is skipped, but a failed apply is surfaced so the session drops and
    /// reconnects from the unchanged cursor rather than skipping those changes.
    fn apply(&self, bytes: &[u8]) -> Result<(), Disconnected> {
        let Ok(ServerMsg { changes, cursor }) = ciborium::from_reader(bytes) else {
            return Ok(());
        };
        let applied = guard(&self.vault)
            .apply_remote(&self.graph, changes, cursor)
            .map_err(|e| Disconnected::Apply(e.to_string()))?;
        emit_applied(&self.vault, &self.events, &applied);
        Ok(())
    }

    /// Sends any local changes not yet pushed to the relay.
    async fn push(&self, socket: &mut Socket) -> Result<(), Disconnected> {
        let Ok(changes) = guard(&self.vault).collect_outgoing(&self.graph) else {
            return Ok(());
        };
        if changes.is_empty() {
            return Ok(());
        }
        self.send(socket, &ClientMsg::Push { changes }).await
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
