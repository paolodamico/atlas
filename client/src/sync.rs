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
use tokio_tungstenite::tungstenite::Message;
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

pub(crate) async fn run(
    vault: Arc<Mutex<Vault>>,
    events: broadcast::Sender<Event>,
    url: String,
    graph: String,
) {
    let ws_url = format!("{url}/graphs/{graph}/ws");
    let mut backoff = BACKOFF_START;
    loop {
        set_status(&events, SyncStatus::Connecting);
        if let Ok((socket, _)) = connect_async(ws_url.as_str()).await {
            backoff = BACKOFF_START;
            set_status(&events, SyncStatus::Live);
            session(socket, &vault, &events, &graph).await;
        }
        set_status(&events, SyncStatus::Offline);
        sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn session(
    mut socket: Socket,
    vault: &Mutex<Vault>,
    events: &broadcast::Sender<Event>,
    graph: &str,
) {
    let Ok(cursor) = guard(vault).sync_cursor(graph) else {
        return;
    };
    if send(&mut socket, &ClientMsg::Hello { since: cursor })
        .await
        .is_err()
    {
        return;
    }

    let mut push = interval(PUSH_INTERVAL);
    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Binary(bytes))) => {
                        if let Ok(msg) = decode(&bytes) {
                            apply(vault, events, graph, msg);
                        }
                    }
                    None | Some(Err(_) | Ok(Message::Close(_))) => return,
                    Some(Ok(_)) => {}
                }
            }
            _ = push.tick() => {
                if push_local(&mut socket, vault, graph).await.is_err() {
                    return;
                }
            }
        }
    }
}

fn apply(vault: &Mutex<Vault>, events: &broadcast::Sender<Event>, graph: &str, msg: ServerMsg) {
    let Ok(applied) = guard(vault).apply_remote(graph, msg.changes, msg.cursor) else {
        return;
    };
    emit_applied(vault, events, &applied);
}

async fn push_local(socket: &mut Socket, vault: &Mutex<Vault>, graph: &str) -> Result<(), ()> {
    let Ok(outgoing) = guard(vault).collect_outgoing(graph) else {
        return Ok(());
    };
    if outgoing.is_empty() {
        return Ok(());
    }
    send(socket, &ClientMsg::Push { changes: outgoing }).await
}

async fn send(socket: &mut Socket, msg: &ClientMsg) -> Result<(), ()> {
    let mut body = Vec::new();
    ciborium::into_writer(msg, &mut body).map_err(|_| ())?;
    socket.send(Message::binary(body)).await.map_err(|_| ())
}

fn decode(bytes: &[u8]) -> Result<ServerMsg, ()> {
    ciborium::from_reader(bytes).map_err(|_| ())
}

fn set_status(events: &broadcast::Sender<Event>, status: SyncStatus) {
    let _ = events.send(Event::Status(status));
}
