//! `GET /graphs/{graph}/ws`: live sync over a websocket.
//!
//! The client sends a `Hello` with its cursor, then `Push` frames as it makes
//! changes. The server replies with `Sync` frames: a catch-up from the cursor,
//! then each batch fanned out from the graph as clients push. Frames are CBOR.

use std::ops::ControlFlow;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Path};
use axum::response::Response;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::interval;

use super::sync::Cursor;
use crate::store::MemStore;

/// How often to ping idle peers: keeps NAT/proxy paths warm and lets a dead
/// half-open connection surface as a failed send.
const PING_INTERVAL: Duration = Duration::from_secs(15);

/// Max payload per catch-up frame. Splitting by bytes (not change count) keeps a
/// big backlog flowing as steady frames instead of one giant message that could
/// outlast the client's read timeout.
const CATCHUP_BYTES: usize = 256 * 1024;

#[derive(Deserialize)]
enum ClientMsg {
    Hello { since: Option<Cursor> },
    Push { changes: Vec<Vec<u8>> },
}

#[derive(Serialize)]
struct ServerMsg {
    changes: Vec<Vec<u8>>,
    cursor: Cursor,
}

pub(super) async fn handler(
    upgrade: WebSocketUpgrade,
    Path(graph): Path<String>,
    Extension(store): Extension<MemStore>,
) -> Response {
    upgrade.on_upgrade(move |socket| serve(socket, graph, store))
}

async fn serve(mut socket: WebSocket, graph: String, store: MemStore) {
    let Some(since) = hello(&mut socket).await else {
        return;
    };
    // Subscribe and read catch-up atomically: the subscription then carries
    // exactly the changes appended after `head`, so counting them tracks it.
    let from = since.map_or(0, |c| c.seq);
    let (mut updates, changes, mut head) = store.subscribe_and_read(&graph, from);
    if send_catchup(&mut socket, changes, from, head)
        .await
        .is_err()
    {
        return;
    }

    let mut ping = interval(PING_INTERVAL);
    ping.tick().await;
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                if handle_incoming(incoming, &graph, &store).is_break() {
                    break;
                }
            }
            _ = ping.tick() => {
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            batch = updates.recv() => {
                let sent = match batch {
                    // Delivered in order with no gaps, so this batch starts at head.
                    Ok(changes) => {
                        head += u64::try_from(changes.len()).unwrap_or(0);
                        send_sync(&mut socket, changes, head).await
                    }
                    // Fell behind the ring: re-subscribe and re-read atomically,
                    // dropping the stale buffer and realigning head to the log.
                    Err(RecvError::Lagged(_)) => {
                        let (fresh, missed, new_head) = store.subscribe_and_read(&graph, head);
                        updates = fresh;
                        head = new_head;
                        send_sync(&mut socket, missed, head).await
                    }
                    Err(RecvError::Closed) => break,
                };
                if sent.is_err() {
                    break;
                }
            }
        }
    }
}

async fn hello(socket: &mut WebSocket) -> Option<Option<Cursor>> {
    loop {
        let message = socket.recv().await?.ok()?;
        match message {
            Message::Binary(bytes) => {
                let ClientMsg::Hello { since } = decode(&bytes)? else {
                    return None;
                };
                return Some(since);
            }
            Message::Close(_) => return None,
            _ => {}
        }
    }
}

fn handle_incoming(
    incoming: Option<Result<Message, axum::Error>>,
    graph: &str,
    store: &MemStore,
) -> ControlFlow<()> {
    match incoming {
        Some(Ok(Message::Binary(bytes))) => {
            if let Some(ClientMsg::Push { changes }) = decode(&bytes) {
                store.append(graph, changes);
            }
            ControlFlow::Continue(())
        }
        None | Some(Err(_) | Ok(Message::Close(_))) => ControlFlow::Break(()),
        Some(Ok(_)) => ControlFlow::Continue(()),
    }
}

/// Streams the catch-up backlog as frames capped at [`CATCHUP_BYTES`], each
/// carrying the seq it reaches. A change bigger than the cap can't be split, so
/// it goes in its own frame. Intermediate frames use the running seq; the last
/// carries `head`, which also covers an empty backlog.
async fn send_catchup(
    socket: &mut WebSocket,
    changes: Vec<Vec<u8>>,
    from: u64,
    head: u64,
) -> Result<(), axum::Error> {
    let mut seq = from;
    let mut batch: Vec<Vec<u8>> = Vec::new();
    let mut batch_bytes = 0usize;
    for change in changes {
        if !batch.is_empty() && batch_bytes + change.len() > CATCHUP_BYTES {
            seq += u64::try_from(batch.len()).unwrap_or(0);
            send_sync(socket, std::mem::take(&mut batch), seq).await?;
            batch_bytes = 0;
        }
        batch_bytes += change.len();
        batch.push(change);
    }
    send_sync(socket, batch, head).await
}

async fn send_sync(
    socket: &mut WebSocket,
    changes: Vec<Vec<u8>>,
    head: u64,
) -> Result<(), axum::Error> {
    let message = ServerMsg {
        changes,
        cursor: Cursor {
            epoch: 0,
            seq: head,
        },
    };
    socket.send(Message::binary(encode(&message))).await
}

fn encode(message: &ServerMsg) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = ciborium::into_writer(message, &mut buf);
    buf
}

fn decode(bytes: &[u8]) -> Option<ClientMsg> {
    ciborium::from_reader(bytes).ok()
}
