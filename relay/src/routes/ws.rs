//! `GET /graphs/{graph}/ws`: live sync over a websocket.
//!
//! The client sends a `Hello` with its cursor, then `Push` frames as it makes
//! changes. The server replies with `Sync` frames: a catch-up from the cursor,
//! then each batch fanned out from the graph as clients push. Frames are CBOR.

use std::ops::ControlFlow;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Extension, Path};
use axum::response::Response;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;

use super::sync::Cursor;
use crate::store::MemStore;

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
    let (mut updates, changes, mut head) =
        store.subscribe_and_read(&graph, since.map_or(0, |c| c.seq));
    if send_sync(&mut socket, changes, head).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                if handle_incoming(incoming, &graph, &store).is_break() {
                    break;
                }
            }
            update = updates.recv() => {
                if forward(update, &mut socket, &graph, &store, &mut head).await.is_break() {
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

async fn forward(
    batch: Result<Vec<Vec<u8>>, RecvError>,
    socket: &mut WebSocket,
    graph: &str,
    store: &MemStore,
    head: &mut u64,
) -> ControlFlow<()> {
    let changes = match batch {
        // Delivered in order with no gaps, so this batch starts at `head`.
        Ok(changes) => {
            *head += u64::try_from(changes.len()).unwrap_or(0);
            changes
        }
        // Fell behind the fan-out ring; re-read the missed tail from the log.
        Err(RecvError::Lagged(_)) => {
            let (missed, new_head) = store.read_from(graph, *head);
            *head = new_head;
            missed
        }
        Err(RecvError::Closed) => return ControlFlow::Break(()),
    };
    if send_sync(socket, changes, *head).await.is_err() {
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
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
