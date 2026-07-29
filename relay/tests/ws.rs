//! Websocket live sync: a push fans out to every subscriber on the graph.
#![expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Serialize, Deserialize)]
struct Cursor {
    epoch: u64,
    seq: u64,
}

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

async fn start_relay() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = atlas_relay::router(atlas_relay::MemStore::default());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}/graphs/g/ws")
}

async fn send_msg(ws: &mut Ws, msg: &ClientMsg) {
    let mut body = Vec::new();
    ciborium::into_writer(msg, &mut body).unwrap();
    ws.send(Message::binary(body)).await.unwrap();
}

async fn recv_sync(ws: &mut Ws) -> ServerMsg {
    loop {
        if let Message::Binary(bytes) = ws.next().await.unwrap().unwrap() {
            return ciborium::from_reader(bytes.as_ref()).unwrap();
        }
    }
}

#[tokio::test]
async fn push_fans_out_to_all_subscribers() {
    let url = start_relay().await;
    let (mut a, _) = connect_async(url.as_str()).await.unwrap();
    let (mut b, _) = connect_async(url.as_str()).await.unwrap();

    // Both connect and drain their (empty) catch-up. After the catch-up arrives
    // each connection has already subscribed, so neither can miss the push.
    send_msg(&mut a, &ClientMsg::Hello { since: None }).await;
    send_msg(&mut b, &ClientMsg::Hello { since: None }).await;
    assert!(recv_sync(&mut a).await.changes.is_empty());
    assert!(recv_sync(&mut b).await.changes.is_empty());

    send_msg(
        &mut a,
        &ClientMsg::Push {
            changes: vec![b"x".to_vec()],
        },
    )
    .await;

    let from_a = recv_sync(&mut a).await;
    let from_b = recv_sync(&mut b).await;
    assert_eq!(from_a.changes, vec![b"x".to_vec()]);
    assert_eq!(from_b.changes, vec![b"x".to_vec()]);
    assert_eq!(from_a.cursor.seq, 1);
    assert_eq!(from_b.cursor.seq, 1);
}

#[tokio::test]
async fn catch_up_replays_existing_changes() {
    let url = start_relay().await;
    let (mut a, _) = connect_async(url.as_str()).await.unwrap();
    send_msg(&mut a, &ClientMsg::Hello { since: None }).await;
    recv_sync(&mut a).await;
    send_msg(
        &mut a,
        &ClientMsg::Push {
            changes: vec![b"one".to_vec()],
        },
    )
    .await;
    recv_sync(&mut a).await;

    // A late joiner with no cursor gets the existing change as catch-up.
    let (mut b, _) = connect_async(url.as_str()).await.unwrap();
    send_msg(&mut b, &ClientMsg::Hello { since: None }).await;
    assert_eq!(recv_sync(&mut b).await.changes, vec![b"one".to_vec()]);
}

#[tokio::test]
async fn large_catch_up_is_chunked_with_advancing_cursors() {
    let url = start_relay().await;
    let (mut a, _) = connect_async(url.as_str()).await.unwrap();
    send_msg(&mut a, &ClientMsg::Hello { since: None }).await;
    recv_sync(&mut a).await;

    // More than one chunk's worth of changes (chunk size is 256).
    let all: Vec<Vec<u8>> = (0..600usize).map(|i| i.to_le_bytes().to_vec()).collect();
    send_msg(
        &mut a,
        &ClientMsg::Push {
            changes: all.clone(),
        },
    )
    .await;
    // The live fan-out echoes the whole batch in one frame at the final head.
    assert_eq!(recv_sync(&mut a).await.cursor.seq, 600);

    // A late joiner receives the backlog across several bounded frames whose
    // cursors advance to exactly the head, with no gap or overlap.
    let (mut b, _) = connect_async(url.as_str()).await.unwrap();
    send_msg(&mut b, &ClientMsg::Hello { since: None }).await;
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut frames = 0;
    while got.len() < all.len() {
        let msg = recv_sync(&mut b).await;
        assert!(!msg.changes.is_empty() && msg.changes.len() <= 256);
        got.extend(msg.changes);
        // The cursor is the absolute seq of the last change in this frame.
        assert_eq!(usize::try_from(msg.cursor.seq).unwrap(), got.len());
        frames += 1;
    }
    assert_eq!(got, all);
    assert_eq!(frames, 3, "600 changes should arrive as 256 + 256 + 88");
}
