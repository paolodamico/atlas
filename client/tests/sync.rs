//! End-to-end: an edit on one client reaches another through a live relay.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "tests read better with unwrap/expect"
)]

use std::time::Duration;

use atlas_client::{Client, Event, SyncStatus};
use tokio::sync::broadcast::Receiver;

async fn wait_live(events: &mut Receiver<Event>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(Event::Status(SyncStatus::Live)) = events.recv().await {
                return;
            }
        }
    })
    .await
    .expect("client did not reach Live");
}

async fn start_relay() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = atlas_relay::router(atlas_relay::MemStore::default());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_edit_on_one_client_reaches_another() {
    let relay = start_relay().await;
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = Client::open(dir_a.path()).unwrap();
    let b = Client::open(dir_b.path()).unwrap();
    let mut a_events = a.subscribe();
    let mut b_events = b.subscribe();

    a.connect(relay.as_str(), "demo");
    b.connect(relay.as_str(), "demo");
    wait_live(&mut a_events).await;
    wait_live(&mut b_events).await;

    let id = a.create_note("n.md", "N", "hello").unwrap();

    // The edit reaches B over the relay.
    let received = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(Event::Note { id, body }) = b_events.recv().await
                && body == "hello"
            {
                return id;
            }
        }
    })
    .await
    .expect("B did not receive the note");

    assert_eq!(received, id);
    assert_eq!(b.note_body(&id).unwrap(), "hello");
}
