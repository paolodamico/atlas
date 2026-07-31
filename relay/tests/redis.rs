//! Full Redis-backed relay workflow, driven against a real Redis spun up in a
//! throwaway container via testcontainers.
#![expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]

use atlas_relay::{Cursor, RedisStore, Store, SyncRequest, SyncResponse, router};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::Request;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// Starts a Redis container and returns it alongside a store connected to it.
/// The container is kept alive by the returned handle; dropping it stops Redis.
async fn redis_store() -> (ContainerAsync<Redis>, RedisStore) {
    let node = Redis::default().start().await.unwrap();
    let host = node.get_host().await.unwrap();
    let port = node.get_host_port_ipv4(REDIS_PORT).await.unwrap();
    let store = RedisStore::connect(&format!("redis://{host}:{port}"))
        .await
        .unwrap();
    (node, store)
}

async fn sync(app: Router, graph: &str, req: &SyncRequest) -> SyncResponse {
    let mut body = Vec::new();
    ciborium::into_writer(req, &mut body).unwrap();
    let request = Request::builder()
        .method("POST")
        .uri(format!("/graphs/{graph}/sync"))
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(request).await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    ciborium::from_reader(bytes.as_ref()).unwrap()
}

fn req(since_seq: Option<u64>, changes: Vec<&[u8]>) -> SyncRequest {
    SyncRequest {
        since: since_seq.map(|seq| Cursor { epoch: 0, seq }),
        changes: changes.into_iter().map(<[u8]>::to_vec).collect(),
    }
}

#[tokio::test]
async fn full_sync_workflow() {
    let (_node, store) = redis_store().await;
    let app = router(store);

    // First sync appends and echoes everything back.
    let r1 = sync(app.clone(), "g", &req(None, vec![b"a", b"b"])).await;
    assert_eq!(r1.cursor.seq, 2);
    assert_eq!(r1.changes, vec![b"a".to_vec(), b"b".to_vec()]);

    // Caught up: nothing new after the cursor.
    let r2 = sync(app.clone(), "g", &req(Some(r1.cursor.seq), vec![])).await;
    assert_eq!(r2.cursor.seq, 2);
    assert!(r2.changes.is_empty());

    // Appending advances the head and returns only the new change.
    let r3 = sync(app.clone(), "g", &req(Some(r2.cursor.seq), vec![b"c"])).await;
    assert_eq!(r3.cursor.seq, 3);
    assert_eq!(r3.changes, vec![b"c".to_vec()]);

    // Graphs are independent in Redis.
    let other = sync(app.clone(), "g2", &req(None, vec![])).await;
    assert_eq!(other.cursor.seq, 0);
    assert!(other.changes.is_empty());
}

#[tokio::test]
async fn log_persists_across_reconnects() {
    let (node, store) = redis_store().await;
    store
        .append_and_read("g", vec![b"x".to_vec(), b"y".to_vec()], 0)
        .await
        .unwrap();

    // A fresh connection to the same Redis sees the persisted log.
    let host = node.get_host().await.unwrap();
    let port = node.get_host_port_ipv4(REDIS_PORT).await.unwrap();
    let reconnected = RedisStore::connect(&format!("redis://{host}:{port}"))
        .await
        .unwrap();
    let (changes, head) = reconnected.append_and_read("g", vec![], 0).await.unwrap();
    assert_eq!(head, 2);
    assert_eq!(changes, vec![b"x".to_vec(), b"y".to_vec()]);
}

#[tokio::test]
async fn append_fans_out_live_and_persists_for_late_joiners() {
    let (_node, store) = redis_store().await;

    // Subscribe with an empty catch-up, then append through a clone of the store
    // (as the websocket push path does).
    let (mut updates, catchup, head) = store.subscribe_and_read("g", 0).await.unwrap();
    assert!(catchup.is_empty());
    assert_eq!(head, 0);

    store
        .clone()
        .append("g", vec![b"a".to_vec(), b"b".to_vec()])
        .await
        .unwrap();

    // The live subscriber receives exactly the appended batch, in order.
    let batch = updates.recv().await.unwrap();
    assert_eq!(batch, vec![b"a".to_vec(), b"b".to_vec()]);

    // A late joiner gets the same data as durable catch-up read from Redis.
    let (_late, catchup, head) = store.subscribe_and_read("g", 0).await.unwrap();
    assert_eq!(head, 2);
    assert_eq!(catchup, vec![b"a".to_vec(), b"b".to_vec()]);
}

#[tokio::test]
async fn concurrent_appends_stay_atomic() {
    let (_node, store) = redis_store().await;
    let handles: Vec<_> = (0..8u8)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move { store.append_and_read("g", vec![vec![i]], 0).await.unwrap() })
        })
        .collect();
    for handle in handles {
        handle.await.unwrap();
    }

    let (all, head) = store.append_and_read("g", Vec::new(), 0).await.unwrap();
    assert_eq!(head, 8);
    assert_eq!(all.len() as u64, head);
}
