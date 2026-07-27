//! Relay HTTP behaviour, driven in-process with tower's `oneshot`.
#![expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]

use atlas_relay::{Cursor, MemStore, SyncRequest, SyncResponse, router};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::Request;
use tower::ServiceExt;

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
async fn append_returns_everything_since_cursor() {
    let app = router(MemStore::default());

    let r1 = sync(app.clone(), "g", &req(None, vec![b"a", b"b"])).await;
    assert_eq!(r1.cursor.seq, 2);
    assert_eq!(r1.changes, vec![b"a".to_vec(), b"b".to_vec()]);

    let r2 = sync(app.clone(), "g", &req(Some(r1.cursor.seq), vec![])).await;
    assert_eq!(r2.cursor.seq, 2);
    assert!(r2.changes.is_empty());

    let r3 = sync(app.clone(), "g", &req(Some(r2.cursor.seq), vec![b"c"])).await;
    assert_eq!(r3.cursor.seq, 3);
    assert_eq!(r3.changes, vec![b"c".to_vec()]);
}

#[tokio::test]
async fn graphs_are_independent() {
    let app = router(MemStore::default());
    sync(app.clone(), "g1", &req(None, vec![b"x"])).await;
    let other = sync(app.clone(), "g2", &req(None, vec![])).await;
    assert_eq!(other.cursor.seq, 0);
    assert!(other.changes.is_empty());
}
