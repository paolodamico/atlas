//! `POST /graphs/{graph}/sync`: append the client's changes, then return
//! everything in the graph after the client's cursor.

use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::store::Store;

/// Position in a graph's append-only log; seq restarts each snapshot `epoch`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Cursor {
    /// Snapshot generation.
    pub epoch: u64,
    /// Position within the epoch.
    pub seq: u64,
}

/// Client to relay: append `changes`, then send back everything after `since`.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SyncRequest {
    /// The client's current cursor, or `None` on first sync.
    pub since: Option<Cursor>,
    /// Opaque change blobs to append.
    pub changes: Vec<Vec<u8>>,
}

/// Relay to client: changes since the cursor, plus the new cursor.
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SyncResponse {
    /// Opaque change blobs the client was missing.
    pub changes: Vec<Vec<u8>>,
    /// The client's new cursor.
    pub cursor: Cursor,
    /// Present only when the client was behind the snapshot cut.
    pub snapshot: Option<Vec<u8>>,
}

#[instrument(skip(store, body), fields(graph = %graph, bytes = body.len()))]
pub(super) async fn handler<S: Store>(
    Path(graph): Path<String>,
    Extension(store): Extension<S>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    let request: SyncRequest =
        ciborium::from_reader(body.as_ref()).map_err(|_| StatusCode::BAD_REQUEST)?;
    let pushed = request.changes.len();
    let from = request.since.map_or(0, |c| c.seq);
    let (changes, seq) = store
        .append_and_read(&graph, request.changes, from)
        .await
        .map_err(|e| {
            tracing::error!("store error syncing graph: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let response = SyncResponse {
        changes,
        cursor: Cursor { epoch: 0, seq },
        snapshot: None,
    };
    tracing::info!(pushed, pulled = response.changes.len(), "synced graph");

    let mut buf = Vec::new();
    ciborium::into_writer(&response, &mut buf).map_err(|e| {
        tracing::error!("Unexpected error serializing SyncResponse to CBOR: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(buf)
}
