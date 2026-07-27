//! HTTP transport to a relay, implementing [`atlas_core::Transport`].
//!
//! The wire body is CBOR; these types must match the relay's shape.

use atlas_core::{Cursor, Delta, Transport, TransportError};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct SyncRequest {
    since: Option<Cursor>,
    changes: Vec<Vec<u8>>,
}

#[derive(Deserialize)]
struct SyncResponse {
    changes: Vec<Vec<u8>>,
    cursor: Cursor,
    snapshot: Option<Vec<u8>>,
}

/// Syncs with a relay over HTTP at `base_url` (e.g. `http://127.0.0.1:4000`).
pub struct RelayTransport {
    base_url: String,
}

impl RelayTransport {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl Transport for RelayTransport {
    fn sync(
        &mut self,
        graph: &str,
        since: Option<Cursor>,
        outgoing: Vec<Vec<u8>>,
    ) -> Result<Delta, TransportError> {
        let request = SyncRequest {
            since,
            changes: outgoing,
        };
        let mut body = Vec::new();
        ciborium::into_writer(&request, &mut body).map_err(|e| TransportError(e.to_string()))?;
        let url = format!("{}/graphs/{graph}/sync", self.base_url);
        let mut response = ureq::post(&url)
            .send(body)
            .map_err(|e| TransportError(e.to_string()))?;
        let bytes = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| TransportError(e.to_string()))?;
        let response: SyncResponse =
            ciborium::from_reader(bytes.as_slice()).map_err(|e| TransportError(e.to_string()))?;
        Ok(Delta {
            changes: response.changes,
            cursor: response.cursor,
            snapshot: response.snapshot,
        })
    }
}
