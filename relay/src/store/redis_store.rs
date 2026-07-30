//! Redis-backed append-only log: one Redis list per graph.
//!
//! # Data model
//!
//! Each graph maps to a single Redis key holding a [Redis list]:
//!
//! ```text
//! key:   atlas:graph:{graph}:log        (see `log_key`)
//! type:  list
//! value: [ blob_0, blob_1, blob_2, ... ]
//!          └── each element is one opaque, end-to-end-encrypted change blob,
//!              stored as a raw Redis bulk string (arbitrary bytes)
//! ```
//!
//! - **Key.** `atlas:graph:{graph}:log`, where `{graph}` is the caller-supplied
//!   graph id.
//! - **Value.** A list appended to with `RPUSH`, so element order is insertion
//!   order and never changes. The relay treats each element as opaque bytes; it
//!   never parses or decrypts them.
//! - **Cursor / sequence.** A client's cursor is a 0-based index into this list.
//!   `LRANGE key <cursor> -1` returns everything the client is missing, and
//!   `LLEN key` is the new head (the count of elements = the next cursor).
//!
//!  TODO: The list is never popped or reordered YET, until snapshot/epoch rollover.

use redis::aio::ConnectionManager;

use crate::store::{Store, StoreError};

// FIXME: TTL, clean up beyond snapshots.

/// Redis-backed [`Store`]. Cheap to clone: shares one multiplexed, self-healing
/// connection.
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
}

impl RedisStore {
    /// Connects to Redis at `url`, e.g. `redis://127.0.0.1:6379`.
    ///
    /// # Errors
    /// Returns [`StoreError`] if the URL is malformed or the initial connection
    /// cannot be established.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let client = redis::Client::open(url)?;
        let conn = client.get_connection_manager().await?;
        Ok(Self { conn })
    }
}

/// Redis key of the list holding `graph`'s append-only change log.
///
/// Format: `atlas:graph:{graph}:log`. See the [module docs](self) for the full
/// key/value layout.
fn log_key(graph: &str) -> String {
    format!("atlas:graph:{graph}:log")
}

impl Store for RedisStore {
    async fn append_and_read(
        &self,
        graph: &str,
        blobs: Vec<Vec<u8>>,
        from: u64,
    ) -> Result<(Vec<Vec<u8>>, u64), StoreError> {
        let key = log_key(graph);
        let from = isize::try_from(from).unwrap_or(isize::MAX);

        // Use a Redis pipe for atomic operations (same as the `MemStore`).
        let mut txn = redis::pipe();
        txn.atomic();
        if !blobs.is_empty() {
            txn.rpush(&key, blobs).ignore();
        }
        txn.lrange(&key, from, -1);
        txn.llen(&key);

        let mut conn = self.conn.clone();
        let (changes, head): (Vec<Vec<u8>>, u64) = txn.query_async(&mut conn).await?;
        Ok((changes, head))
    }
}
