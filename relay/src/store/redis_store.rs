//! Redis-backed append-only log: one Redis list per graph, with in-process
//! live fanout to websocket subscribers.
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
//!
//! # Live fanout
//!
//! The durable log lives in Redis; live updates are broadcast **in-process**, so
//! subscribers see appends made through this relay node. Cross-node fanout
//! (several relays sharing one Redis) needs Redis pub/sub and is a follow-up. On
//! restart, subscribers reconnect and catch up from the durable Redis log.
//!
//! [Redis list]: https://redis.io/docs/latest/develop/data-types/lists/

use std::collections::HashMap;
use std::sync::Arc;

use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, broadcast};

use crate::store::{Changes, FANOUT_CAPACITY, Store, StoreError};

// FIXME: TTL, clean up beyond snapshots.

/// Per-graph broadcast senders for in-process live fanout.
type Fanout = Arc<Mutex<HashMap<String, broadcast::Sender<Changes>>>>;

/// Redis-backed [`Store`]. Cheap to clone: shares one multiplexed, self-healing
/// connection and the in-process fanout map.
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
    fanout: Fanout,
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
        Ok(Self {
            conn,
            fanout: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Reads `graph`'s blobs at or after `from` with the new head, in one atomic
    /// `LRANGE`/`LLEN` so the head matches the returned range.
    async fn read_from(&self, key: &str, from: u64) -> Result<(Changes, u64), StoreError> {
        let from = isize::try_from(from).unwrap_or(isize::MAX);
        let mut txn = redis::pipe();
        txn.atomic();
        txn.lrange(key, from, -1);
        txn.llen(key);
        let mut conn = self.conn.clone();
        Ok(txn.query_async(&mut conn).await?)
    }
}

/// Redis key of the list holding `graph`'s append-only change log.
///
/// Format: `atlas:graph:{graph}:log`. See the [module docs](self) for the full
/// key/value layout.
fn log_key(graph: &str) -> String {
    format!("atlas:graph:{graph}:log")
}

/// Returns `graph`'s broadcast sender, creating it on first use.
fn channel<'a>(
    fanout: &'a mut HashMap<String, broadcast::Sender<Changes>>,
    graph: &str,
) -> &'a broadcast::Sender<Changes> {
    fanout
        .entry(graph.to_string())
        .or_insert_with(|| broadcast::channel(FANOUT_CAPACITY).0)
}

impl Store for RedisStore {
    async fn append_and_read(
        &self,
        graph: &str,
        blobs: Changes,
        from: u64,
    ) -> Result<(Changes, u64), StoreError> {
        let key = log_key(graph);
        let from = isize::try_from(from).unwrap_or(isize::MAX);

        // Hold the fanout lock across the Redis round-trip so the publish below
        // is ordered against a concurrent `subscribe_and_read`, keeping
        // subscribers contiguous with no gap or overlap. tokio's async mutex is
        // safe to hold across `.await`. The MULTI/EXEC keeps the Redis side
        // atomic on its own.
        let mut fanout = self.fanout.lock().await;
        let mut txn = redis::pipe();
        txn.atomic();
        if !blobs.is_empty() {
            txn.rpush(&key, &blobs).ignore();
        }
        txn.lrange(&key, from, -1);
        txn.llen(&key);
        let mut conn = self.conn.clone();
        let (changes, head): (Changes, u64) = txn.query_async(&mut conn).await?;

        if !blobs.is_empty() {
            let _ = channel(&mut fanout, graph).send(blobs);
        }
        Ok((changes, head))
    }

    async fn append(&self, graph: &str, blobs: Changes) -> Result<(), StoreError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let key = log_key(graph);
        let mut fanout = self.fanout.lock().await;
        let mut conn = self.conn.clone();
        redis::pipe()
            .rpush(&key, &blobs)
            .ignore()
            .query_async::<()>(&mut conn)
            .await?;

        let _ = channel(&mut fanout, graph).send(blobs);
        Ok(())
    }

    async fn subscribe_and_read(
        &self,
        graph: &str,
        from: u64,
    ) -> Result<(broadcast::Receiver<Changes>, Changes, u64), StoreError> {
        let key = log_key(graph);
        // Subscribe before reading catch-up, both under the fanout lock, so no
        // append lands between the two: the subscription then carries exactly
        // the blobs appended after the returned head.
        let mut fanout = self.fanout.lock().await;
        let updates = channel(&mut fanout, graph).subscribe();
        let (changes, head) = self.read_from(&key, from).await?;
        Ok((updates, changes, head))
    }
}
