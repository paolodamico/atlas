//! Blob storage backends: an append-only log of opaque change blobs per graph,
//! plus live update propagation to websocket subscribers.
//!
//! [`RedisStore`] is the production backend. [`MemStore`] is an in-memory
//! backend kept for tests.

mod mem;
mod redis_store;

use std::future::Future;

use tokio::sync::broadcast;

pub use mem::MemStore;
pub use redis_store::RedisStore;

/// Maximum buffered updates per graph before slow subscribers lag and re-read.
pub(crate) const FANOUT_CAPACITY: usize = 128;

/// A batch of change blobs; each blob is one opaque, end-to-end-encrypted change.
pub type Changes = Vec<Vec<u8>>;

/// Failure from a storage backend.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The Redis backend returned an error.
    #[error("redis store error: {0}")]
    Redis(#[from] redis::RedisError),
}

/// An append-only log of opaque blobs per graph, with live fanout to
/// subscribers.
///
/// Implementations MUST keep each graph's operations serialized so a concurrent
/// append can't slip between a read and its head, nor between a subscribe and
/// its catch-up read: subscribers must receive exactly the blobs appended after
/// the head they were handed, contiguous and gap-free.
pub trait Store: Clone + Send + Sync + 'static {
    /// Appends `blobs` to `graph`, then returns its blobs at or after `from`
    /// together with the new head sequence. Also fans the appended blobs out to
    /// live subscribers.
    fn append_and_read(
        &self,
        graph: &str,
        blobs: Changes,
        from: u64,
    ) -> impl Future<Output = Result<(Changes, u64), StoreError>> + Send;

    /// Appends `blobs` to `graph` and fans them out to live subscribers. The
    /// pusher sees its own changes echoed back through its subscription (with
    /// the new head), so there is nothing to return here.
    fn append(
        &self,
        graph: &str,
        blobs: Changes,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Subscribes to `graph`'s live updates and reads catch-up from `from`,
    /// atomically. The subscriber then receives exactly the blobs appended
    /// after the returned head, contiguous and with no overlap, so it can track
    /// its head by counting them.
    fn subscribe_and_read(
        &self,
        graph: &str,
        from: u64,
    ) -> impl Future<Output = Result<(broadcast::Receiver<Changes>, Changes, u64), StoreError>> + Send;
}
