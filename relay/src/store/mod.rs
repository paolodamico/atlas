//! Blob storage backends: an append-only log of opaque change blobs per graph.
//!
//! [`RedisStore`] is the production backend. [`MemStore`] is an in-memory
//! backend kept for tests.

mod mem;
mod redis_store;

use std::future::Future;

pub use mem::MemStore;
pub use redis_store::RedisStore;

/// Failure from a storage backend.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The Redis backend returned an error.
    #[error("redis store error: {0}")]
    Redis(#[from] redis::RedisError),
}

/// An append-only log of opaque blobs per graph.
///
/// Implementations MUST apply [`append_and_read`](Store::append_and_read)
/// atomically per graph: a concurrent append must not slip between the read and
/// the head and advance a client's cursor past changes it never received.
pub trait Store: Clone + Send + Sync + 'static {
    /// Appends `blobs` to `graph`, then returns its blobs at or after `from`
    /// together with the new head sequence.
    fn append_and_read(
        &self,
        graph: &str,
        blobs: Vec<Vec<u8>>,
        from: u64,
    ) -> impl Future<Output = Result<(Vec<Vec<u8>>, u64), StoreError>> + Send;
}
