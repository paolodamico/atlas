#![doc = include_str!("../README.md")]

mod routes;
mod store;

use axum::{Extension, Router};

pub use routes::sync::{Cursor, SyncRequest, SyncResponse};
pub use store::{MemStore, RedisStore, Store, StoreError};

/// Build the main router backed by `store`.
pub fn router<S: Store>(store: S) -> Router {
    routes::router::<S>().layer(Extension(store))
}
