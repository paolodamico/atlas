#![doc = include_str!("../README.md")]

mod routes;
mod store;

use axum::{Extension, Router};

pub use routes::sync::{Cursor, SyncRequest, SyncResponse};
pub use store::MemStore;

/// Build the main router
pub fn router(store: MemStore) -> Router {
    routes::router().layer(Extension(store))
}
