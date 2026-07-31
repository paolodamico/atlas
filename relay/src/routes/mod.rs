//! HTTP routes, one module per route.
use axum::routing::{get, post};

pub(crate) mod sync;
pub(crate) mod ws;

use axum::Router;

use crate::store::Store;

pub fn router<S: Store>() -> Router {
    Router::new()
        .route("/graphs/{graph}/sync", post(sync::handler::<S>))
        .route("/graphs/{graph}/ws", get(ws::handler::<S>))
}
