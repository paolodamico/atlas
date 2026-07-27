//! HTTP routes, one module per route.
use axum::routing::{get, post};

pub(crate) mod sync;
pub(crate) mod ws;

use axum::Router;

pub fn router() -> Router {
    Router::new()
        .route("/graphs/{graph}/sync", post(sync::handler))
        .route("/graphs/{graph}/ws", get(ws::handler))
}
