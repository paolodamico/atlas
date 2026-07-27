//! HTTP routes, one module per route.
use axum::routing::post;

pub(crate) mod sync;

use axum::Router;

pub fn router() -> Router {
    Router::new().route("/graphs/{graph}/sync", post(sync::handler))
}
