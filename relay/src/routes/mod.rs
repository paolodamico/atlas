//! HTTP routes, one module per route.
use axum::routing::post;

pub(crate) mod sync;

use axum::Router;

use crate::store::Store;

pub fn router<S: Store>() -> Router {
    Router::new().route("/graphs/{graph}/sync", post(sync::handler::<S>))
}
