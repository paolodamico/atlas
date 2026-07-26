//! Main entrypoint for the atlas-relay

use atlas_relay::{MemStore, router};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("RELAY_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "relay listening");
    axum::serve(listener, router(MemStore::default())).await?;
    Ok(())
}
