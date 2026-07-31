//! Main entrypoint for the atlas-relay

use atlas_relay::{RedisStore, router};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let store = RedisStore::connect(&redis_url).await?;

    let addr = std::env::var("RELAY_ADDR").unwrap_or_else(|_| "127.0.0.1:4000".to_string());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, %redis_url, "🌐 atlas relay listening");
    axum::serve(listener, router(store)).await?;
    Ok(())
}
