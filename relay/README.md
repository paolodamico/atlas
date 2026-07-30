# atlas-relay

A dumb, zero-knowledge sync relay for Atlas. It stores opaque change blobs in an append-only log per graph and returns whatever a client is missing since its cursor. Dumb because it has zero access to the actual data, all content is end-to-end encrypted between clients.

## Configuration

- `REDIS_URL` — Redis connection string for blob storage (default `redis://127.0.0.1:6379`). Each graph is stored as one Redis list; appends and reads run in a single atomic Lua script.
- `RELAY_ADDR` — address to bind (default `127.0.0.1:4000`).

## Testing

Unit and HTTP-level tests use an in-memory store and need no external services. The Redis integration tests (`tests/redis.rs`) spin up a throwaway Redis via testcontainers and require a running Docker daemon.
