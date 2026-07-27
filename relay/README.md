# atlas-relay

A dumb, zero-knowledge sync relay for Atlas. It stores opaque change blobs in an append-only log per graph and returns whatever a client is missing since its cursor. Dumb because it has zero access to the actual data, all content is end-to-end encrypted between clients.
