//! In-memory blob storage: an append-only log per graph.
//!
//! TODO: Redis backend

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Append-only log of opaque blobs per graph. Cheap to clone.
#[derive(Clone, Default)]
pub struct MemStore {
    logs: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl MemStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<Vec<u8>>>> {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Appends `blobs` to `graph`'s log.
    pub fn append(&self, graph: &str, blobs: Vec<Vec<u8>>) {
        self.lock()
            .entry(graph.to_string())
            .or_default()
            .extend(blobs);
    }

    /// Returns `graph`'s blobs at or after sequence `from`.
    #[must_use]
    pub fn read_from(&self, graph: &str, from: u64) -> Vec<Vec<u8>> {
        let from = usize::try_from(from).unwrap_or(usize::MAX);
        self.lock()
            .get(graph)
            .map_or_else(Vec::new, |log| log.iter().skip(from).cloned().collect())
    }

    /// Returns `graph`'s length (its next sequence number).
    #[must_use]
    pub fn head(&self, graph: &str) -> u64 {
        u64::try_from(self.lock().get(graph).map_or(0, Vec::len)).unwrap_or(u64::MAX)
    }
}
