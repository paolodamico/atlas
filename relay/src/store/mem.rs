//! In-memory append-only log, kept for tests that do not need a running Redis.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::store::{Store, StoreError};

/// Append-only log of opaque blobs per graph. Cheap to clone.
#[derive(Clone, Default)]
pub struct MemStore {
    logs: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl MemStore {
    /// Append, read, and head share one lock: a concurrent append can't slip
    /// between the read and the head and advance a client's cursor past
    /// changes it never received.
    fn locked_append_and_read(
        &self,
        graph: &str,
        blobs: Vec<Vec<u8>>,
        from: u64,
    ) -> (Vec<Vec<u8>>, u64) {
        let mut logs = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let log = logs.entry(graph.to_string()).or_default();
        log.extend(blobs);
        let from = usize::try_from(from).unwrap_or(usize::MAX);
        let changes = log.iter().skip(from).cloned().collect();
        let head = u64::try_from(log.len()).unwrap_or(u64::MAX);
        (changes, head)
    }
}

impl Store for MemStore {
    async fn append_and_read(
        &self,
        graph: &str,
        blobs: Vec<Vec<u8>>,
        from: u64,
    ) -> Result<(Vec<Vec<u8>>, u64), StoreError> {
        Ok(self.locked_append_and_read(graph, blobs, from))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::MemStore;

    #[test]
    fn concurrent_appends_all_land_and_stay_consistent() {
        let store = MemStore::default();
        let handles: Vec<_> = (0..8u8)
            .map(|i| {
                let store = store.clone();
                std::thread::spawn(move || store.locked_append_and_read("g", vec![vec![i]], 0))
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let (all, head) = store.locked_append_and_read("g", Vec::new(), 0);
        assert_eq!(head, 8);
        assert_eq!(all.len() as u64, head);
    }
}
