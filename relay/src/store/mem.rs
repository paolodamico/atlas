//! In-memory log store plus update propagation to all subscribers.
//!
//! Kept for tests that do not need a running Redis. Each graph is a `Vec` of
//! opaque blobs and a broadcast channel; appending extends the log and
//! publishes the appended blobs under one lock, so websocket subscribers
//! receive them in log order with no gaps.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::store::{Changes, FANOUT_CAPACITY, Store, StoreError};

/// One graph's durable log and its live broadcast tail. The broadcast carries
/// just the newly appended blobs; each subscriber tracks its own head by
/// counting them, since delivery is contiguous and gap-free.
struct GraphLog {
    /// The graph's list of changes.
    changes: Changes,
    /// The subscribers' broadcast channel, fed the blobs of each append.
    updates: broadcast::Sender<Changes>,
}

impl Default for GraphLog {
    fn default() -> Self {
        Self {
            changes: Vec::new(),
            updates: broadcast::channel(FANOUT_CAPACITY).0,
        }
    }
}

impl GraphLog {
    fn append(&mut self, blobs: Changes) -> u64 {
        if !blobs.is_empty() {
            self.changes.extend(blobs.iter().cloned());
            let _ = self.updates.send(blobs);
        }
        self.head()
    }

    fn read_from(&self, from: u64) -> Changes {
        let from = usize::try_from(from).unwrap_or(usize::MAX);
        self.changes.iter().skip(from).cloned().collect()
    }

    fn head(&self) -> u64 {
        u64::try_from(self.changes.len()).unwrap_or(u64::MAX)
    }
}

/// In-memory log store, cheap to clone (shared inner).
#[derive(Clone, Default)]
pub struct MemStore {
    graphs: Arc<Mutex<HashMap<String, GraphLog>>>,
}

impl MemStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, GraphLog>> {
        self.graphs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Store for MemStore {
    async fn append_and_read(
        &self,
        graph: &str,
        blobs: Changes,
        from: u64,
    ) -> Result<(Changes, u64), StoreError> {
        let mut graphs = self.lock();
        let log = graphs.entry(graph.to_string()).or_default();
        let head = log.append(blobs);
        Ok((log.read_from(from), head))
    }

    async fn append(&self, graph: &str, blobs: Changes) -> Result<(), StoreError> {
        self.lock()
            .entry(graph.to_string())
            .or_default()
            .append(blobs);
        Ok(())
    }

    async fn subscribe_and_read(
        &self,
        graph: &str,
        from: u64,
    ) -> Result<(broadcast::Receiver<Changes>, Changes, u64), StoreError> {
        let mut graphs = self.lock();
        let log = graphs.entry(graph.to_string()).or_default();
        let updates = log.updates.subscribe();
        Ok((updates, log.read_from(from), log.head()))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::MemStore;
    use crate::store::Store;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_all_land_and_stay_consistent() {
        let store = MemStore::default();
        let handles: Vec<_> = (0..8u8)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move { store.append_and_read("g", vec![vec![i]], 0).await })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        let (all, head) = store.append_and_read("g", Vec::new(), 0).await.unwrap();
        assert_eq!(head, 8);
        assert_eq!(all.len() as u64, head);
    }
}
