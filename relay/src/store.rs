//! In-memory log store plus update propagation to all subscribers.
//!
//! Each graph is a `Vec` of opaque blobs and a broadcast channel. Appending
//! extends the log and publishes the appended blobs under one lock, so
//! websocket subscribers receive them in log order with no gaps.
//!
//! TODO: Redis backend

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

/// Maximum subscriber capacity per graph.
const FANOUT_CAPACITY: usize = 128;

/// A batch of change blobs (each blob is one opaque change).
type Changes = Vec<Vec<u8>>;

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

/// In-memory log store, cheap to clone (shared inner)
#[derive(Clone, Default)]
pub struct MemStore {
    graphs: Arc<Mutex<HashMap<String, GraphLog>>>,
}

impl MemStore {
    /// Appends `blobs` to `graph`, then returns its blobs at or after `from`
    /// with the new head. One lock spans append, publish, read, and head, so a
    /// concurrent append can't reorder or advance a cursor past
    /// changes a client never received.
    #[must_use]
    pub fn append_and_read(&self, graph: &str, blobs: Changes, from: u64) -> (Changes, u64) {
        let mut graphs = self.lock();
        let log = graphs.entry(graph.to_string()).or_default();
        let head = log.append(blobs);
        (log.read_from(from), head)
    }

    /// Appends `blobs` to `graph`. Websocket pushers get the appended blobs
    /// back through their subscription (with the new head), so no return here.
    pub fn append(&self, graph: &str, blobs: Changes) {
        self.lock()
            .entry(graph.to_string())
            .or_default()
            .append(blobs);
    }

    /// Returns `graph`'s blobs at or after `from`, with the current head.
    #[must_use]
    pub fn read_from(&self, graph: &str, from: u64) -> (Changes, u64) {
        let graphs = self.lock();
        graphs
            .get(graph)
            .map_or_else(|| (Vec::new(), 0), |log| (log.read_from(from), log.head()))
    }

    /// Subscribes to `graph`'s live updates and reads catch-up from `from`,
    /// both under one lock. Subscribing and reading atomically means the
    /// subscriber receives exactly the blobs appended after `head`, contiguous
    /// with no overlap, so it can track its head by counting them.
    #[must_use]
    pub fn subscribe_and_read(
        &self,
        graph: &str,
        from: u64,
    ) -> (broadcast::Receiver<Changes>, Changes, u64) {
        let mut graphs = self.lock();
        let log = graphs.entry(graph.to_string()).or_default();
        let updates = log.updates.subscribe();
        (updates, log.read_from(from), log.head())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, GraphLog>> {
        self.graphs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
                std::thread::spawn(move || store.append_and_read("g", vec![vec![i]], 0))
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let (all, head) = store.append_and_read("g", Vec::new(), 0);
        assert_eq!(head, 8);
        assert_eq!(all.len() as u64, head);
    }
}
