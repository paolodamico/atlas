//! Incremental sync with a dumb relay.
//!
//! A vault's docs (its root doc plus every note) are exchanged as automerge
//! changes, one opaque blob each, through a [`Transport`]. The relay stores
//! and orders blobs but can't read them. Every blob is an [`Envelope`]. Each
//! client pushes its new changes and pulls the rest since its
//! cursor.

use std::collections::{BTreeMap, BTreeSet};

use automerge::{Change, ChangeHash};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{NoteDoc, Vault, VaultError};

/// Envelope `doc_id` for the vault's root doc; note docs use their note id.
const ROOT_DOC: &str = "root";

/// Wraps blobs before they reach the relay, so the relay only ever sees
/// ciphertext. Internal: encryption is the vault's concern, not the caller's.
trait Cipher {
    fn encrypt(&self, plaintext: Vec<u8>) -> Vec<u8>;
    fn decrypt(&self, ciphertext: Vec<u8>) -> Result<Vec<u8>, CipherError>;
}

/// Identity cipher, replaced once encryption lands.
struct NoCipher;

impl Cipher for NoCipher {
    fn encrypt(&self, plaintext: Vec<u8>) -> Vec<u8> {
        plaintext
    }
    fn decrypt(&self, ciphertext: Vec<u8>) -> Result<Vec<u8>, CipherError> {
        Ok(ciphertext)
    }
}

/// A position in a graph's change log. Seq restarts each `epoch` (a snapshot
/// generation), so a stale epoch tells the relay to send a snapshot instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// Snapshot generation.
    pub epoch: u64,
    /// Position within the epoch.
    pub seq: u64,
}

/// The relay's reply to a sync: changes since the client's cursor.
pub struct Delta {
    /// Opaque change blobs the client was missing.
    pub changes: Vec<Vec<u8>>,
    /// The client's new cursor.
    pub cursor: Cursor,
    /// Present only when the client was behind the snapshot cut.
    pub snapshot: Option<Vec<u8>>,
}

/// The relay: a dumb, append-only log of opaque change blobs per graph.
pub trait Transport {
    /// Appends `outgoing` and returns everything after `since`.
    ///
    /// # Errors
    /// Returns an error if the relay is unreachable or rejects the request.
    fn sync(
        &mut self,
        graph: &str,
        since: Option<Cursor>,
        outgoing: Vec<Vec<u8>>,
    ) -> Result<Delta, TransportError>;
}

/// How many change blobs a sync moved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncOutcome {
    /// Blobs sent to the relay.
    pub pushed: usize,
    /// Blobs received and applied.
    pub pulled: usize,
}

/// What a [`Vault::apply_remote`] changed, so a caller can react (e.g. re-render).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Applied {
    /// Note ids whose body changed.
    pub notes: Vec<String>,
    /// Whether the note list (metadata) changed.
    pub list_changed: bool,
}

impl Applied {
    fn from_touched(touched: Vec<String>) -> Self {
        let mut applied = Self::default();
        for id in touched {
            if id == ROOT_DOC {
                applied.list_changed = true;
            } else {
                applied.notes.push(id);
            }
        }
        applied
    }
}

#[derive(Debug, thiserror::Error)]
#[error("decryption failed: {0}")]
struct CipherError(String);

/// A [`Transport::sync`] failure.
#[derive(Debug, thiserror::Error)]
#[error("transport error: {0}")]
pub struct TransportError(pub String);

/// Errors from [`Vault::sync`].
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// A vault operation or automerge change failed.
    #[error(transparent)]
    Vault(Box<VaultError>),
    /// A note doc operation failed.
    #[error(transparent)]
    Note(Box<crate::NoteError>),
    /// The relay call failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// A blob could not be decrypted.
    #[error("decryption failed: {0}")]
    Cipher(String),
    /// The relay returned a snapshot, which this client cannot apply yet.
    #[error("relay returned an unsupported snapshot")]
    Snapshot,
    /// A stored envelope or sync-state was not readable.
    #[error("malformed sync data: {0}")]
    Malformed(String),
}

impl From<CipherError> for SyncError {
    fn from(e: CipherError) -> Self {
        Self::Cipher(e.0)
    }
}

impl From<VaultError> for SyncError {
    fn from(e: VaultError) -> Self {
        Self::Vault(Box::new(e))
    }
}

impl From<crate::NoteError> for SyncError {
    fn from(e: crate::NoteError) -> Self {
        Self::Note(Box::new(e))
    }
}

/// Per-graph sync progress, persisted in the vault's store as CBOR.
#[derive(Default, Serialize, Deserialize)]
struct SyncState {
    cursor: Option<Cursor>,
    /// Doc id to the heads we last pushed, so we only send newer changes.
    pushed: BTreeMap<String, Heads>,
}

/// Automerge change heads, serialized as their raw 32-byte hashes.
#[derive(Default)]
struct Heads(Vec<ChangeHash>);

impl Serialize for Heads {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .iter()
            .map(|h| h.0)
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Heads {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Vec::<[u8; 32]>::deserialize(deserializer)?;
        Ok(Self(raw.into_iter().map(ChangeHash).collect()))
    }
}

impl Vault {
    /// Syncs this vault with a relay: push local changes, pull remote ones.
    ///
    /// `graph` names the shared log on the relay. Progress is persisted, so a
    /// later call only exchanges changes made since.
    ///
    /// # Errors
    /// Returns an error if a store, transport, cipher, or automerge operation
    /// fails. On failure the vault may be left partially synced.
    pub fn sync(
        &mut self,
        graph: &str,
        transport: &mut impl Transport,
    ) -> Result<SyncOutcome, SyncError> {
        let cipher = NoCipher;
        let mut state = self.load_sync_state(graph)?;
        let outgoing = self.gather_outgoing(&state, &cipher)?;
        let pushed = outgoing.len();

        let delta = transport.sync(graph, state.cursor, outgoing)?;
        if delta.snapshot.is_some() {
            // Snapshot hydration is not built yet; advancing the cursor here
            // would skip data we never applied, so fail instead of dropping it.
            return Err(SyncError::Snapshot);
        }
        let pulled = delta.changes.len();
        let applied = self.apply_incoming(delta.changes, &cipher)?;
        self.persist()?;

        state.cursor = Some(delta.cursor);
        advance_pushed(&mut state, &applied)?;
        self.save_sync_state(graph, &state)?;
        Ok(SyncOutcome { pushed, pulled })
    }

    /// The persisted cursor for `graph`, or `None` if it has never synced.
    /// Used to say hello to a relay on connect.
    ///
    /// # Errors
    /// Returns an error if the stored sync state is unreadable.
    pub fn sync_cursor(&self, graph: &str) -> Result<Option<Cursor>, SyncError> {
        Ok(self.load_sync_state(graph)?.cursor)
    }

    /// Collects local changes not yet on the relay for `graph`, as opaque blobs
    /// to hand a relay.
    ///
    /// Pushed state is not advanced here: a change is marked pushed only once it
    /// is echoed back through [`Vault::apply_remote`], which is the relay's
    /// acknowledgement. So a send that never reaches the relay is retried, and
    /// unsent local edits are never prematurely marked pushed.
    ///
    /// # Errors
    /// Returns an error if a store or automerge operation fails.
    pub fn collect_outgoing(&mut self, graph: &str) -> Result<Vec<Vec<u8>>, SyncError> {
        let state = self.load_sync_state(graph)?;
        self.gather_outgoing(&state, &NoCipher)
    }

    /// Applies change blobs pulled from a relay and advances `graph`'s cursor,
    /// persisting the vault and sync state. Returns what changed.
    ///
    /// # Errors
    /// Returns an error if a blob is malformed or a store/automerge op fails.
    pub fn apply_remote(
        &mut self,
        graph: &str,
        changes: Vec<Vec<u8>>,
        cursor: Cursor,
    ) -> Result<Applied, SyncError> {
        let mut state = self.load_sync_state(graph)?;
        let applied = self.apply_incoming(changes, &NoCipher)?;
        self.persist()?;
        let touched: Vec<String> = applied.keys().cloned().collect();
        // Received changes are on the relay by definition, so mark them pushed
        // (without disturbing unsent local heads). This acknowledges our own
        // changes echoed back and stops us re-sending other devices' changes.
        advance_pushed(&mut state, &applied)?;
        state.cursor = Some(cursor);
        self.save_sync_state(graph, &state)?;
        Ok(Applied::from_touched(touched))
    }

    fn gather_outgoing(
        &mut self,
        state: &SyncState,
        cipher: &impl Cipher,
    ) -> Result<Vec<Vec<u8>>, SyncError> {
        let mut out = Vec::new();
        let have = heads_for(state, ROOT_DOC);
        for change in self.root_changes_since(have) {
            out.push(cipher.encrypt(Envelope::new(ROOT_DOC, change).encode()?));
        }
        for id in self.note_ids() {
            let Some(bytes) = self.store_get(&id)? else {
                continue;
            };
            let mut note = NoteDoc::load(&bytes)?;
            for change in note.changes_since(heads_for(state, &id)) {
                out.push(cipher.encrypt(Envelope::new(&id, change).encode()?));
            }
        }
        Ok(out)
    }

    /// Decrypts and applies `blobs`, returning the applied changes grouped by
    /// doc id (so callers can both react to and record what arrived).
    fn apply_incoming(
        &mut self,
        blobs: Vec<Vec<u8>>,
        cipher: &impl Cipher,
    ) -> Result<BTreeMap<String, Vec<Vec<u8>>>, SyncError> {
        let mut by_doc: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
        for blob in blobs {
            let env = Envelope::decode(&cipher.decrypt(blob)?)?;
            by_doc.entry(env.doc_id).or_default().push(env.change);
        }
        for (doc_id, changes) in &by_doc {
            if doc_id == ROOT_DOC {
                self.root_apply(changes.clone())?;
                continue;
            }
            let mut note = match self.store_get(doc_id)? {
                Some(bytes) => NoteDoc::load(&bytes)?,
                None => NoteDoc::empty(),
            };
            note.apply(changes.clone())?;
            self.store_put(doc_id, note.to_bytes())?;
        }
        Ok(by_doc)
    }

    fn load_sync_state(&self, graph: &str) -> Result<SyncState, SyncError> {
        match self.store_get(&sync_key(graph))? {
            Some(bytes) => ciborium::from_reader(bytes.as_slice())
                .map_err(|e| SyncError::Malformed(e.to_string())),
            None => Ok(SyncState::default()),
        }
    }

    fn save_sync_state(&mut self, graph: &str, state: &SyncState) -> Result<(), SyncError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(state, &mut bytes)
            .map_err(|e| SyncError::Malformed(e.to_string()))?;
        self.store_put(&sync_key(graph), bytes)?;
        Ok(())
    }
}

fn heads_for<'a>(state: &'a SyncState, doc_id: &str) -> &'a [ChangeHash] {
    state.pushed.get(doc_id).map_or(&[], |h| h.0.as_slice())
}

/// Advances `pushed` to cover `applied` (changes now known to be on the relay),
/// leaving unsent local-only heads untouched. For each doc the new frontier is
/// the old pushed heads plus the applied change hashes, minus any hash that
/// another applied change lists as a dependency.
fn advance_pushed(
    state: &mut SyncState,
    applied: &BTreeMap<String, Vec<Vec<u8>>>,
) -> Result<(), SyncError> {
    for (doc_id, changes) in applied {
        let mut frontier = heads_for(state, doc_id).to_vec();
        let mut superseded: BTreeSet<ChangeHash> = BTreeSet::new();
        for bytes in changes {
            let change = Change::from_bytes(bytes.clone())
                .map_err(|e| SyncError::Malformed(e.to_string()))?;
            superseded.extend(change.deps().iter().copied());
            frontier.push(change.hash());
        }
        frontier.retain(|h| !superseded.contains(h));
        frontier.sort_unstable_by_key(|h| h.0);
        frontier.dedup();
        state.pushed.insert(doc_id.clone(), Heads(frontier));
    }
    Ok(())
}

fn sync_key(graph: &str) -> String {
    format!("sync-{graph}")
}

/// One change tagged with the doc it belongs to, as stored on the relay.
#[derive(Serialize, Deserialize)]
struct Envelope {
    doc_id: String,
    change: Vec<u8>,
}

impl Envelope {
    fn new(doc_id: &str, change: Vec<u8>) -> Self {
        Self {
            doc_id: doc_id.to_string(),
            change,
        }
    }

    fn encode(&self) -> Result<Vec<u8>, SyncError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf).map_err(|e| SyncError::Malformed(e.to_string()))?;
        Ok(buf)
    }

    fn decode(bytes: &[u8]) -> Result<Self, SyncError> {
        ciborium::from_reader(bytes).map_err(|e| SyncError::Malformed(e.to_string()))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::*;
    use crate::storage::InMemoryStore;
    use std::collections::HashMap;

    /// An in-memory relay: one append-only blob log per graph.
    #[derive(Default)]
    struct MemRelay {
        logs: HashMap<String, Vec<Vec<u8>>>,
    }

    impl Transport for MemRelay {
        fn sync(
            &mut self,
            graph: &str,
            since: Option<Cursor>,
            outgoing: Vec<Vec<u8>>,
        ) -> Result<Delta, TransportError> {
            let log = self.logs.entry(graph.to_string()).or_default();
            log.extend(outgoing);
            let from = since.map_or(0, |c| usize::try_from(c.seq).unwrap_or(usize::MAX));
            let changes = log.iter().skip(from).cloned().collect();
            let cursor = Cursor {
                epoch: 0,
                seq: u64::try_from(log.len()).unwrap_or(u64::MAX),
            };
            Ok(Delta {
                changes,
                cursor,
                snapshot: None,
            })
        }
    }

    fn vault() -> Vault {
        Vault::new(InMemoryStore::default()).unwrap()
    }

    #[test]
    fn empty_device_bootstraps_from_the_relay() {
        let mut relay = MemRelay::default();

        let mut a = vault();
        let (id, _) = a.create_note("n.md", "N", "hello").unwrap();
        a.sync("g", &mut relay).unwrap();

        let mut b = vault();
        let out = b.sync("g", &mut relay).unwrap();

        assert!(out.pulled > 0);
        assert_eq!(b.list_notes(0, 10).len(), 1);
        assert_eq!(b.get_note(&id).unwrap().body().unwrap(), "hello");
    }

    #[test]
    fn second_sync_exchanges_nothing_new() {
        let mut relay = MemRelay::default();
        let mut a = vault();
        a.create_note("n.md", "N", "hello").unwrap();
        a.sync("g", &mut relay).unwrap();

        let out = a.sync("g", &mut relay).unwrap();
        assert_eq!(out, SyncOutcome::default());
    }

    #[test]
    fn concurrent_edits_merge_through_the_relay() {
        let mut relay = MemRelay::default();
        let mut a = vault();
        let (id, _) = a.create_note("n.md", "N", "one two three").unwrap();
        a.sync("g", &mut relay).unwrap();
        let mut b = vault();
        b.sync("g", &mut relay).unwrap();

        let mut on_a = a.get_note(&id).unwrap();
        on_a.splice(0, 3, "ONE").unwrap();
        a.update_note(&id, &mut on_a).unwrap();
        let mut on_b = b.get_note(&id).unwrap();
        on_b.splice(8, 5, "THREE").unwrap();
        b.update_note(&id, &mut on_b).unwrap();

        a.sync("g", &mut relay).unwrap();
        b.sync("g", &mut relay).unwrap();
        a.sync("g", &mut relay).unwrap();

        assert_eq!(a.get_note(&id).unwrap().body().unwrap(), "ONE two THREE");
        assert_eq!(b.get_note(&id).unwrap().body().unwrap(), "ONE two THREE");
    }

    #[test]
    fn a_relay_snapshot_is_rejected_not_silently_skipped() {
        struct Snapshotting;
        impl Transport for Snapshotting {
            fn sync(
                &mut self,
                _graph: &str,
                _since: Option<Cursor>,
                _outgoing: Vec<Vec<u8>>,
            ) -> Result<Delta, TransportError> {
                Ok(Delta {
                    changes: Vec::new(),
                    cursor: Cursor { epoch: 1, seq: 0 },
                    snapshot: Some(vec![1, 2, 3]),
                })
            }
        }

        let mut a = vault();
        let result = a.sync("g", &mut Snapshotting);
        assert!(matches!(result, Err(SyncError::Snapshot)));
        // The cursor was not advanced, so a retry still starts from scratch.
        assert!(a.load_sync_state("g").unwrap().cursor.is_none());
    }

    #[test]
    fn collect_outgoing_and_apply_remote_converge() {
        // Simulate the websocket flow: A collects its changes, a relay assigns
        // a cursor, B applies them via apply_remote.
        let mut a = vault();
        let (id, _) = a.create_note("n.md", "N", "hello").unwrap();
        let outgoing = a.collect_outgoing("g").unwrap();
        let cursor = Cursor {
            epoch: 0,
            seq: outgoing.len() as u64,
        };

        let mut b = vault();
        let applied = b.apply_remote("g", outgoing, cursor).unwrap();

        assert!(applied.list_changed);
        assert_eq!(applied.notes, vec![id.clone()]);
        assert_eq!(b.get_note(&id).unwrap().body().unwrap(), "hello");
        assert_eq!(b.sync_cursor("g").unwrap(), Some(cursor));
    }

    #[test]
    fn applied_remote_changes_are_not_pushed_back() {
        // B receives A's changes; it must not echo them to the relay again.
        let mut a = vault();
        a.create_note("n.md", "N", "hello").unwrap();
        let outgoing = a.collect_outgoing("g").unwrap();
        let cursor = Cursor {
            epoch: 0,
            seq: outgoing.len() as u64,
        };

        let mut b = vault();
        b.apply_remote("g", outgoing, cursor).unwrap();

        assert!(b.collect_outgoing("g").unwrap().is_empty());
    }

    #[test]
    fn a_change_stays_outgoing_until_its_echo_is_applied() {
        // collect_outgoing must not advance pushed state: an unacknowledged send
        // (which may have been lost) has to be retried until it echoes back.
        let mut a = vault();
        a.create_note("n.md", "N", "hello").unwrap();

        let first = a.collect_outgoing("g").unwrap();
        assert!(!first.is_empty());
        let again = a.collect_outgoing("g").unwrap();
        assert_eq!(again.len(), first.len());

        // The relay echoes the batch back; now it is acknowledged.
        let cursor = Cursor {
            epoch: 0,
            seq: first.len() as u64,
        };
        a.apply_remote("g", first, cursor).unwrap();
        assert!(a.collect_outgoing("g").unwrap().is_empty());
    }

    #[test]
    fn an_unsent_local_edit_survives_an_unrelated_remote_batch() {
        // A has an unpushed local note when an unrelated remote note arrives.
        // Applying the remote batch must not mark A's local edit as pushed.
        let mut a = vault();
        a.create_note("local.md", "L", "mine").unwrap();

        let mut other = vault();
        other.create_note("remote.md", "R", "theirs").unwrap();
        let incoming = other.collect_outgoing("g").unwrap();
        let cursor = Cursor {
            epoch: 0,
            seq: incoming.len() as u64,
        };
        a.apply_remote("g", incoming, cursor).unwrap();

        // A's own note is still waiting to be pushed.
        assert!(!a.collect_outgoing("g").unwrap().is_empty());
    }

    #[test]
    fn envelope_round_trips() {
        let blob = Envelope::new("note_abc", b"change-bytes".to_vec())
            .encode()
            .unwrap();
        let env = Envelope::decode(&blob).unwrap();
        assert_eq!(env.doc_id, "note_abc");
        assert_eq!(env.change, b"change-bytes");
    }
}
