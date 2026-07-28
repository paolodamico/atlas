//! atlas-client: full client SDK for atlas.
//!
//! Extends the core handling of atlas with network layer, websocket sync, state event streaming and
//! foreign bindings.

mod sync;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use atlas_core::{Applied, FileStore, NoteError, NoteSummary, Vault, VaultError};
use tokio::sync::broadcast;

const EVENT_QUEUE: usize = 64;

/// A state change for the host to render. Carries the new state so callers
/// never have to re-query.
#[derive(Debug, Clone)]
pub enum Event {
    /// The note list changed (created or deleted).
    Notes(Vec<NoteSummary>),
    /// A note's body changed.
    Note {
        /// The note's id.
        id: String,
        /// The note's current body.
        body: String,
    },
    /// The relay sync connection state changed.
    Status(SyncStatus),
}

/// The relay sync connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    /// Not connected; will retry with backoff.
    Offline,
    /// Attempting to connect.
    Connecting,
    /// Connected and syncing.
    Live,
}

/// Errors from [`Client`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A vault operation failed.
    #[error(transparent)]
    Vault(Box<VaultError>),
    /// A note doc operation failed.
    #[error(transparent)]
    Note(Box<NoteError>),
}

impl From<VaultError> for ClientError {
    fn from(e: VaultError) -> Self {
        Self::Vault(Box::new(e))
    }
}

impl From<NoteError> for ClientError {
    fn from(e: NoteError) -> Self {
        Self::Note(Box::new(e))
    }
}

/// The sync SDK: a guarded vault plus a stream of state events.
///
/// Mutations are synchronous local operations. Subscribe with [`Client::subscribe`]
/// to observe changes (including, once wired, changes pulled from a relay).
pub struct Client {
    vault: Arc<Mutex<Vault>>,
    events: broadcast::Sender<Event>,
}

impl Client {
    /// Opens (or creates) the vault at `dir`.
    ///
    /// # Errors
    /// Returns an error if the vault can't be opened.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, ClientError> {
        let vault = Vault::load(FileStore::new(dir.into())?)?;
        let (events, _) = broadcast::channel(EVENT_QUEUE);
        Ok(Self {
            vault: Arc::new(Mutex::new(vault)),
            events,
        })
    }

    /// Subscribes to state changes. Events sent after this call are delivered.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Starts background websocket sync with the relay at `url`, for `graph`.
    /// Remote changes flow into the same event stream as local edits, and local
    /// edits are pushed. Reconnects with backoff; status is reported as events.
    ///
    /// Must be called from within a tokio runtime.
    pub fn connect(&self, url: impl Into<String>, graph: impl Into<String>) {
        let syncer = sync::Syncer::new(Arc::clone(&self.vault), self.events.clone(), graph.into());
        tokio::spawn(syncer.run(url.into()));
    }

    /// Creates a note, returning its id.
    ///
    /// # Errors
    /// Returns an error if the note can't be created.
    pub fn create_note(&self, path: &str, title: &str, body: &str) -> Result<String, ClientError> {
        let mut vault = self.lock();
        let (id, _) = vault.create_note(path, title, body)?;
        self.emit_note(id.clone(), body.to_string());
        self.emit_notes(&vault);
        Ok(id)
    }

    /// Replaces a note's body.
    ///
    /// # Errors
    /// Returns an error if the note is unknown.
    pub fn edit_note(&self, id: &str, body: &str) -> Result<(), ClientError> {
        let mut vault = self.lock();
        let mut doc = vault.get_note(id)?;
        doc.set_body(body)?;
        vault.update_note(id, &mut doc)?;
        self.emit_note(id.to_string(), body.to_string());
        Ok(())
    }

    /// Deletes a note.
    ///
    /// # Errors
    /// Returns an error if the note is unknown.
    pub fn delete_note(&self, id: &str) -> Result<(), ClientError> {
        let mut vault = self.lock();
        vault.delete_note(id)?;
        self.emit_notes(&vault);
        Ok(())
    }

    /// Lists all notes.
    #[must_use]
    pub fn list_notes(&self) -> Vec<NoteSummary> {
        self.lock().list_notes(0, usize::MAX)
    }

    /// Returns a note's current body.
    ///
    /// # Errors
    /// Returns an error if the note is unknown.
    pub fn note_body(&self, id: &str) -> Result<String, ClientError> {
        Ok(self.lock().get_note(id)?.body()?)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vault> {
        guard(&self.vault)
    }

    fn emit_note(&self, id: String, body: String) {
        let _ = self.events.send(Event::Note { id, body });
    }

    fn emit_notes(&self, vault: &Vault) {
        let _ = self
            .events
            .send(Event::Notes(vault.list_notes(0, usize::MAX)));
    }
}

fn guard(vault: &Mutex<Vault>) -> std::sync::MutexGuard<'_, Vault> {
    vault
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Emits events for what a relay pull changed, reading current state once.
fn emit_applied(vault: &Mutex<Vault>, events: &broadcast::Sender<Event>, applied: &Applied) {
    let vault = guard(vault);
    for id in &applied.notes {
        let Ok(doc) = vault.get_note(id) else {
            continue;
        };
        let Ok(body) = doc.body() else {
            continue;
        };
        let _ = events.send(Event::Note {
            id: id.clone(),
            body,
        });
    }
    if applied.list_changed {
        let _ = events.send(Event::Notes(vault.list_notes(0, usize::MAX)));
    }
}
