use std::path::PathBuf;

use automerge::{
    AutoCommit, ObjType, ROOT, ReadDoc, ScalarValue, Value, transaction::Transactable,
};
use uuid::Uuid;

use crate::storage::Store;
use crate::{NoteDoc, NoteError};

const PATH_KEY: &str = "path";
const TITLE_KEY: &str = "title";
/// Prefix for every note id. Critical to avoid collisions with other reserved keys.
const NOTE_ID_PREFIX: &str = "note_";
/// Reserved id the vault's root-doc is stored under
const VAULT_ID: &str = "vault";

/// Primary state.
///
/// Holds all the required metadata about notes, and all the relevant
/// pointers. Everything (the root doc's own bytes, under the reserved id
/// `"vault"`, and every note's bytes, under its own id) lives in one
/// [`Store`] (fs by default) — the crate manages what goes where, callers
/// just supply a backend.
///
/// In `automerge` context this is the Root doc.
pub struct Vault {
    root: AutoCommit,
    store: Box<dyn Store>,
}

/// Metadata about a note.
///
/// This should remain a lightweight struct, as it is used frequently in UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteSummary {
    /// Stable id within the vault.
    pub id: String,
    /// File path within the vault.
    pub path: String,
    /// Cached display title.
    pub title: String,
}

impl Vault {
    /// Creates a vault with a fresh, empty root doc, backed by `store`.
    ///
    /// # Errors
    /// Returns an error if the underlying Automerge operations fail.
    pub fn new(store: impl Store + 'static) -> Result<Self, VaultError> {
        Ok(Self {
            root: AutoCommit::new(),
            store: Box::new(store),
        })
    }

    /// Loads a vault whose root-doc bytes already exist in `store` (under
    /// the reserved id [`VAULT_ID`]). Errors if they don't — see
    /// [`Vault::load`] for a version that creates a fresh vault instead.
    ///
    /// # Errors
    /// Returns [`VaultError::NoRootDoc`] if `store` has nothing stored under
    /// that id, or an error if the bytes aren't a valid Automerge document.
    pub fn load_existing(store: impl Store + 'static) -> Result<Self, VaultError> {
        let bytes = store.get(VAULT_ID)?.ok_or(VaultError::NoRootDoc)?;
        let root = AutoCommit::load(&bytes)?;
        Ok(Self {
            root,
            store: Box::new(store),
        })
    }

    /// Serializes the vault's metadata (notes are **not** included) to bytes.
    pub fn to_bytes(&mut self) -> Vec<u8> {
        self.root.save()
    }

    /// Loads a vault backed by `store`, or creates a fresh one if `store`
    /// doesn't have one yet. This is the usual entry point (e.g.
    /// `Vault::load(FileStore::new(dir)?)`), since it doesn't require the
    /// caller to know ahead of time whether the backend is new or
    /// pre-existing.
    ///
    /// # Errors
    /// Returns an error if the underlying Automerge operations fail.
    pub fn load(store: impl Store + 'static) -> Result<Self, VaultError> {
        if store.get(VAULT_ID)?.is_some() {
            Self::load_existing(store)
        } else {
            Self::new(store)
        }
    }

    /// Writes the vault's metadata to `store` (atomically, if disk backed).
    /// Structural changes (`create_note`/`rename_note`/`move_note`/
    /// `delete_note`) already call this automatically, so it's exposed
    /// mainly as a manual checkpoint.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub fn persist(&mut self) -> Result<(), VaultError> {
        let bytes = self.to_bytes();
        self.store.put(VAULT_ID, bytes)
    }

    /// Merges another vault's root doc (note metadata) into this one, leaving
    /// `other` untouched. The caller is responsible for persisting afterward.
    pub(crate) fn merge_root_from(&mut self, other: &Self) -> Result<(), VaultError> {
        // Automerge's merge needs `&mut` on the source (it commits pending
        // ops), so clone `other`'s doc to keep the remote read-only.
        self.root.merge(&mut other.root.clone())?;
        Ok(())
    }

    /// All registered note ids, unpaginated. Reads only the root doc.
    pub(crate) fn note_ids(&self) -> Vec<String> {
        self.root
            .keys(ROOT)
            .filter(|k| k.starts_with(NOTE_ID_PREFIX))
            .collect()
    }

    /// Reads a note's raw stored bytes, bypassing the registration check
    /// (the sync layer works from the already-merged id set).
    pub(crate) fn store_get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError> {
        self.store.get(id)
    }

    /// Writes a note's raw bytes into this vault's store.
    pub(crate) fn store_put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError> {
        self.store.put(id, bytes)
    }

    /// Creates a new note, storing its bytes and registering it in the
    /// vault's note list. Returns the new note's id and the live `NoteDoc`
    /// so the caller can keep editing it without an extra fetch.
    ///
    /// # Errors
    /// Returns an error if the underlying Automerge operations fail.
    pub fn create_note(
        &mut self,
        path: &str,
        title: &str,
        initial_markdown: &str,
    ) -> Result<(String, NoteDoc), VaultError> {
        let id = Self::mint_note_id();
        let mut note = NoteDoc::new(initial_markdown)?;
        self.store.put(&id, note.to_bytes())?;

        let entry = self.root.put_object(ROOT, id.as_str(), ObjType::Map)?;
        self.root.put(&entry, PATH_KEY, path)?;
        self.root.put(&entry, TITLE_KEY, title)?;
        self.persist()?;

        Ok((id, note))
    }

    /// Hydrates the note with the given id from the store. This is
    /// the only place a note's body is loaded into memory.
    ///
    /// # Errors
    /// Returns an error if the id is unknown or the stored bytes are invalid.
    pub fn get_note(&self, id: &str) -> Result<NoteDoc, VaultError> {
        // Notes and the root doc's own bytes share one `Store`, so a
        // registration check keeps `get_note` from hydrating the reserved
        // `"vault"` id (or any orphaned id) as if it were a note.
        self.entry_obj(id)?;
        let bytes = self
            .store
            .get(id)?
            .ok_or_else(|| VaultError::NoteNotFound(id.to_string()))?;
        Ok(NoteDoc::load(&bytes)?)
    }

    /// Persists `note`'s current bytes back into the vault. Doesn't touch
    /// title/path — use [`Vault::rename_note`]/[`Vault::move_note`] for that.
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn update_note(&mut self, id: &str, note: &mut NoteDoc) -> Result<(), VaultError> {
        self.entry_obj(id)?;
        self.store.put(id, note.to_bytes())?;
        Ok(())
    }

    /// Renames a note (updates its cached title only).
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn rename_note(&mut self, id: &str, title: &str) -> Result<(), VaultError> {
        let entry = self.entry_obj(id)?;
        self.root.put(&entry, TITLE_KEY, title)?;
        self.persist()
    }

    /// Moves a note to a new path (identity is unaffected).
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn move_note(&mut self, id: &str, new_path: &str) -> Result<(), VaultError> {
        let entry = self.entry_obj(id)?;
        self.root.put(&entry, PATH_KEY, new_path)?;
        self.persist()
    }

    /// Permanently deletes a note from the vault.
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn delete_note(&mut self, id: &str) -> Result<(), VaultError> {
        if self.root.get(ROOT, id)?.is_none() {
            return Err(VaultError::NoteNotFound(id.to_string()));
        }
        // Persist the metadata removal *before* deleting the bytes: if the
        // persist fails (disk full, permissions), the note stays fully
        // intact and recoverable rather than listed-but-unreadable.
        self.root.delete(ROOT, id)?;
        self.persist()?;
        self.store.delete(id)
    }

    /// Lists notes' metadata, paginated by `offset`/`limit`. Reads only the
    /// root doc's note list — never hydrates a note body.
    // TODO: ordering
    #[must_use]
    pub fn list_notes(&self, offset: usize, limit: usize) -> Vec<NoteSummary> {
        let summaries: Vec<NoteSummary> = self
            .root
            .keys(ROOT)
            .filter(|id| id.starts_with(NOTE_ID_PREFIX))
            .filter_map(|id| self.summary_of(&id))
            .collect();
        summaries.into_iter().skip(offset).take(limit).collect()
    }

    fn summary_of(&self, id: &str) -> Option<NoteSummary> {
        let (_, entry) = self.root.get(ROOT, id).ok().flatten()?;
        Some(NoteSummary {
            id: id.to_string(),
            path: self.string_field(&entry, PATH_KEY)?,
            title: self.string_field(&entry, TITLE_KEY)?,
        })
    }

    fn string_field(&self, obj: &automerge::ObjId, key: &str) -> Option<String> {
        match self.root.get(obj, key).ok()?? {
            (Value::Scalar(s), _) => match s.as_ref() {
                ScalarValue::Str(s) => Some(s.to_string()),
                _ => None,
            },
            _ => None,
        }
    }

    fn entry_obj(&self, id: &str) -> Result<automerge::ObjId, VaultError> {
        match self.root.get(ROOT, id)? {
            Some((Value::Object(ObjType::Map), obj_id)) => Ok(obj_id),
            _ => Err(VaultError::NoteNotFound(id.to_string())),
        }
    }

    // Random so two offline devices don't race to mint the same id
    fn mint_note_id() -> String {
        format!("{NOTE_ID_PREFIX}{}", Uuid::new_v4().as_simple())
    }
}

/// Errors returned by [`Vault`] operations.
#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    /// The underlying Automerge document operation failed.
    #[error("automerge operation failed: {0}")]
    Automerge(#[from] automerge::AutomergeError),
    /// Reading or writing a note's own doc failed.
    #[error("note doc error: {0}")]
    Note(#[from] NoteError),
    /// No note with the given id exists in this vault.
    #[error("no note with id {0}")]
    NoteNotFound(String),
    /// A storage read/write failed.
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A path used internally by the storage layer has no parent directory
    /// or file name component (e.g. it's empty or a filesystem root).
    #[error("invalid storage path: {0:?}")]
    InvalidPath(PathBuf),
    /// [`Vault::load_existing`] was called with a `store` that has no root
    /// doc bytes stored under the reserved id.
    #[error("no root doc found in the given store")]
    NoRootDoc,
    /// An error that doesn't fit the other variants above.
    #[error("unexpected error: {0}")]
    UnexpectedError(String),
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::*;
    use crate::storage::{FileStore, InMemoryStore};

    #[test]
    fn create_and_list_notes() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let (id, _note) = vault
            .create_note("ideas/atlas.md", "Atlas", "# Atlas\n\nBrain extension")
            .unwrap();

        let summaries = vault.list_notes(0, 10);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].path, "ideas/atlas.md");
        assert_eq!(summaries[0].title, "Atlas");
    }

    #[test]
    fn note_ids_are_not_sequential_or_reused() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let (id1, _) = vault.create_note("a.md", "A", "content").unwrap();
        let (id2, _) = vault.create_note("b.md", "B", "content").unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn get_note_hydrates_created_content() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "Hello vault").unwrap();

        let fetched = vault.get_note(&id).unwrap();
        assert_eq!(fetched.body().unwrap(), "Hello vault");
    }

    #[test]
    fn update_note_persists_body() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let (id, mut note) = vault.create_note("a.md", "A", "Original body").unwrap();

        note.set_body("Updated body").unwrap();
        vault.update_note(&id, &mut note).unwrap();

        let fetched = vault.get_note(&id).unwrap();
        assert_eq!(fetched.body().unwrap(), "Updated body");
    }

    #[test]
    fn list_notes_paginates() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let mut ids: Vec<_> = ["Charlie", "Alice", "Bob"]
            .iter()
            .map(|title| {
                vault
                    .create_note(&format!("{title}.md"), title, "content")
                    .unwrap()
                    .0
            })
            .collect();

        let page1 = vault.list_notes(0, 2);
        let page2 = vault.list_notes(2, 2);
        assert_eq!(page1.len(), 2);
        assert_eq!(page2.len(), 1);

        let mut paged_ids: Vec<_> = page1.iter().chain(&page2).map(|s| s.id.clone()).collect();
        paged_ids.sort();
        ids.sort();
        assert_eq!(paged_ids, ids);
    }

    #[test]
    fn rename_move_and_delete_note() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "content").unwrap();

        vault.rename_note(&id, "Renamed").unwrap();
        vault.move_note(&id, "b.md").unwrap();

        let summaries = vault.list_notes(0, 10);
        assert_eq!(summaries[0].title, "Renamed");
        assert_eq!(summaries[0].path, "b.md");

        vault.delete_note(&id).unwrap();
        assert!(vault.list_notes(0, 10).is_empty());
        assert!(matches!(
            vault.get_note(&id),
            Err(VaultError::NoteNotFound(_))
        ));
    }

    #[test]
    fn persist_and_reload_round_trips_metadata() {
        // A cloned `InMemoryStore` shares the same backing map, standing in
        // for two `FileStore`s pointed at the same directory.
        let store = InMemoryStore::default();
        let mut vault = Vault::new(store.clone()).unwrap();
        vault.create_note("a.md", "A", "content a").unwrap();
        vault.create_note("b.md", "B", "content b").unwrap();
        vault.persist().unwrap();

        let loaded = Vault::load_existing(store).unwrap();

        let original_titles: Vec<_> = vault
            .list_notes(0, 10)
            .into_iter()
            .map(|s| s.title)
            .collect();
        let loaded_titles: Vec<_> = loaded
            .list_notes(0, 10)
            .into_iter()
            .map(|s| s.title)
            .collect();
        assert_eq!(original_titles, loaded_titles);
    }

    #[test]
    fn load_on_empty_dir_creates_empty_vault() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        let vault = Vault::load(store).unwrap();
        assert!(vault.list_notes(0, 10).is_empty());
    }

    #[test]
    fn note_content_and_metadata_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        let mut vault = Vault::load(store).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "Hello disk").unwrap();
        // No explicit `persist()`, `create_note` auto-persists metadata.
        drop(vault);

        let store = FileStore::new(dir.path()).unwrap();
        let reopened = Vault::load(store).unwrap();
        let summaries = reopened.list_notes(0, 10);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, id);
        assert_eq!(summaries[0].title, "A");
        assert_eq!(
            reopened.get_note(&id).unwrap().body().unwrap(),
            "Hello disk"
        );
    }

    #[test]
    fn update_note_survives_reopen_without_explicit_persist() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        let mut vault = Vault::load(store.clone()).unwrap();
        let (id, mut note) = vault.create_note("a.md", "A", "Original").unwrap();
        note.set_body("Edited on disk").unwrap();
        vault.update_note(&id, &mut note).unwrap();
        drop(vault);

        let reopened = Vault::load(store).unwrap();
        assert_eq!(
            reopened.get_note(&id).unwrap().body().unwrap(),
            "Edited on disk"
        );
    }

    #[test]
    fn delete_note_deletes_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        let mut vault = Vault::load(store.clone()).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "content").unwrap();
        vault.delete_note(&id).unwrap();
        drop(vault);

        let reopened = Vault::load(store).unwrap();
        assert!(reopened.list_notes(0, 10).is_empty());
        assert!(matches!(
            reopened.get_note(&id),
            Err(VaultError::NoteNotFound(_))
        ));
    }

    #[test]
    fn metadata_changes_auto_persist_without_explicit_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileStore::new(dir.path()).unwrap();
        let mut vault = Vault::load(store.clone()).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "content").unwrap();
        vault.rename_note(&id, "Renamed").unwrap();
        // No explicit `persist()` call anywhere in this test on purpose.
        drop(vault);

        let reopened = Vault::load(store).unwrap();
        assert_eq!(reopened.list_notes(0, 10)[0].title, "Renamed");
    }

    #[test]
    fn load_existing_on_empty_store_errors() {
        let store = InMemoryStore::default();
        assert!(matches!(
            Vault::load_existing(store),
            Err(VaultError::NoRootDoc)
        ));
    }

    #[test]
    fn get_note_rejects_reserved_vault_id() {
        let mut vault = Vault::new(InMemoryStore::default()).unwrap();
        // Put the root doc's own bytes under the reserved id, so this only
        // passes if `get_note` checks registration rather than raw presence.
        vault.persist().unwrap();
        assert!(matches!(
            vault.get_note(VAULT_ID),
            Err(VaultError::NoteNotFound(_))
        ));
    }

    /// Wraps a store so a single `put` can be made to fail on demand, to
    /// prove a failed `delete_note` leaves the note recoverable.
    #[derive(Clone, Default)]
    struct FaultyStore {
        inner: InMemoryStore,
        fail_put: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl Store for FaultyStore {
        fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError> {
            self.inner.get(id)
        }

        fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError> {
            if self.fail_put.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(VaultError::Io(std::io::Error::other(
                    "injected put failure",
                )));
            }
            self.inner.put(id, bytes)
        }

        fn delete(&mut self, id: &str) -> Result<(), VaultError> {
            self.inner.delete(id)
        }
    }

    #[test]
    fn delete_note_leaves_note_recoverable_when_persist_fails() {
        let store = FaultyStore::default();
        let mut vault = Vault::new(store.clone()).unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "irreplaceable").unwrap();
        vault.persist().unwrap();

        // Arm the fault so the persist inside `delete_note` fails.
        store
            .fail_put
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(vault.delete_note(&id).is_err());

        // Reopen from the shared backing store: the note must be intact,
        // both in metadata and in its readable bytes.
        store
            .fail_put
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let reopened = Vault::load_existing(store).unwrap();
        assert_eq!(reopened.list_notes(0, 10).len(), 1);
        assert_eq!(
            reopened.get_note(&id).unwrap().body().unwrap(),
            "irreplaceable"
        );
    }
}
