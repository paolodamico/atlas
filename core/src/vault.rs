use std::collections::HashMap;

use automerge::{
    AutoCommit, ObjType, ROOT, ReadDoc, ScalarValue, Value, transaction::Transactable,
};
use uuid::Uuid;

use crate::{NoteDoc, NoteError};

const NOTES_KEY: &str = "notes";
const PATH_KEY: &str = "path";
const TITLE_KEY: &str = "title";

/// The main struct which holds the entire app state for the user.
///
/// A vault holds all the required metadata about notes, and all the relevant
/// pointers (note content stored separately).
pub struct Vault {
    root: AutoCommit,
    // TODO: stand-in for the real storage/repo layer. Once that exists,
    // note bytes should live on disk (encrypted) and be addressed by real
    // repo document ids
    store: HashMap<String, Vec<u8>>,
}

/// Cheap-to-list metadata about a note, without loading its body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteSummary {
    /// The note's stable id within this vault.
    pub id: String,
    /// The note's file path within the vault.
    pub path: String,
    /// The note's cached display title.
    pub title: String,
}

impl Vault {
    /// Creates an empty vault.
    ///
    /// # Errors
    /// Returns an error if the underlying Automerge operations fail.
    pub fn new() -> Result<Self, VaultError> {
        let mut root = AutoCommit::new();
        root.put_object(ROOT, NOTES_KEY, ObjType::Map)?;
        Ok(Self {
            root,
            store: HashMap::new(),
        })
    }

    /// Loads a vault's metadata from previously saved bytes.
    // TODO: once a real storage layer exists, `load` should also rehydrate
    // `store` (e.g. from disk) so `get_note` works right after loading,
    // instead of leaving every note unavailable until re-created/re-attached.
    ///
    /// # Errors
    /// Returns an error if `bytes` is not a valid Automerge document.
    pub fn load(bytes: &[u8]) -> Result<Self, VaultError> {
        let root = AutoCommit::load(bytes)?;
        Ok(Self {
            root,
            store: HashMap::new(),
        })
    }

    /// Serializes the vault's metadata (not note bodies) to bytes.
    // TODO: `save` only covers root-doc metadata; persisting `store`'s note
    // bytes alongside it is the same storage-layer gap as `load`, above.
    pub fn save(&mut self) -> Vec<u8> {
        self.root.save()
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
        self.store.insert(id.clone(), note.save());

        let notes = self.notes_obj()?;
        let entry = self.root.put_object(&notes, id.as_str(), ObjType::Map)?;
        self.root.put(&entry, PATH_KEY, path)?;
        self.root.put(&entry, TITLE_KEY, title)?;

        Ok((id, note))
    }

    /// Hydrates the note with the given id from the vault's store. This is
    /// the only place a note's body is loaded into memory.
    ///
    /// # Errors
    /// Returns an error if the id is unknown or the stored bytes are invalid.
    pub fn get_note(&self, id: &str) -> Result<NoteDoc, VaultError> {
        let bytes = self
            .store
            .get(id)
            .ok_or_else(|| VaultError::NoteNotFound(id.to_string()))?;
        Ok(NoteDoc::load(bytes)?)
    }

    /// Persists `note`'s current bytes back into the vault.
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn update_note(&mut self, id: &str, note: &mut NoteDoc) -> Result<(), VaultError> {
        self.entry_obj(id)?;
        self.store.insert(id.to_string(), note.save());
        Ok(())
    }

    /// Renames a note (updates its cached title only).
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn rename_note(&mut self, id: &str, title: &str) -> Result<(), VaultError> {
        let entry = self.entry_obj(id)?;
        self.root.put(&entry, TITLE_KEY, title)?;
        Ok(())
    }

    /// Moves a note to a new path (identity is unaffected).
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn move_note(&mut self, id: &str, new_path: &str) -> Result<(), VaultError> {
        let entry = self.entry_obj(id)?;
        self.root.put(&entry, PATH_KEY, new_path)?;
        Ok(())
    }

    /// Permanently deletes a note from the vault.
    ///
    /// # Errors
    /// Returns an error if the id is unknown.
    pub fn delete_note(&mut self, id: &str) -> Result<(), VaultError> {
        let notes = self.notes_obj()?;
        if self.root.get(&notes, id)?.is_none() {
            return Err(VaultError::NoteNotFound(id.to_string()));
        }
        self.root.delete(&notes, id)?;
        self.store.remove(id);
        Ok(())
    }

    /// Lists notes' metadata, paginated by `offset`/`limit`. Reads only the
    /// root doc's note list — never hydrates a note body.
    // TODO: no defined ordering yet. This currently returns whatever order
    // Automerge's map iteration happens to yield, which isn't a stable
    // guarantee. Add real sorting (title, recency, etc.) once it's clear
    // what the UI actually needs.
    #[must_use]
    pub fn list_notes(&self, offset: usize, limit: usize) -> Vec<NoteSummary> {
        let Ok(notes) = self.notes_obj() else {
            return Vec::new();
        };
        let summaries: Vec<NoteSummary> = self
            .root
            .keys(&notes)
            .filter_map(|id| self.summary_of(&notes, &id))
            .collect();
        summaries.into_iter().skip(offset).take(limit).collect()
    }

    fn summary_of(&self, notes: &automerge::ObjId, id: &str) -> Option<NoteSummary> {
        let (_, entry) = self.root.get(notes, id).ok().flatten()?;
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

    fn notes_obj(&self) -> Result<automerge::ObjId, VaultError> {
        match self.root.get(ROOT, NOTES_KEY)? {
            Some((Value::Object(ObjType::Map), obj_id)) => Ok(obj_id),
            _ => {
                // Should be unreachable for vaults created via `new`/`load`,
                // but a foreign doc could lack the field.
                Err(VaultError::NoteNotFound(NOTES_KEY.to_string()))
            }
        }
    }

    fn entry_obj(&self, id: &str) -> Result<automerge::ObjId, VaultError> {
        let notes = self.notes_obj()?;
        match self.root.get(&notes, id)? {
            Some((Value::Object(ObjType::Map), obj_id)) => Ok(obj_id),
            _ => Err(VaultError::NoteNotFound(id.to_string())),
        }
    }

    // Random so two offline devices don't race to mint the same id
    fn mint_note_id() -> String {
        format!("note_{}", Uuid::new_v4().as_simple())
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
    /// An error that doesn't fit the other variants above.
    #[error("unexpected error: {0}")]
    UnexpectedError(String),
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_notes() {
        let mut vault = Vault::new().unwrap();
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
        let mut vault = Vault::new().unwrap();
        let (id1, _) = vault.create_note("a.md", "A", "content").unwrap();
        let (id2, _) = vault.create_note("b.md", "B", "content").unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn get_note_hydrates_created_content() {
        let mut vault = Vault::new().unwrap();
        let (id, _note) = vault.create_note("a.md", "A", "Hello vault").unwrap();

        let fetched = vault.get_note(&id).unwrap();
        assert_eq!(fetched.body().unwrap(), "Hello vault");
    }

    #[test]
    fn update_note_persists_body() {
        let mut vault = Vault::new().unwrap();
        let (id, mut note) = vault.create_note("a.md", "A", "Original body").unwrap();

        note.set_body("Updated body").unwrap();
        vault.update_note(&id, &mut note).unwrap();

        let fetched = vault.get_note(&id).unwrap();
        assert_eq!(fetched.body().unwrap(), "Updated body");
    }

    #[test]
    fn list_notes_paginates() {
        let mut vault = Vault::new().unwrap();
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
        let mut vault = Vault::new().unwrap();
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
    fn save_and_load_round_trips_metadata() {
        let mut vault = Vault::new().unwrap();
        vault.create_note("a.md", "A", "content a").unwrap();
        vault.create_note("b.md", "B", "content b").unwrap();

        let bytes = vault.save();
        let loaded = Vault::load(&bytes).unwrap();

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
}
