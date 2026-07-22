use automerge::{AutoCommit, ObjType, ROOT, ReadDoc, Value, transaction::Transactable};

const BODY_KEY: &str = "body";

/// An individual note.
///
/// Internally, it's an Automerge document holding one `body` field (Markdown
/// text, YAML frontmatter included) as a character-level CRDT.
// TODO: expose doc heads (e.g. a `heads()` accessor) once the History doc
// needs to record snapshot pointers against this note's changes.
pub struct NoteDoc {
    doc: AutoCommit,
}

/// Errors returned by [`NoteDoc`] operations.
#[derive(Debug, thiserror::Error)]
pub enum NoteError {
    /// The underlying Automerge document operation failed.
    #[error("automerge operation failed: {0}")]
    Automerge(#[from] automerge::AutomergeError),
    /// The doc has no `body` field at all.
    #[error("document has no body field")]
    MissingBody,
    /// The doc's `body` field exists but isn't a text object.
    #[error("body field is not a text object")]
    InvalidBodyType,
}

impl NoteDoc {
    /// Creates a new note doc with the given Markdown as its initial body.
    ///
    /// # Errors
    /// Returns an error if the underlying Automerge operations fail.
    pub fn new(markdown: &str) -> Result<Self, NoteError> {
        let mut doc = AutoCommit::new();
        let body = doc.put_object(ROOT, BODY_KEY, ObjType::Text)?;
        doc.splice_text(&body, 0, 0, markdown)?;
        Ok(Self { doc })
    }

    /// Loads a note doc from previously saved Automerge bytes.
    ///
    /// # Errors
    /// Returns an error if `bytes` is not a valid Automerge document.
    pub fn load(bytes: &[u8]) -> Result<Self, NoteError> {
        let doc = AutoCommit::load(bytes)?;
        Ok(Self { doc })
    }

    /// Serializes the doc to bytes for storage or transfer.
    pub fn to_bytes(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    /// Returns the current Markdown body.
    ///
    /// # Errors
    /// Returns an error if the doc has no `body` field or it isn't a text object.
    pub fn body(&self) -> Result<String, NoteError> {
        let body = self.body_obj()?;
        Ok(self.doc.text(&body)?)
    }

    /// Replaces the whole body with `markdown`, diffed against the current
    /// value so unrelated regions keep their CRDT identity. This is the
    /// entry point for editors that hand over the full buffer on every edit.
    ///
    /// # Errors
    /// Returns an error if the doc has no `body` field or it isn't a text object.
    pub fn set_body(&mut self, markdown: &str) -> Result<(), NoteError> {
        let body = self.body_obj()?;
        Ok(self.doc.update_text(&body, markdown)?)
    }

    /// Splices the body at a known position, for editors that already track
    /// precise cursor-position edits. `del` deletes after `pos` if positive,
    /// before `pos` if negative.
    ///
    /// # Errors
    /// Returns an error if the doc has no `body` field or it isn't a text object.
    pub fn splice(&mut self, pos: usize, del: isize, text: &str) -> Result<(), NoteError> {
        let body = self.body_obj()?;
        Ok(self.doc.splice_text(&body, pos, del, text)?)
    }

    /// Forks this doc into an independent replica (e.g. to simulate a second
    /// device) that can later be merged back in.
    #[must_use]
    pub fn fork(&mut self) -> Self {
        Self {
            doc: self.doc.fork(),
        }
    }

    /// Merges changes from another replica of this note into this one.
    ///
    /// # Errors
    /// Returns an error if the two docs' histories can't be merged.
    pub fn merge(&mut self, other: &mut Self) -> Result<(), NoteError> {
        self.doc.merge(&mut other.doc)?;
        Ok(())
    }

    fn body_obj(&self) -> Result<automerge::ObjId, NoteError> {
        match self.doc.get(ROOT, BODY_KEY)? {
            Some((Value::Object(ObjType::Text), obj_id)) => Ok(obj_id),
            Some(_) => Err(NoteError::InvalidBodyType),
            None => Err(NoteError::MissingBody),
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]
mod tests {
    use super::*;

    #[test]
    fn new_and_body_round_trip() {
        let note = NoteDoc::new("# Hello\n\nWorld").unwrap();
        assert_eq!(note.body().unwrap(), "# Hello\n\nWorld");
    }

    #[test]
    fn splice_edits_a_range() {
        let mut note = NoteDoc::new("Hello World").unwrap();
        note.splice(6, 5, "There").unwrap();
        assert_eq!(note.body().unwrap(), "Hello There");
    }

    #[test]
    fn set_body_replaces_whole_string() {
        let mut note = NoteDoc::new("Draft one").unwrap();
        note.set_body("Draft two, revised").unwrap();
        assert_eq!(note.body().unwrap(), "Draft two, revised");
    }

    #[test]
    fn to_bytes_and_load_round_trip() {
        let mut note = NoteDoc::new("Persisted content").unwrap();
        let bytes = note.to_bytes();
        let loaded = NoteDoc::load(&bytes).unwrap();
        assert_eq!(loaded.body().unwrap(), "Persisted content");
    }

    #[test]
    fn concurrent_edits_to_different_regions_merge_cleanly() {
        let mut original = NoteDoc::new("one two three").unwrap();
        let mut replica = original.fork();

        // Two "devices" edit disjoint parts of the same note independently.
        original.splice(0, 3, "ONE").unwrap();
        replica.splice(8, 5, "THREE").unwrap();

        original.merge(&mut replica).unwrap();
        assert_eq!(original.body().unwrap(), "ONE two THREE");
    }
}
