//! Atlas core: the local Automerge engine for notes.

mod note;
mod storage;
mod vault;

pub use note::{NoteDoc, NoteError};
pub use storage::{FileNoteStore, NoteStore};
pub use vault::{NoteSummary, Vault, VaultError};
