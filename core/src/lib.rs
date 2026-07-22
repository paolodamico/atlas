//! Atlas core: the local Automerge engine for notes.

mod note;
mod vault;

pub use note::{NoteDoc, NoteError};
pub use vault::{NoteSummary, Vault, VaultError};
