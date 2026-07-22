//! Atlas core: the local Automerge engine for notes.

mod note;
mod storage;
mod sync;
mod vault;

pub use note::{NoteDoc, NoteError};
pub use storage::{FileStore, Store};
pub use sync::SyncReport;
pub use vault::{NoteSummary, Vault, VaultError};
