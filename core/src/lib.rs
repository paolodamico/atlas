//! Atlas core: the local Automerge engine for notes.

mod merge;
mod note;
mod remote;
mod storage;
mod vault;

pub use merge::MergeReport;
pub use note::{NoteDoc, NoteError};
pub use remote::{Cursor, Delta, SyncError, SyncOutcome, Transport, TransportError};
pub use storage::{FileStore, Store};
pub use vault::{NoteSummary, Vault, VaultError};
