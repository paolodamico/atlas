//! Merging one local vault into another, whole doc at a time.
//!
//! [`Vault::merge_from`] folds `other`'s state into this vault and persists
//! only this vault, for reconciling two vault directories on the same machine.
//! Incremental sync with a relay lives in [`crate::remote`].

use crate::{NoteDoc, Vault, VaultError};

/// What a [`Vault::merge_from`] pulled in, from the receiver's perspective.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MergeReport {
    /// Notes present on both sides whose bodies were merged together.
    pub merged: usize,
    /// Notes copied in because only the other vault had them.
    pub pulled: usize,
}

impl Vault {
    /// Merges `other`'s state into this vault, persisting the current vault.
    ///
    /// Note metadata (which notes exist, their titles and paths) is merged
    /// first, then each shared note's body; notes only `other` has are pulled
    /// in. Concurrent edits to the same note are resolved by `automerge`, never
    /// lost. `other` is left untouched.
    ///
    /// # Errors
    /// Returns an error if any merge, store read/write, or persist fails. On
    /// failure this vault may be left partially reconciled.
    pub fn merge_from(&mut self, other: &Vault) -> Result<MergeReport, VaultError> {
        self.merge_root_from(other)?;
        self.persist()?;

        let mut report = MergeReport::default();
        for id in self.note_ids() {
            match (self.store_get(&id)?, other.store_get(&id)?) {
                (Some(here), Some(there)) => {
                    let mut note = NoteDoc::load(&here)?;
                    let mut incoming = NoteDoc::load(&there)?;
                    note.merge(&mut incoming)?;
                    self.store_put(&id, note.to_bytes())?;
                    report.merged += 1;
                }
                (None, Some(there)) => {
                    self.store_put(&id, there)?;
                    report.pulled += 1;
                }
                (_, None) => {}
            }
        }
        Ok(report)
    }
}
