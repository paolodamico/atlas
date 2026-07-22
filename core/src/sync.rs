//! Merging remote vault state into a local vault.
//!
//! This is the local foundation for multi-device sync: [`Vault::merge_from`]
//! folds another vault's state into this one and persists only this vault,
//! modelling "apply the changes I received from a peer to my local vault".
//! `other` stands in for a snapshot received from a peer (load the received
//! bytes into a [`Vault`], then merge it in). A full two-way reconcile is
//! what two peers each do: `a.merge_from(&b)` then `b.merge_from(&a)`.
//!
//! TODO: The networked relay and the Automerge sync-message protocol (efficient
//! deltas, rather than whole-doc merges) layer on top of this later.

use crate::{NoteDoc, Vault, VaultError};

/// What a [`Vault::merge_from`] pulled in, from the receiver's perspective.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// Notes present on both sides whose bodies were merged together.
    pub merged: usize,
    /// Notes copied in because only the other vault had them.
    pub pulled: usize,
}

impl Vault {
    /// Merges `other`'s state into this vault, persisting only this vault.
    ///
    /// Note metadata (which notes exist, their titles and paths) is merged
    /// first, then each shared note's body; notes only `other` has are pulled
    /// in. Concurrent edits to the same note are resolved by Automerge, never
    /// lost. `other` is left untouched. To bring two vaults fully into sync,
    /// call this from each side.
    ///
    /// # Errors
    /// Returns an error if any Automerge merge, store read/write, or persist
    /// fails. On failure this vault may be left partially reconciled.
    pub fn merge_from(&mut self, other: &Vault) -> Result<SyncReport, VaultError> {
        self.merge_root_from(other)?;
        self.persist()?;

        let mut report = SyncReport::default();
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
