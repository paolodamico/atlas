//! Integration tests for multi-device flows
#![expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]

use atlas_core::{FileStore, Vault};
use tempfile::TempDir;

/// A device simulated as an on-disk vault plus the temp dir backing it.
struct Device {
    vault: Vault,
    dir: TempDir,
}

impl Device {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::load(FileStore::new(dir.path()).unwrap()).unwrap();
        Self { vault, dir }
    }

    /// Reopens this device's vault fresh from disk, dropping in-memory state.
    /// Takes `&self` so the backing temp dir stays alive.
    fn reopen(&self) -> Vault {
        Vault::load(FileStore::new(self.dir.path()).unwrap()).unwrap()
    }
}

fn body(vault: &Vault, id: &str) -> String {
    vault.get_note(id).unwrap().body().unwrap()
}

#[test]
fn pull_copies_a_note_from_the_other_device() {
    let mut a = Device::new();
    let mut b = Device::new();
    let (id, _) = a.vault.create_note("n.md", "N", "created on A").unwrap();

    let report = b.vault.merge_from(&a.vault).unwrap();

    assert_eq!(report.pulled, 1);
    assert_eq!(report.merged, 0);
    assert_eq!(body(&b.vault, &id), "created on A");

    // The receiver persisted: the note survives reopening B from disk.
    let reopened = b.reopen();
    assert_eq!(body(&reopened, &id), "created on A");
}

#[test]
fn merge_from_leaves_the_remote_untouched() {
    let mut a = Device::new();
    let mut b = Device::new();
    a.vault.create_note("a.md", "A", "only on A").unwrap();

    // B pulls from A. A must be unaffected, on disk and in memory.
    b.vault.merge_from(&a.vault).unwrap();

    assert_eq!(a.vault.list_notes(0, 10).len(), 1);
    assert_eq!(a.reopen().list_notes(0, 10).len(), 1);
}

#[test]
fn two_way_reconcile_unions_independently_created_notes() {
    let mut a = Device::new();
    let mut b = Device::new();
    a.vault.create_note("a.md", "A", "from A").unwrap();
    b.vault.create_note("b.md", "B", "from B").unwrap();

    // Each side pulls the other, as two real peers would.
    a.vault.merge_from(&b.vault).unwrap();
    b.vault.merge_from(&a.vault).unwrap();

    let mut a_titles: Vec<_> = a
        .vault
        .list_notes(0, 10)
        .into_iter()
        .map(|s| s.title)
        .collect();
    let mut b_titles: Vec<_> = b
        .vault
        .list_notes(0, 10)
        .into_iter()
        .map(|s| s.title)
        .collect();
    a_titles.sort();
    b_titles.sort();
    assert_eq!(a_titles, vec!["A", "B"]);
    assert_eq!(b_titles, vec!["A", "B"]);
}

#[test]
fn concurrent_edits_to_the_same_note_merge_without_loss() {
    let mut a = Device::new();
    let mut b = Device::new();
    let (id, _) = a.vault.create_note("n.md", "N", "one two three").unwrap();
    // Hand the note to B first, so both start from the same base.
    b.vault.merge_from(&a.vault).unwrap();

    // Each device edits a disjoint region of the same note, offline.
    let mut on_a = a.vault.get_note(&id).unwrap();
    on_a.splice(0, 3, "ONE").unwrap();
    a.vault.update_note(&id, &mut on_a).unwrap();

    let mut on_b = b.vault.get_note(&id).unwrap();
    on_b.splice(8, 5, "THREE").unwrap();
    b.vault.update_note(&id, &mut on_b).unwrap();

    let report_a = a.vault.merge_from(&b.vault).unwrap();
    let report_b = b.vault.merge_from(&a.vault).unwrap();

    assert_eq!(report_a.merged, 1);
    assert_eq!(report_b.merged, 1);
    assert_eq!(body(&a.vault, &id), "ONE two THREE");
    assert_eq!(body(&b.vault, &id), "ONE two THREE");
}

#[test]
fn pulling_after_reopening_from_disk_converges() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let (id, _) = {
        let mut a = Vault::load(FileStore::new(dir_a.path()).unwrap()).unwrap();
        a.create_note("n.md", "N", "durable").unwrap()
    };

    // Reopen both devices fresh from disk, then B pulls from A.
    let a = Vault::load(FileStore::new(dir_a.path()).unwrap()).unwrap();
    let mut b = Vault::load(FileStore::new(dir_b.path()).unwrap()).unwrap();
    b.merge_from(&a).unwrap();
    drop(b);

    // Reopen B once more: the pulled note persisted.
    let b = Vault::load(FileStore::new(dir_b.path()).unwrap()).unwrap();
    assert_eq!(b.get_note(&id).unwrap().body().unwrap(), "durable");
}
