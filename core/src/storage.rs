#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::VaultError;

/// Defines behavior of a note storage backend.
pub trait NoteStore: Send {
    /// Returns `Ok(None)` if no note with this id has been stored yet
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be read.
    fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError>;

    /// Stores (or replaces) a note's bytes.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be written.
    fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError>;

    /// Deletes a note's bytes. Deleting an id that isn't present is not an
    /// error.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be written.
    fn delete(&mut self, id: &str) -> Result<(), VaultError>;
}

/// Default storage engine using the host's filesystem.
///
/// Each note is stored as `<dir>/<id>.atlas`.
#[derive(Debug)]
pub struct FileNoteStore {
    dir: PathBuf,
}

impl FileNoteStore {
    /// Opens (creating if necessary) a note store rooted at `dir`.
    ///
    /// # Errors
    /// Returns an error if `dir` can't be created.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, VaultError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path_for(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.atlas"))
    }
}

impl NoteStore for FileNoteStore {
    fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError> {
        match std::fs::read(self.path_for(id)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError> {
        write_atomic(&self.path_for(id), &bytes)
    }

    fn delete(&mut self, id: &str) -> Result<(), VaultError> {
        match std::fs::remove_file(self.path_for(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Writes `bytes` to `path` via a same-directory temp file + rename, so a
/// process killed mid-write (routine on mobile) can't leave a
/// half-written, corrupt file at `path`. Rename is atomic on the POSIX
/// filesystems which iOS/macOS/Android all use.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), VaultError> {
    let Some(dir) = path.parent() else {
        return Err(VaultError::InvalidPath(path.to_path_buf()));
    };
    let Some(file_name) = path.file_name() else {
        return Err(VaultError::InvalidPath(path.to_path_buf()));
    };
    let tmp_path = dir.join(format!("{}.tmp", file_name.to_string_lossy()));
    std::fs::write(&tmp_path, bytes)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// In-memory [`NoteStore`], for tests only. Nothing here survives past the
/// process; real callers want [`FileNoteStore`] via [`Vault::open`](crate::Vault::open).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct InMemoryNoteStore(HashMap<String, Vec<u8>>);

#[cfg(test)]
impl NoteStore for InMemoryNoteStore {
    fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError> {
        Ok(self.0.get(id).cloned())
    }

    fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError> {
        self.0.insert(id.to_string(), bytes);
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<(), VaultError> {
        self.0.remove(id);
        Ok(())
    }
}
