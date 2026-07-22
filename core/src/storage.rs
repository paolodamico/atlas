#[cfg(test)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::{Arc, Mutex};

use crate::VaultError;

/// Defines behavior for a storage backend. The store is keyed by `id`.
///
/// This is generally used for both storing notes and the vault's root document.
pub trait Store: Send {
    /// Returns `Ok(None)` if nothing has been stored under this id yet.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be read.
    fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError>;

    /// Stores (or replaces) the bytes at `id`.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be written.
    fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError>;

    /// Deletes the bytes at `id`. Deleting an id that isn't present is not
    /// an error.
    ///
    /// # Errors
    /// Returns an error if the underlying storage can't be written.
    fn delete(&mut self, id: &str) -> Result<(), VaultError>;
}

/// Default [`Store`] using the host's filesystem.
///
/// Each id is stored as `<dir>/<id>.atlas`.
#[derive(Debug, Clone)]
pub struct FileStore {
    dir: PathBuf,
}

impl FileStore {
    /// Opens (creating if necessary) a store rooted at `dir`.
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

impl Store for FileStore {
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

/// In-memory [`Store`], for tests only. Clones shares the same state.
#[cfg(test)]
#[derive(Debug, Default, Clone)]
pub(crate) struct InMemoryStore(Arc<Mutex<HashMap<String, Vec<u8>>>>);

#[cfg(test)]
impl InMemoryStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<u8>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
impl Store for InMemoryStore {
    fn get(&self, id: &str) -> Result<Option<Vec<u8>>, VaultError> {
        Ok(self.lock().get(id).cloned())
    }

    fn put(&mut self, id: &str, bytes: Vec<u8>) -> Result<(), VaultError> {
        self.lock().insert(id.to_string(), bytes);
        Ok(())
    }

    fn delete(&mut self, id: &str) -> Result<(), VaultError> {
        self.lock().remove(id);
        Ok(())
    }
}
