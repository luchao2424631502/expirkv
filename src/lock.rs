//! Root-directory exclusivity and path identity.
#![allow(dead_code)] // Stage 7 capability; public Open wiring is added later.

use std::collections::HashSet;
use std::fs::{self, File, Metadata, OpenOptions, ReadDir};
use std::io;
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

const LOCK_FILE_NAME: &str = "LOCK";
const LOCK_EXCLUSIVE: c_int = 2;
const LOCK_NONBLOCKING: c_int = 4;
const LOCK_UNLOCK: c_int = 8;
#[cfg(target_os = "macos")]
const FULL_FILE_SYNC: c_int = 51;

#[cfg(target_os = "linux")]
const OPEN_NOFOLLOW: c_int = 0x0002_0000;
#[cfg(target_os = "macos")]
const OPEN_NOFOLLOW: c_int = 0x0000_0100;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("RustKV root locking currently supports only Linux and macOS");

unsafe extern "C" {
    #[link_name = "flock"]
    fn os_flock(file_descriptor: c_int, operation: c_int) -> c_int;

    #[cfg(target_os = "macos")]
    #[link_name = "fcntl"]
    fn os_fcntl(file_descriptor: c_int, command: c_int) -> c_int;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RootPathIdentity {
    pub(crate) canonical_path: PathBuf,
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagedEntryKind {
    Missing,
    RegularFile { len: u64 },
    Directory,
    Symlink,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootCreationFault {
    None,
    #[cfg(test)]
    BeforeParentSync,
}

pub(crate) struct RootLock {
    identity: RootPathIdentity,
    root_directory: File,
    lock_file: Option<File>,
}

impl RootLock {
    /// Resolves one database root identity and obtains both the process-local
    /// reservation and the non-blocking OS lock. A missing root is created only
    /// when `create_root` is true.
    pub(crate) fn acquire(path: &Path, should_create_root: bool) -> Result<Option<Self>> {
        Self::acquire_inner(path, should_create_root, RootCreationFault::None)
    }

    #[cfg(test)]
    pub(crate) fn acquire_with_parent_sync_failure_for_test(path: &Path) -> Result<Option<Self>> {
        Self::acquire_inner(path, true, RootCreationFault::BeforeParentSync)
    }

    fn acquire_inner(
        path: &Path,
        should_create_root: bool,
        creation_fault: RootCreationFault,
    ) -> Result<Option<Self>> {
        let canonical_path = match fs::symlink_metadata(path) {
            Ok(_) => fs::canonicalize(path).map_err(open_io_error)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound && !should_create_root => {
                return Ok(None);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                create_root(path, creation_fault)?
            }
            Err(error) => return Err(open_io_error(error)),
        };

        let root_directory = open_directory_nofollow(&canonical_path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotADirectory {
                layout_error()
            } else {
                open_io_error(error)
            }
        })?;
        let metadata = root_directory.metadata().map_err(open_io_error)?;
        if !metadata.file_type().is_dir() {
            return Err(layout_error());
        }
        let file_identity = identity_from_metadata(&metadata);
        let identity = RootPathIdentity {
            canonical_path,
            device: file_identity.device,
            inode: file_identity.inode,
        };

        {
            let mut locked = process_lock_table();
            if !locked.insert(file_identity) {
                return Err(busy_error());
            }
        }

        let lock_file = match open_and_lock_file(&identity.canonical_path) {
            Ok(file) => file,
            Err(error) => {
                process_lock_table().remove(&file_identity);
                return Err(error);
            }
        };

        Ok(Some(Self {
            identity,
            root_directory,
            lock_file: Some(lock_file),
        }))
    }

    pub(crate) fn identity(&self) -> &RootPathIdentity {
        &self.identity
    }

    pub(crate) fn canonical_path(&self) -> &Path {
        &self.identity.canonical_path
    }

    pub(crate) fn inspect_child(&self, name: &str) -> Result<ManagedEntryKind> {
        let path = self.checked_child_path(name)?;
        match fs::symlink_metadata(path) {
            Ok(metadata) => Ok(classify_metadata(&metadata)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ManagedEntryKind::Missing),
            Err(error) => Err(open_io_error(error)),
        }
    }

    pub(crate) fn open_existing_regular(&self, name: &str) -> Result<Option<File>> {
        let path = self.checked_child_path(name)?;
        match self.inspect_child(name)? {
            ManagedEntryKind::Missing => Ok(None),
            ManagedEntryKind::RegularFile { .. } => open_regular_nofollow(&path, false)
                .map(Some)
                .map_err(open_io_error),
            _ => Err(layout_error()),
        }
    }

    pub(crate) fn create_new_regular(&self, name: &str) -> Result<File> {
        let path = self.checked_child_path(name)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(OPEN_NOFOLLOW)
            .open(path)
            .map_err(open_io_error)?;
        if !file
            .metadata()
            .map_err(open_io_error)?
            .file_type()
            .is_file()
        {
            return Err(layout_error());
        }
        Ok(file)
    }

    pub(crate) fn create_directory(&self, name: &str) -> Result<()> {
        let path = self.checked_child_path(name)?;
        fs::create_dir(path).map_err(open_io_error)
    }

    pub(crate) fn read_directory(&self, name: &str) -> Result<ReadDir> {
        let path = self.checked_child_path(name)?;
        match self.inspect_child(name)? {
            ManagedEntryKind::Directory => fs::read_dir(path).map_err(open_io_error),
            _ => Err(layout_error()),
        }
    }

    pub(crate) fn sync_directory(&self, name: &str) -> Result<()> {
        let path = self.checked_child_path(name)?;
        let directory = open_directory_nofollow(&path).map_err(open_io_error)?;
        sync_directory_handle(&directory).map_err(open_io_error)
    }

    pub(crate) fn sync_root(&self) -> Result<()> {
        self.ensure_path_identity()?;
        sync_directory_handle(&self.root_directory).map_err(open_io_error)
    }

    pub(crate) fn rename_child(&self, from: &str, to: &str) -> Result<()> {
        let from = self.checked_child_path(from)?;
        let to = self.checked_child_path(to)?;
        fs::rename(from, to).map_err(open_io_error)
    }

    pub(crate) fn remove_regular_child_if_present(&self, name: &str) -> Result<()> {
        let path = self.checked_child_path(name)?;
        match self.inspect_child(name)? {
            ManagedEntryKind::Missing => Ok(()),
            ManagedEntryKind::RegularFile { .. } => fs::remove_file(path).map_err(open_io_error),
            _ => Err(layout_error()),
        }
    }

    pub(crate) fn remove_directory_tree_if_present(&self, name: &str) -> Result<()> {
        let path = self.checked_child_path(name)?;
        match self.inspect_child(name)? {
            ManagedEntryKind::Missing => Ok(()),
            ManagedEntryKind::Directory => fs::remove_dir_all(path).map_err(open_io_error),
            _ => Err(layout_error()),
        }
    }

    fn checked_child_path(&self, name: &str) -> Result<PathBuf> {
        self.ensure_path_identity()?;
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(layout_error());
        }
        Ok(self.identity.canonical_path.join(name))
    }

    fn ensure_path_identity(&self) -> Result<()> {
        let metadata = fs::metadata(&self.identity.canonical_path).map_err(open_io_error)?;
        if !metadata.file_type().is_dir()
            || metadata.dev() != self.identity.device
            || metadata.ino() != self.identity.inode
        {
            return Err(layout_error());
        }
        Ok(())
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        if let Some(file) = self.lock_file.take() {
            // SAFETY: `file` owns a valid descriptor for the locked regular file.
            let _ = unsafe { os_flock(file.as_raw_fd(), LOCK_UNLOCK) };
            drop(file);
        }
        process_lock_table().remove(&FileIdentity {
            device: self.identity.device,
            inode: self.identity.inode,
        });
    }
}

pub(crate) fn open_regular_nofollow(path: &Path, writable: bool) -> io::Result<File> {
    let before = fs::symlink_metadata(path)?;
    if !before.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed object is not a regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(OPEN_NOFOLLOW);
    if writable {
        options.write(true);
    }
    let file = options.open(path)?;
    let after = file.metadata()?;
    if !after.file_type().is_file() || before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed object identity changed while opening",
        ));
    }
    Ok(file)
}

pub(crate) fn open_directory_nofollow(path: &Path) -> io::Result<File> {
    let before = fs::symlink_metadata(path)?;
    if !before.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "managed object is not a directory",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(OPEN_NOFOLLOW)
        .open(path)?;
    let after = file.metadata()?;
    if !after.file_type().is_dir() || before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "managed directory identity changed while opening",
        ));
    }
    Ok(file)
}

pub(crate) fn sync_directory_tree_nofollow(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(open_io_error)?;
    if !metadata.file_type().is_dir() {
        return Err(layout_error());
    }
    for entry in fs::read_dir(path).map_err(open_io_error)? {
        let entry = entry.map_err(open_io_error)?;
        let child = entry.path();
        let file_type = entry.file_type().map_err(open_io_error)?;
        if file_type.is_dir() {
            sync_directory_tree_nofollow(&child)?;
        } else if file_type.is_file() {
            let file = open_regular_nofollow(&child, false).map_err(open_io_error)?;
            sync_file_data(&file).map_err(open_io_error)?;
        } else {
            return Err(layout_error());
        }
    }
    let directory = open_directory_nofollow(path).map_err(open_io_error)?;
    sync_directory_handle(&directory).map_err(open_io_error)
}

pub(crate) fn sync_file_data(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: `file` owns a live descriptor and F_FULLFSYNC retains no pointer.
        if unsafe { os_fcntl(file.as_raw_fd(), FULL_FILE_SYNC) } == 0 {
            return Ok(());
        }
        return file.sync_all();
    }
    #[cfg(target_os = "linux")]
    {
        file.sync_data()
    }
}

fn sync_directory_handle(directory: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // Some filesystems reject F_FULLFSYNC for directories; fsync is the
        // required fallback for persisting directory entries.
        // SAFETY: `directory` owns a live descriptor and the call retains no pointer.
        if unsafe { os_fcntl(directory.as_raw_fd(), FULL_FILE_SYNC) } == 0 {
            return Ok(());
        }
    }
    directory.sync_all()
}

pub(crate) fn open_io_error(error: io::Error) -> StorageError {
    let mut storage_error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    storage_error.os_code = error.raw_os_error();
    storage_error
}

pub(crate) fn layout_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidLayout,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

pub(crate) fn not_found_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::NotFound,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::DoNotRetry,
    )
}

pub(crate) fn invalid_argument_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidArgument,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn create_root(path: &Path, creation_fault: RootCreationFault) -> Result<PathBuf> {
    let name = path.file_name().ok_or_else(layout_error)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let canonical_parent =
        fs::canonicalize(parent.unwrap_or_else(|| Path::new("."))).map_err(open_io_error)?;
    let parent_directory = open_directory_nofollow(&canonical_parent).map_err(open_io_error)?;
    let candidate = canonical_parent.join(name);
    match fs::create_dir(&candidate) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(open_io_error(error)),
    }
    let canonical_candidate = fs::canonicalize(candidate).map_err(open_io_error)?;
    sync_created_root_parent(&parent_directory, creation_fault)?;
    Ok(canonical_candidate)
}

fn sync_created_root_parent(
    parent_directory: &File,
    _creation_fault: RootCreationFault,
) -> Result<()> {
    #[cfg(test)]
    if _creation_fault == RootCreationFault::BeforeParentSync {
        return Err(open_io_error(io::Error::other(
            "injected parent directory sync failure",
        )));
    }
    sync_directory_handle(parent_directory).map_err(open_io_error)
}

fn open_and_lock_file(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE_NAME);
    let file = match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            open_regular_nofollow(&path, true).map_err(open_io_error)?
        }
        Ok(_) => return Err(layout_error()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(OPEN_NOFOLLOW)
                .open(&path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    open_regular_nofollow(&path, true).map_err(open_io_error)?
                }
                Err(error) => return Err(open_io_error(error)),
            }
        }
        Err(error) => return Err(open_io_error(error)),
    };
    if !file
        .metadata()
        .map_err(open_io_error)?
        .file_type()
        .is_file()
    {
        return Err(layout_error());
    }

    // SAFETY: `file` owns a live descriptor, and these flock flags are valid on
    // both supported targets. The call does not retain a Rust pointer.
    if unsafe { os_flock(file.as_raw_fd(), LOCK_EXCLUSIVE | LOCK_NONBLOCKING) } != 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::WouldBlock {
            Err(busy_error())
        } else {
            Err(open_io_error(error))
        };
    }
    Ok(file)
}

fn identity_from_metadata(metadata: &Metadata) -> FileIdentity {
    FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

fn classify_metadata(metadata: &Metadata) -> ManagedEntryKind {
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        ManagedEntryKind::Symlink
    } else if file_type.is_file() {
        ManagedEntryKind::RegularFile {
            len: metadata.len(),
        }
    } else if file_type.is_dir() {
        ManagedEntryKind::Directory
    } else {
        ManagedEntryKind::Other
    }
}

fn busy_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Busy,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::RetrySameInstance,
    )
}

fn process_lock_table() -> std::sync::MutexGuard<'static, HashSet<FileIdentity>> {
    static PROCESS_LOCKS: OnceLock<Mutex<HashSet<FileIdentity>>> = OnceLock::new();
    let table = PROCESS_LOCKS.get_or_init(|| Mutex::new(HashSet::new()));
    match table.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
