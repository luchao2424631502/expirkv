//! Managed data-file set and bounded read-only handle cache.
#![allow(dead_code)] // Stage 8 boundary; Db assembly consumes it in later stages.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::{CString, c_char, c_int};
use std::fs::{File, Metadata};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::lock::open_directory_nofollow;
use crate::vlog::format::{
    FILE_HEADER_ENCODED_LEN, MAX_VLOG_FILE_ID, MAX_VLOG_FILE_SIZE, PAGE_HEADER_ENCODED_LEN,
    VLOG_PAGE_SIZE, VLogFileHeader, VLogGeometry,
};
use crate::vlog::writer::WriterFileCapability;
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

const EINTR_RETRY_LIMIT: usize = 8;
const TRANSIENT_OPEN_RETRY_LIMIT: usize = 3;
const EMFILE: i32 = 24;
const ENFILE: i32 = 23;
#[cfg(target_os = "linux")]
const ELOOP: i32 = 40;
#[cfg(target_os = "macos")]
const ELOOP: i32 = 62;
#[cfg(target_os = "macos")]
const FULL_FILE_SYNC: c_int = 51;

#[cfg(target_os = "linux")]
const OPEN_CREATE: c_int = 0o100;
#[cfg(target_os = "macos")]
const OPEN_CREATE: c_int = 0x0200;
#[cfg(target_os = "linux")]
const OPEN_EXCLUSIVE: c_int = 0o200;
#[cfg(target_os = "macos")]
const OPEN_EXCLUSIVE: c_int = 0x0800;
#[cfg(target_os = "linux")]
const OPEN_CLOEXEC: c_int = 0o2_000_000;
#[cfg(target_os = "macos")]
const OPEN_CLOEXEC: c_int = 0x0100_0000;
#[cfg(target_os = "linux")]
const OPEN_NOFOLLOW: c_int = 0o400_000;
#[cfg(target_os = "macos")]
const OPEN_NOFOLLOW: c_int = 0x0000_0100;
const OPEN_READ_ONLY: c_int = 0;
const OPEN_READ_WRITE: c_int = 2;

unsafe extern "C" {
    fn openat(directory_fd: c_int, path: *const c_char, flags: c_int, ...) -> c_int;
    fn unlinkat(directory_fd: c_int, path: *const c_char, flags: c_int) -> c_int;

    #[cfg(target_os = "macos")]
    #[link_name = "fcntl"]
    fn os_fcntl(file_descriptor: c_int, command: c_int) -> c_int;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct FileCatalog {
    entries: RwLock<BTreeMap<u32, FileIdentity>>,
}

impl FileCatalog {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&self, file_id: u32, file: &File) -> Result<()> {
        if file_id > MAX_VLOG_FILE_ID {
            return Err(read_corruption(file_id, None));
        }
        let metadata = file
            .metadata()
            .map_err(|error| read_io(file_id, None, error))?;
        if !metadata.file_type().is_file() {
            return Err(read_corruption(file_id, None));
        }
        let identity = FileIdentity::from_metadata(&metadata);
        let mut entries = write_lock(&self.entries)?;
        if entries
            .get(&file_id)
            .is_some_and(|current| *current != identity)
        {
            return Err(read_corruption(file_id, None));
        }
        entries.insert(file_id, identity);
        Ok(())
    }

    pub(crate) fn unregister(&self, file_id: u32) -> Result<()> {
        write_lock(&self.entries)?.remove(&file_id);
        Ok(())
    }

    pub(crate) fn verify(&self, file_id: u32, file: &File) -> Result<()> {
        let expected = self.identity(file_id)?;
        let metadata = file
            .metadata()
            .map_err(|error| read_io(file_id, None, error))?;
        if !metadata.file_type().is_file() || FileIdentity::from_metadata(&metadata) != expected {
            return Err(read_corruption(file_id, None));
        }
        Ok(())
    }

    pub(crate) fn file_ids(&self) -> Result<Vec<u32>> {
        Ok(read_lock(&self.entries)?.keys().copied().collect())
    }

    fn identity(&self, file_id: u32) -> Result<FileIdentity> {
        read_lock(&self.entries)?
            .get(&file_id)
            .copied()
            .ok_or_else(|| read_corruption(file_id, None))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, file_id: u32) -> bool {
        read_lock(&self.entries)
            .map(|entries| entries.contains_key(&file_id))
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct VLogDirectory {
    path: PathBuf,
    handle: File,
    identity: FileIdentity,
}

impl VLogDirectory {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let handle = open_directory_nofollow(path).map_err(directory_io)?;
        let identity = FileIdentity::from_metadata(&handle.metadata().map_err(directory_io)?);
        Ok(Self {
            path: path.to_path_buf(),
            handle,
            identity,
        })
    }

    pub(crate) fn writer_identity(&self) -> (u64, u64) {
        (self.identity.device, self.identity.inode)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn open_read_only(&self, file_id: u32) -> io::Result<File> {
        let name = vlog_file_name_c(file_id)?;
        self.open_at(&name, OPEN_READ_ONLY | OPEN_CLOEXEC | OPEN_NOFOLLOW, 0)
    }

    pub(super) fn open_writable(
        &self,
        _capability: &WriterFileCapability,
        file_id: u32,
    ) -> io::Result<File> {
        let name = vlog_file_name_c(file_id)?;
        self.open_at(&name, OPEN_READ_WRITE | OPEN_CLOEXEC | OPEN_NOFOLLOW, 0)
    }

    pub(super) fn create_new(
        &self,
        _capability: &WriterFileCapability,
        file_id: u32,
    ) -> io::Result<File> {
        let name = vlog_file_name_c(file_id)?;
        self.open_at(
            &name,
            OPEN_READ_WRITE | OPEN_CREATE | OPEN_EXCLUSIVE | OPEN_CLOEXEC | OPEN_NOFOLLOW,
            0o600,
        )
    }

    pub(super) fn remove_file(
        &self,
        _capability: &WriterFileCapability,
        file_id: u32,
    ) -> io::Result<()> {
        let name = vlog_file_name_c(file_id)?;
        // SAFETY: `self.handle` owns a live directory descriptor, `name` is a
        // NUL-terminated single path component, and unlinkat does not retain it.
        let result = unsafe { unlinkat(self.handle.as_raw_fd(), name.as_ptr(), 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    pub(crate) fn create_new_for_test(&self, file_id: u32) -> io::Result<File> {
        let name = vlog_file_name_c(file_id)?;
        self.open_at(
            &name,
            OPEN_READ_WRITE | OPEN_CREATE | OPEN_EXCLUSIVE | OPEN_CLOEXEC | OPEN_NOFOLLOW,
            0o600,
        )
    }

    #[cfg(test)]
    pub(crate) fn open_writable_for_test(&self, file_id: u32) -> io::Result<File> {
        let name = vlog_file_name_c(file_id)?;
        self.open_at(&name, OPEN_READ_WRITE | OPEN_CLOEXEC | OPEN_NOFOLLOW, 0)
    }

    pub(crate) fn sync(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        {
            // SAFETY: `self.handle` owns a live directory descriptor and
            // F_FULLFSYNC does not retain it. As with data-file sync, filesystems
            // that do not support F_FULLFSYNC fall back to ordinary fsync.
            if unsafe { os_fcntl(self.handle.as_raw_fd(), FULL_FILE_SYNC) } == 0 {
                return Ok(());
            }
        }
        self.handle.sync_all()
    }

    fn open_at(&self, name: &CString, flags: c_int, mode: c_int) -> io::Result<File> {
        // SAFETY: `self.handle` owns a live directory descriptor, `name` is a
        // NUL-terminated single path component, and openat does not retain it.
        let descriptor = unsafe { openat(self.handle.as_raw_fd(), name.as_ptr(), flags, mode) };
        if descriptor < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: a successful openat returns a new owned descriptor.
            Ok(unsafe { File::from_raw_fd(descriptor) })
        }
    }
}

#[cfg(test)]
pub(crate) trait HandleOpener: Send + Sync {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File>;
}

#[cfg(test)]
#[derive(Debug, Default)]
struct SystemHandleOpener;

#[cfg(test)]
impl HandleOpener for SystemHandleOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        directory.open_read_only(file_id)
    }
}

#[derive(Debug, Default)]
struct HandleCacheInner {
    handles: HashMap<u32, Arc<File>>,
    insertion_order: VecDeque<u32>,
}

#[derive(Debug)]
struct ReadHandleCache {
    capacity: usize,
    inner: RwLock<HandleCacheInner>,
}

impl ReadHandleCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: RwLock::new(HandleCacheInner::default()),
        }
    }

    fn hit(&self, file_id: u32) -> Result<Option<Arc<File>>> {
        Ok(read_lock(&self.inner)?.handles.get(&file_id).cloned())
    }

    fn insert_or_get(&self, file_id: u32, candidate: Arc<File>) -> Result<Arc<File>> {
        if self.capacity == 0 {
            return Ok(candidate);
        }

        let evicted = {
            let mut inner = write_lock(&self.inner)?;
            if let Some(existing) = inner.handles.get(&file_id) {
                return Ok(Arc::clone(existing));
            }

            let evicted = if inner.handles.len() == self.capacity {
                let oldest = inner
                    .insertion_order
                    .pop_front()
                    .ok_or_else(cache_internal_error)?;
                inner.handles.remove(&oldest)
            } else {
                None
            };
            inner.handles.insert(file_id, Arc::clone(&candidate));
            inner.insertion_order.push_back(file_id);
            evicted
        };
        drop(evicted);
        Ok(candidate)
    }

    fn clear(&self) -> Result<Vec<Arc<File>>> {
        let mut inner = write_lock(&self.inner)?;
        let handles = inner.handles.drain().map(|(_, file)| file).collect();
        inner.insertion_order.clear();
        Ok(handles)
    }

    fn remove(&self, file_id: u32) -> Result<Option<Arc<File>>> {
        let removed = {
            let mut inner = write_lock(&self.inner)?;
            inner.insertion_order.retain(|current| *current != file_id);
            inner.handles.remove(&file_id)
        };
        Ok(removed)
    }

    #[cfg(test)]
    fn len(&self) -> Result<usize> {
        Ok(read_lock(&self.inner)?.handles.len())
    }

    #[cfg(test)]
    fn order(&self) -> Result<Vec<u32>> {
        Ok(read_lock(&self.inner)?
            .insertion_order
            .iter()
            .copied()
            .collect())
    }
}

pub(crate) struct FileSet {
    directory: Arc<VLogDirectory>,
    database_uuid: [u8; 16],
    geometry: VLogGeometry,
    catalog: Arc<FileCatalog>,
    read_cache: ReadHandleCache,
    #[cfg(test)]
    opener: Arc<dyn HandleOpener>,
}

impl FileSet {
    pub(crate) fn new(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        capacity: usize,
    ) -> Result<Self> {
        if database_uuid == [0; 16] {
            return Err(read_corruption(0, None));
        }
        crate::vlog::format::LayoutPlanner::empty(geometry)
            .map_err(|error| read_context(error, None, None))?;
        Ok(Self {
            directory,
            database_uuid,
            geometry,
            catalog,
            read_cache: ReadHandleCache::new(capacity),
            #[cfg(test)]
            opener: Arc::new(SystemHandleOpener),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_opener(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        capacity: usize,
        opener: Arc<dyn HandleOpener>,
    ) -> Result<Self> {
        let mut files = Self::new(directory, database_uuid, geometry, catalog, capacity)?;
        files.opener = opener;
        Ok(files)
    }

    pub(crate) fn handle(&self, file_id: u32) -> Result<Arc<File>> {
        let expected_identity = self.catalog.identity(file_id)?;
        if let Some(handle) = self.read_cache.hit(file_id)? {
            return Ok(handle);
        }

        let candidate = Arc::new(self.open_candidate(file_id, expected_identity)?);
        self.read_cache.insert_or_get(file_id, candidate)
    }

    pub(crate) fn geometry(&self) -> VLogGeometry {
        self.geometry
    }

    pub(crate) fn database_uuid(&self) -> [u8; 16] {
        self.database_uuid
    }

    pub(crate) fn directory(&self) -> &Arc<VLogDirectory> {
        &self.directory
    }

    pub(crate) fn catalog(&self) -> &Arc<FileCatalog> {
        &self.catalog
    }

    pub(crate) fn evict(&self, file_id: u32) -> Result<()> {
        let removed = self.read_cache.remove(file_id)?;
        drop(removed);
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let handles = self.read_cache.clear()?;
        drop(handles);
        Ok(())
    }

    fn open_candidate(&self, file_id: u32, expected: FileIdentity) -> Result<File> {
        let mut emfile_retried = false;
        let mut interrupted_attempts = 0_usize;
        let mut transient_attempts = 0_usize;
        loop {
            match self.open_read_handle(file_id) {
                Ok(file) => {
                    self.validate_candidate(file_id, expected, &file)?;
                    return Ok(file);
                }
                Err(error) if error.raw_os_error() == Some(EMFILE) && !emfile_retried => {
                    emfile_retried = true;
                    let handles = self.read_cache.clear()?;
                    drop(handles);
                }
                Err(error)
                    if error.kind() == io::ErrorKind::Interrupted
                        && interrupted_attempts < EINTR_RETRY_LIMIT =>
                {
                    interrupted_attempts += 1;
                }
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        && transient_attempts < TRANSIENT_OPEN_RETRY_LIMIT =>
                {
                    transient_attempts += 1;
                }
                Err(error)
                    if error.raw_os_error() == Some(ENFILE)
                        && transient_attempts < TRANSIENT_OPEN_RETRY_LIMIT =>
                {
                    transient_attempts += 1;
                }
                Err(error) => return Err(classify_open_error(file_id, error)),
            }
        }
    }

    fn open_read_handle(&self, file_id: u32) -> io::Result<File> {
        #[cfg(test)]
        {
            self.opener.open(&self.directory, file_id)
        }
        #[cfg(not(test))]
        {
            self.directory.open_read_only(file_id)
        }
    }

    fn validate_candidate(&self, file_id: u32, expected: FileIdentity, file: &File) -> Result<()> {
        let metadata = file
            .metadata()
            .map_err(|error| read_io(file_id, None, error))?;
        if !metadata.file_type().is_file()
            || FileIdentity::from_metadata(&metadata) != expected
            || metadata.len() > self.geometry.max_file_size
        {
            return Err(read_corruption(file_id, None));
        }

        // The verified catalog already established the page topology during
        // Open. A random Get validates only the immutable FileHeader here; it
        // never adds a target-page PageHeader read to the query path.
        let mut encoded_header = [0_u8; FILE_HEADER_ENCODED_LEN];
        read_exact_at(
            file,
            &mut encoded_header,
            PAGE_HEADER_ENCODED_LEN as u64,
            file_id,
        )?;
        let file_header = VLogFileHeader::decode(&encoded_header).map_err(|error| {
            read_context(error, Some(file_id), Some(PAGE_HEADER_ENCODED_LEN as u64))
        })?;
        if file_header.file_id != file_id
            || file_header.database_uuid != self.database_uuid
            || u64::from(file_header.page_size) != VLOG_PAGE_SIZE
            || file_header.max_file_size != MAX_VLOG_FILE_SIZE
        {
            return Err(read_corruption(file_id, None));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn cache_len(&self) -> Result<usize> {
        self.read_cache.len()
    }

    #[cfg(test)]
    pub(crate) fn cache_order(&self) -> Result<Vec<u32>> {
        self.read_cache.order()
    }
}

pub(crate) fn vlog_file_name(file_id: u32) -> Result<String> {
    if file_id > MAX_VLOG_FILE_ID {
        return Err(read_corruption(file_id, None));
    }
    Ok(format!("D{file_id:06}.data"))
}

fn vlog_file_name_c(file_id: u32) -> io::Result<CString> {
    if file_id > MAX_VLOG_FILE_ID {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "VLog file id exceeds the frozen range",
        ));
    }
    CString::new(format!("D{file_id:06}.data"))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid VLog file name"))
}

pub(crate) fn read_exact_at(
    file: &File,
    buffer: &mut [u8],
    offset: u64,
    file_id: u32,
) -> Result<()> {
    use std::os::unix::fs::FileExt;

    read_exact_at_impl(
        |buffer, offset| file.read_at(buffer, offset),
        buffer,
        offset,
        file_id,
    )
}

#[cfg(test)]
pub(crate) trait PositionedRead: Send + Sync {
    fn read_at(&self, file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize>;
}

#[cfg(test)]
pub(crate) fn read_exact_at_with(
    reader: &dyn PositionedRead,
    file: &File,
    buffer: &mut [u8],
    offset: u64,
    file_id: u32,
) -> Result<()> {
    read_exact_at_impl(
        |buffer, offset| reader.read_at(file, buffer, offset),
        buffer,
        offset,
        file_id,
    )
}

fn read_exact_at_impl(
    mut read_at: impl FnMut(&mut [u8], u64) -> io::Result<usize>,
    mut buffer: &mut [u8],
    mut offset: u64,
    file_id: u32,
) -> Result<()> {
    let mut interrupted_attempts = 0_usize;
    let mut transient_attempts = 0_usize;
    while !buffer.is_empty() {
        match read_at(buffer, offset) {
            Ok(0) => return Err(read_corruption(file_id, Some(offset))),
            Ok(read) => {
                let read_u64 =
                    u64::try_from(read).map_err(|_| read_corruption(file_id, Some(offset)))?;
                offset = offset
                    .checked_add(read_u64)
                    .ok_or_else(|| read_corruption(file_id, Some(offset)))?;
                buffer = buffer
                    .get_mut(read..)
                    .ok_or_else(|| read_corruption(file_id, Some(offset)))?;
                interrupted_attempts = 0;
                transient_attempts = 0;
            }
            Err(error)
                if error.kind() == io::ErrorKind::Interrupted
                    && interrupted_attempts < EINTR_RETRY_LIMIT =>
            {
                interrupted_attempts += 1;
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && transient_attempts < TRANSIENT_OPEN_RETRY_LIMIT =>
            {
                transient_attempts += 1;
            }
            Err(error) => return Err(read_io(file_id, Some(offset), error)),
        }
    }
    Ok(())
}

fn classify_open_error(file_id: u32, error: io::Error) -> StorageError {
    if error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(ELOOP) {
        return read_corruption(file_id, None);
    }
    let kind = if matches!(error.raw_os_error(), Some(EMFILE) | Some(ENFILE))
        || matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
        ) {
        StorageErrorKind::ResourceExhausted
    } else {
        StorageErrorKind::Io
    };
    let retry_advice = if kind == StorageErrorKind::ResourceExhausted {
        RetryAdvice::RetrySameInstance
    } else {
        RetryAdvice::FixEnvironmentAndReopen
    };
    let mut storage_error = StorageError::codec_error(
        kind,
        Operation::Get,
        ProtocolStage::Read,
        None,
        retry_advice,
    );
    storage_error.os_code = error.raw_os_error();
    storage_error.vlog_file_id = Some(file_id);
    storage_error
}

fn directory_io(error: io::Error) -> StorageError {
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

fn read_io(file_id: u32, offset: Option<u64>, error: io::Error) -> StorageError {
    let kind = if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) {
        StorageErrorKind::ResourceExhausted
    } else {
        StorageErrorKind::Io
    };
    let mut storage_error = StorageError::codec_error(
        kind,
        Operation::Get,
        ProtocolStage::Read,
        None,
        if kind == StorageErrorKind::ResourceExhausted {
            RetryAdvice::RetrySameInstance
        } else {
            RetryAdvice::FixEnvironmentAndReopen
        },
    );
    storage_error.os_code = error.raw_os_error();
    storage_error.vlog_file_id = Some(file_id);
    storage_error.vlog_offset = offset;
    storage_error
}

fn read_context(
    mut error: StorageError,
    file_id: Option<u32>,
    offset: Option<u64>,
) -> StorageError {
    error.operation = Operation::Get;
    error.protocol_stage = ProtocolStage::Read;
    error.write_outcome = None;
    error.instance_state = None;
    error.vlog_file_id = file_id;
    error.vlog_offset = offset;
    error
}

pub(crate) fn read_corruption(file_id: u32, offset: Option<u64>) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Corruption,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::RestoreOrRepair,
    );
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = offset;
    error
}

fn cache_internal_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn read_lock<T>(lock: &RwLock<T>) -> Result<RwLockReadGuard<'_, T>> {
    lock.read().map_err(|_| cache_internal_error())
}

fn write_lock<T>(lock: &RwLock<T>) -> Result<RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|_| cache_internal_error())
}
