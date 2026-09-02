//! Registered benchmark directories, clonefile-first restore, and safe cleanup.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use crate::BackendKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FsErrorKind {
    AlreadyExists,
    InvalidPath,
    Unregistered,
    WrongRole,
    Busy,
    UnsafeLayout,
    CopyFailed,
    LayoutMismatch,
    Io,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsError {
    kind: FsErrorKind,
    message: String,
}

impl FsError {
    fn new(kind: FsErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> FsErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for FsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for FsError {}

#[derive(Clone)]
pub struct BenchmarkWorkspace {
    inner: Arc<WorkspaceInner>,
}

impl fmt::Debug for BenchmarkWorkspace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BenchmarkWorkspace")
            .field("root", &self.inner.root)
            .finish_non_exhaustive()
    }
}

struct WorkspaceInner {
    root: PathBuf,
    entries: Mutex<BTreeMap<PathBuf, RegisteredEntry>>,
    next_build_id: Mutex<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectoryRole {
    TemplateBuild,
    Template,
    Run,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegisteredEntry {
    role: DirectoryRole,
    backend_kind: BackendKind,
    open: bool,
    root_device: u64,
    root_inode: u64,
}

#[derive(Clone)]
pub struct DatabaseDirectory {
    workspace: Arc<WorkspaceInner>,
    path: PathBuf,
    backend_kind: BackendKind,
    root_device: u64,
    root_inode: u64,
}

impl fmt::Debug for DatabaseDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseDirectory")
            .field("path", &self.path)
            .field("backend_kind", &self.backend_kind)
            .finish_non_exhaustive()
    }
}

impl DatabaseDirectory {
    pub const fn backend_kind(&self) -> BackendKind {
        self.backend_kind
    }

    #[doc(hidden)]
    pub fn path_for_test(&self) -> &Path {
        &self.path
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn begin_open(&self) -> Result<OpenLease, FsError> {
        let metadata = managed_root_metadata(&self.path)?;
        let mut entries = self.workspace.entries.lock().map_err(|_| {
            FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
        })?;
        let entry = entries.get_mut(&self.path).ok_or_else(|| {
            FsError::new(
                FsErrorKind::Unregistered,
                format!("directory is not registered: {}", self.path.display()),
            )
        })?;
        if entry.backend_kind != self.backend_kind {
            return Err(FsError::new(
                FsErrorKind::Unregistered,
                "directory Backend identity does not match its registry entry",
            ));
        }
        if entry.root_device != self.root_device || entry.root_inode != self.root_inode {
            return Err(FsError::new(
                FsErrorKind::Unregistered,
                "directory token no longer identifies the registered database root",
            ));
        }
        if entry.root_device != metadata.dev() || entry.root_inode != metadata.ino() {
            return Err(FsError::new(
                FsErrorKind::UnsafeLayout,
                "database directory root identity changed after registration",
            ));
        }
        if entry.open {
            return Err(FsError::new(
                FsErrorKind::Busy,
                format!(
                    "database directory is already open: {}",
                    self.path.display()
                ),
            ));
        }
        entry.open = true;
        Ok(OpenLease {
            workspace: Arc::clone(&self.workspace),
            path: self.path.clone(),
        })
    }

    pub(crate) fn require_existing_database(&self) -> Result<(), FsError> {
        BenchmarkWorkspace {
            inner: Arc::clone(&self.workspace),
        }
        .require_existing_database(self)
    }
}

pub(crate) struct OpenLease {
    workspace: Arc<WorkspaceInner>,
    path: PathBuf,
}

impl Drop for OpenLease {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.workspace.entries.lock()
            && let Some(entry) = entries.get_mut(&self.path)
        {
            entry.open = false;
        }
    }
}

impl BenchmarkWorkspace {
    /// Creates a new, exclusively benchmark-owned workspace. Existing paths
    /// are rejected so no user directory can be adopted accidentally.
    pub fn create(root: impl AsRef<Path>) -> Result<Self, FsError> {
        let requested = root.as_ref();
        if !requested.is_absolute() || requested.file_name().is_none() {
            return Err(FsError::new(
                FsErrorKind::InvalidPath,
                "benchmark workspace path must be an absolute non-root path",
            ));
        }
        let parent = requested.parent().ok_or_else(|| {
            FsError::new(
                FsErrorKind::InvalidPath,
                "benchmark workspace has no parent",
            )
        })?;
        let parent =
            fs::canonicalize(parent).map_err(|error| io_error("canonicalize", parent, error))?;
        let root = parent.join(requested.file_name().expect("checked above"));
        match fs::symlink_metadata(&root) {
            Ok(_) => {
                return Err(FsError::new(
                    FsErrorKind::AlreadyExists,
                    format!("benchmark workspace already exists: {}", root.display()),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect", &root, error)),
        }
        fs::create_dir(&root).map_err(|error| io_error("create", &root, error))?;
        Ok(Self {
            inner: Arc::new(WorkspaceInner {
                root,
                entries: Mutex::new(BTreeMap::new()),
                next_build_id: Mutex::new(0),
            }),
        })
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub fn create_empty_run(
        &self,
        backend_kind: BackendKind,
        label: &str,
    ) -> Result<DatabaseDirectory, FsError> {
        let path = self.run_path(label)?;
        self.create_and_register(path, DirectoryRole::Run, backend_kind)
    }

    pub fn cleanup_run(&self, directory: &DatabaseDirectory) -> Result<(), FsError> {
        self.remove_registered(directory, DirectoryRole::Run)
    }

    pub(crate) fn create_template_build(
        &self,
        backend_kind: BackendKind,
    ) -> Result<DatabaseDirectory, FsError> {
        let id = self.next_internal_id()?;
        let path = self
            .inner
            .root
            .join(format!(".{}-template-building-{id}", backend_kind.as_str()));
        self.create_and_register(path, DirectoryRole::TemplateBuild, backend_kind)
    }

    pub(crate) fn publish_template(
        &self,
        build: &DatabaseDirectory,
    ) -> Result<DatabaseDirectory, FsError> {
        self.require_entry(build, DirectoryRole::TemplateBuild, false)?;
        scan_directory(&build.path)?;
        let destination = self
            .inner
            .root
            .join(format!("template-{}", build.backend_kind.as_str()));
        require_absent(&destination)?;
        fs::rename(&build.path, &destination)
            .map_err(|error| io_error("publish template", &destination, error))?;

        let mut entries = self.inner.entries.lock().map_err(|_| {
            FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
        })?;
        let build_entry = entries.remove(&build.path).ok_or_else(|| {
            FsError::new(
                FsErrorKind::Unregistered,
                "template build registration vanished",
            )
        })?;
        entries.insert(
            destination.clone(),
            RegisteredEntry {
                role: DirectoryRole::Template,
                backend_kind: build.backend_kind,
                open: false,
                root_device: build_entry.root_device,
                root_inode: build_entry.root_inode,
            },
        );
        Ok(DatabaseDirectory {
            workspace: Arc::clone(&self.inner),
            path: destination,
            backend_kind: build.backend_kind,
            root_device: build_entry.root_device,
            root_inode: build_entry.root_inode,
        })
    }

    pub(crate) fn capture_manifest(
        &self,
        directory: &DatabaseDirectory,
    ) -> Result<DirectoryManifest, FsError> {
        self.require_registered(directory)?;
        scan_directory(&directory.path)
    }

    pub(crate) fn require_existing_database(
        &self,
        directory: &DatabaseDirectory,
    ) -> Result<(), FsError> {
        self.require_registered(directory)?;
        let manifest = scan_directory(&directory.path)?;
        if !manifest.contains_regular_file() {
            return Err(FsError::new(
                FsErrorKind::LayoutMismatch,
                "existing database directory contains no database files",
            ));
        }
        match directory.backend_kind {
            BackendKind::RustKv => {
                require_entry_kind(&directory.path.join("FORMAT"), ManifestKind::File)?;
                require_entry_kind(&directory.path.join("index"), ManifestKind::Directory)?;
                require_entry_kind(&directory.path.join("vlog"), ManifestKind::Directory)?;
            }
            BackendKind::LevelDb => {
                require_entry_kind(&directory.path.join("CURRENT"), ManifestKind::File)?;
            }
        }
        Ok(())
    }

    pub(crate) fn restore_template(
        &self,
        template: &DatabaseDirectory,
        expected: &DirectoryManifest,
        label: &str,
    ) -> Result<DatabaseDirectory, FsError> {
        self.require_entry(template, DirectoryRole::Template, false)?;
        let current = scan_directory(&template.path)?;
        if !current.same_layout(expected) {
            return Err(FsError::new(
                FsErrorKind::LayoutMismatch,
                "published template layout changed after validation",
            ));
        }
        let destination = self.run_path(label)?;
        require_absent(&destination)?;

        let output = Command::new("/bin/cp")
            .arg("-cR")
            .arg(&template.path)
            .arg(&destination)
            .output()
            .map_err(|error| io_error("execute /bin/cp -cR", &destination, error))?;
        if !output.status.success() {
            remove_copy_target(&destination);
            return Err(FsError::new(
                FsErrorKind::CopyFailed,
                format!(
                    "/bin/cp -cR failed for {}: status {}; stderr: {}",
                    destination.display(),
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            ));
        }

        let copied = match scan_directory(&destination) {
            Ok(manifest) => manifest,
            Err(error) => {
                remove_copy_target(&destination);
                return Err(error);
            }
        };
        if !copied.same_layout(expected) {
            remove_copy_target(&destination);
            return Err(FsError::new(
                FsErrorKind::LayoutMismatch,
                "restored run directory differs from the published template",
            ));
        }
        if copied.shares_regular_inode_with(&current) {
            remove_copy_target(&destination);
            return Err(FsError::new(
                FsErrorKind::UnsafeLayout,
                "restored run directory contains a hard link to the template",
            ));
        }

        let mut entries = self.inner.entries.lock().map_err(|_| {
            remove_copy_target(&destination);
            FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
        })?;
        let root_metadata = managed_root_metadata(&destination)?;
        let directory = DatabaseDirectory {
            workspace: Arc::clone(&self.inner),
            path: destination.clone(),
            backend_kind: template.backend_kind,
            root_device: root_metadata.dev(),
            root_inode: root_metadata.ino(),
        };
        entries.insert(
            destination,
            RegisteredEntry {
                role: DirectoryRole::Run,
                backend_kind: template.backend_kind,
                open: false,
                root_device: root_metadata.dev(),
                root_inode: root_metadata.ino(),
            },
        );
        Ok(directory)
    }

    pub(crate) fn cleanup_build(&self, directory: &DatabaseDirectory) -> Result<(), FsError> {
        self.remove_registered(directory, DirectoryRole::TemplateBuild)
    }

    pub(crate) fn same_workspace(&self, directory: &DatabaseDirectory) -> bool {
        Arc::ptr_eq(&self.inner, &directory.workspace)
    }

    pub(crate) fn next_internal_label(&self, prefix: &str) -> Result<String, FsError> {
        validate_label(prefix)?;
        Ok(format!("{prefix}-{}", self.next_internal_id()?))
    }

    fn run_path(&self, label: &str) -> Result<PathBuf, FsError> {
        validate_label(label)?;
        Ok(self.inner.root.join(format!("run-{label}")))
    }

    fn next_internal_id(&self) -> Result<u64, FsError> {
        let mut next = self.inner.next_build_id.lock().map_err(|_| {
            FsError::new(FsErrorKind::Io, "benchmark internal sequence is poisoned")
        })?;
        let id = *next;
        *next = next.checked_add(1).ok_or_else(|| {
            FsError::new(FsErrorKind::Io, "benchmark internal sequence overflowed")
        })?;
        Ok(id)
    }

    fn create_and_register(
        &self,
        path: PathBuf,
        role: DirectoryRole,
        backend_kind: BackendKind,
    ) -> Result<DatabaseDirectory, FsError> {
        require_absent(&path)?;
        fs::create_dir(&path).map_err(|error| io_error("create", &path, error))?;
        let mut entries = self.inner.entries.lock().map_err(|_| {
            let _ = fs::remove_dir(&path);
            FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
        })?;
        let root_metadata = managed_root_metadata(&path)?;
        let directory = DatabaseDirectory {
            workspace: Arc::clone(&self.inner),
            path: path.clone(),
            backend_kind,
            root_device: root_metadata.dev(),
            root_inode: root_metadata.ino(),
        };
        entries.insert(
            path,
            RegisteredEntry {
                role,
                backend_kind,
                open: false,
                root_device: root_metadata.dev(),
                root_inode: root_metadata.ino(),
            },
        );
        Ok(directory)
    }

    fn remove_registered(
        &self,
        directory: &DatabaseDirectory,
        required_role: DirectoryRole,
    ) -> Result<(), FsError> {
        self.require_entry(directory, required_role, false)?;
        scan_directory(&directory.path)?;
        fs::remove_dir_all(&directory.path)
            .map_err(|error| io_error("remove", &directory.path, error))?;
        let mut entries = self.inner.entries.lock().map_err(|_| {
            FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
        })?;
        entries.remove(&directory.path);
        Ok(())
    }

    fn require_registered(
        &self,
        directory: &DatabaseDirectory,
    ) -> Result<RegisteredEntry, FsError> {
        if !self.same_workspace(directory) {
            return Err(FsError::new(
                FsErrorKind::Unregistered,
                "directory belongs to another benchmark workspace",
            ));
        }
        let entry = {
            let entries = self.inner.entries.lock().map_err(|_| {
                FsError::new(FsErrorKind::Io, "benchmark directory registry is poisoned")
            })?;
            entries.get(&directory.path).copied().ok_or_else(|| {
                FsError::new(
                    FsErrorKind::Unregistered,
                    format!("directory is not registered: {}", directory.path.display()),
                )
            })?
        };
        let metadata = managed_root_metadata(&directory.path)?;
        if entry.root_device != directory.root_device || entry.root_inode != directory.root_inode {
            return Err(FsError::new(
                FsErrorKind::Unregistered,
                "directory token no longer identifies the registered database root",
            ));
        }
        if metadata.dev() != entry.root_device || metadata.ino() != entry.root_inode {
            return Err(FsError::new(
                FsErrorKind::UnsafeLayout,
                "database directory root identity changed after registration",
            ));
        }
        Ok(entry)
    }

    fn require_entry(
        &self,
        directory: &DatabaseDirectory,
        role: DirectoryRole,
        allow_open: bool,
    ) -> Result<(), FsError> {
        let entry = self.require_registered(directory)?;
        if entry.role != role {
            return Err(FsError::new(
                FsErrorKind::WrongRole,
                format!("directory has role {:?}, expected {role:?}", entry.role),
            ));
        }
        if entry.backend_kind != directory.backend_kind {
            return Err(FsError::new(
                FsErrorKind::Unregistered,
                "directory Backend identity does not match its registration",
            ));
        }
        if entry.open && !allow_open {
            return Err(FsError::new(
                FsErrorKind::Busy,
                format!("database directory is open: {}", directory.path.display()),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DirectoryManifest {
    entries: Vec<ManifestEntry>,
}

impl DirectoryManifest {
    fn contains_regular_file(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.kind == ManifestKind::File)
    }

    fn same_layout(&self, other: &Self) -> bool {
        self.entries.len() == other.entries.len()
            && self
                .entries
                .iter()
                .zip(&other.entries)
                .all(|(left, right)| {
                    left.relative == right.relative
                        && left.kind == right.kind
                        && left.length == right.length
                })
    }

    fn shares_regular_inode_with(&self, other: &Self) -> bool {
        self.entries
            .iter()
            .zip(&other.entries)
            .any(|(left, right)| {
                left.relative == right.relative
                    && left.kind == ManifestKind::File
                    && right.kind == ManifestKind::File
                    && left.device == right.device
                    && left.inode == right.inode
            })
    }
}

#[derive(Clone, Debug)]
struct ManifestEntry {
    relative: PathBuf,
    kind: ManifestKind,
    length: u64,
    device: u64,
    inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManifestKind {
    Directory,
    File,
}

fn scan_directory(root: &Path) -> Result<DirectoryManifest, FsError> {
    managed_root_metadata(root)?;
    let mut pending = vec![root.to_path_buf()];
    let mut entries = Vec::new();
    while let Some(directory) = pending.pop() {
        let children = fs::read_dir(&directory)
            .map_err(|error| io_error("read directory", &directory, error))?;
        for child in children {
            let child =
                child.map_err(|error| io_error("read directory entry", &directory, error))?;
            let path = child.path();
            let metadata =
                fs::symlink_metadata(&path).map_err(|error| io_error("inspect", &path, error))?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() || (!file_type.is_dir() && !file_type.is_file()) {
                return Err(FsError::new(
                    FsErrorKind::UnsafeLayout,
                    format!(
                        "managed directory contains an unsafe entry: {}",
                        path.display()
                    ),
                ));
            }
            let relative = path
                .strip_prefix(root)
                .expect("walked path is below root")
                .to_path_buf();
            let kind = if file_type.is_dir() {
                pending.push(path);
                ManifestKind::Directory
            } else {
                ManifestKind::File
            };
            entries.push(ManifestEntry {
                relative,
                kind,
                length: if kind == ManifestKind::File {
                    metadata.len()
                } else {
                    0
                },
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }
    }
    entries.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(DirectoryManifest { entries })
}

fn managed_root_metadata(root: &Path) -> Result<fs::Metadata, FsError> {
    let metadata = fs::symlink_metadata(root).map_err(|error| io_error("inspect", root, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(FsError::new(
            FsErrorKind::UnsafeLayout,
            format!(
                "managed database root is not a real directory: {}",
                root.display()
            ),
        ));
    }
    Ok(metadata)
}

fn require_entry_kind(path: &Path, required: ManifestKind) -> Result<(), FsError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        FsError::new(
            FsErrorKind::LayoutMismatch,
            format!(
                "required existing database entry {} is missing: {error}",
                path.display()
            ),
        )
    })?;
    let actual = if metadata.file_type().is_symlink() {
        None
    } else if metadata.is_file() {
        Some(ManifestKind::File)
    } else if metadata.is_dir() {
        Some(ManifestKind::Directory)
    } else {
        None
    };
    if actual != Some(required) {
        return Err(FsError::new(
            FsErrorKind::LayoutMismatch,
            format!(
                "required existing database entry {} has the wrong type",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), FsError> {
    if label.is_empty()
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(FsError::new(
            FsErrorKind::InvalidPath,
            "run label must contain only ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

fn require_absent(path: &Path) -> Result<(), FsError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(FsError::new(
            FsErrorKind::AlreadyExists,
            format!("destination already exists: {}", path.display()),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("inspect", path, error)),
    }
}

fn remove_copy_target(path: &Path) {
    if let Ok(metadata) = fs::symlink_metadata(path)
        && metadata.is_dir()
        && !metadata.file_type().is_symlink()
    {
        let _ = fs::remove_dir_all(path);
    }
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> FsError {
    FsError::new(
        FsErrorKind::Io,
        format!("{operation} {} failed: {error}", path.display()),
    )
}
