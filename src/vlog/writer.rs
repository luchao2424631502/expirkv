//! Value Log append, page transition, file rolling, and synchronization.
#![allow(dead_code)] // Stage 8 boundary; the coordinator is wired in later stages.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io;
#[cfg(test)]
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, OnceLock};

use crate::lock::sync_file_data;
use crate::vlog::file_set::{FileCatalog, VLogDirectory, read_exact_at};
use crate::vlog::format::{
    FILE_HEADER_ENCODED_LEN, LayoutPlanner, MAX_VLOG_FILE_SIZE, PAGE_HEADER_ENCODED_LEN,
    PageHeader, PhysicalChunk, PreparedEnvelope, VLOG_PAGE_SIZE, VLogFileHeader, VLogGeometry,
    VLogPosition,
};
use crate::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

const EINTR_RETRY_LIMIT: usize = 8;
const EAGAIN_RETRY_LIMIT: usize = 3;
#[cfg(target_os = "linux")]
const ELOOP: i32 = 40;
#[cfg(target_os = "macos")]
const ELOOP: i32 = 62;

#[derive(Debug)]
enum AppendState {
    Empty,
    Open {
        file_id: u32,
        offset: u64,
        file: File,
    },
    AtFileLimit {
        last_file_id: u32,
    },
}

/// Unforgeable capability required for every writable VLog directory action.
///
/// Only `ValueLogWriter` can construct or retain this token. The directory and
/// read-cache layers can name the type in order to check it, but cannot create
/// a second write capability.
#[derive(Debug)]
pub(super) struct WriterFileCapability {
    directory_identity: (u64, u64),
}

impl WriterFileCapability {
    fn claim(directory: &VLogDirectory) -> Result<Self> {
        let directory_identity = directory.writer_identity();
        let mut claims = writer_claims()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !claims.insert(directory_identity) {
            return Err(writer_claim_busy());
        }
        Ok(Self { directory_identity })
    }
}

impl Drop for WriterFileCapability {
    fn drop(&mut self) {
        writer_claims()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.directory_identity);
    }
}

fn writer_claims() -> &'static Mutex<HashSet<(u64, u64)>> {
    static CLAIMS: OnceLock<Mutex<HashSet<(u64, u64)>>> = OnceLock::new();
    CLAIMS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppendStateSnapshot {
    Empty,
    Open { file_id: u32, offset: u64 },
    AtFileLimit { last_file_id: u32 },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VLogDirtyState {
    pub(crate) dirty_files: BTreeSet<u32>,
    pub(crate) pending_directory_entries: BTreeMap<u32, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrontierSync {
    id: u64,
    pub(crate) target_seq: u64,
    pub(crate) target_end: Option<VLogPosition>,
    files: BTreeSet<u32>,
    directory_entries: BTreeSet<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AppendedTail {
    commit_seq: u64,
    end: VLogPosition,
}

pub(crate) trait WriterIo: Send + Sync {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize>;
    fn sync_file(&self, file: &File) -> io::Result<()>;
    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()>;

    #[cfg(test)]
    fn before_remove_new_file(&self, _file_id: u32) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct SystemWriterIo;

impl WriterIo for SystemWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        sync_file_data(file)
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()
    }
}

pub(crate) struct ValueLogWriter {
    directory: Arc<VLogDirectory>,
    file_capability: WriterFileCapability,
    database_uuid: [u8; 16],
    geometry: VLogGeometry,
    catalog: Arc<FileCatalog>,
    state: AppendState,
    dirty: VLogDirtyState,
    pending_sync_id: Option<u64>,
    next_sync_id: u64,
    append_failed: bool,
    last_appended: Option<AppendedTail>,
    io: Arc<dyn WriterIo>,
}

impl ValueLogWriter {
    pub(crate) fn empty(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
    ) -> Result<Self> {
        Self::empty_inner(
            directory,
            database_uuid,
            geometry,
            catalog,
            Arc::new(SystemWriterIo),
        )
    }

    #[cfg(test)]
    pub(crate) fn empty_with_io(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        io: Arc<dyn WriterIo>,
    ) -> Result<Self> {
        Self::empty_inner(directory, database_uuid, geometry, catalog, io)
    }

    fn empty_inner(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        io: Arc<dyn WriterIo>,
    ) -> Result<Self> {
        validate_writer_identity(database_uuid, geometry)
            .map_err(|error| open_context(error, None))?;
        let file_capability =
            WriterFileCapability::claim(&directory).map_err(|error| open_context(error, None))?;
        if !catalog
            .file_ids()
            .map_err(|error| open_context(error, None))?
            .is_empty()
        {
            return Err(open_context(append_layout(0, 0), None));
        }
        Ok(Self {
            directory,
            file_capability,
            database_uuid,
            geometry,
            catalog,
            state: AppendState::Empty,
            dirty: VLogDirtyState::default(),
            pending_sync_id: None,
            next_sync_id: 1,
            append_failed: false,
            last_appended: None,
            io,
        })
    }

    pub(crate) fn open(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        accepted_end: Option<VLogPosition>,
    ) -> Result<Self> {
        Self::open_inner(
            directory,
            database_uuid,
            geometry,
            catalog,
            accepted_end,
            Arc::new(SystemWriterIo),
        )
    }

    fn open_inner(
        directory: Arc<VLogDirectory>,
        database_uuid: [u8; 16],
        geometry: VLogGeometry,
        catalog: Arc<FileCatalog>,
        accepted_end: Option<VLogPosition>,
        io: Arc<dyn WriterIo>,
    ) -> Result<Self> {
        validate_writer_identity(database_uuid, geometry)
            .map_err(|error| open_context(error, accepted_end))?;
        let file_capability = WriterFileCapability::claim(&directory)
            .map_err(|error| open_context(error, accepted_end))?;
        let state = match accepted_end {
            None => {
                if !catalog
                    .file_ids()
                    .map_err(|error| open_context(error, None))?
                    .is_empty()
                {
                    return Err(open_context(append_layout(0, 0), None));
                }
                AppendState::Empty
            }
            Some(position) => {
                if LayoutPlanner::from_position(geometry, position).is_err() {
                    return Err(open_context(
                        append_layout(position.file_id, position.offset),
                        Some(position),
                    ));
                }
                if position.offset == 0 {
                    return Err(open_context(
                        append_layout(position.file_id, position.offset),
                        Some(position),
                    ));
                }
                let file = directory
                    .open_writable(&file_capability, position.file_id)
                    .map_err(|error| open_io(position.file_id, position.offset, error))?;
                catalog
                    .verify(position.file_id, &file)
                    .map_err(|error| open_context(error, Some(position)))?;
                let len = file
                    .metadata()
                    .map_err(|error| open_io(position.file_id, position.offset, error))?
                    .len();
                if len != position.offset {
                    return Err(open_context(
                        append_layout(position.file_id, position.offset),
                        Some(position),
                    ));
                }
                validate_accepted_catalog(&directory, &catalog, database_uuid, geometry, position)?;
                if position.offset == geometry.max_file_size {
                    drop(file);
                    AppendState::AtFileLimit {
                        last_file_id: position.file_id,
                    }
                } else {
                    AppendState::Open {
                        file_id: position.file_id,
                        offset: position.offset,
                        file,
                    }
                }
            }
        };
        Ok(Self {
            directory,
            file_capability,
            database_uuid,
            geometry,
            catalog,
            state,
            dirty: VLogDirtyState::default(),
            pending_sync_id: None,
            next_sync_id: 1,
            append_failed: false,
            last_appended: None,
            io,
        })
    }

    pub(crate) fn position(&self) -> VLogPosition {
        match &self.state {
            AppendState::Empty => VLogPosition {
                file_id: 0,
                offset: 0,
            },
            AppendState::Open {
                file_id, offset, ..
            } => VLogPosition {
                file_id: *file_id,
                offset: *offset,
            },
            AppendState::AtFileLimit { last_file_id } => VLogPosition {
                file_id: *last_file_id,
                offset: self.geometry.max_file_size,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn state_snapshot(&self) -> AppendStateSnapshot {
        match &self.state {
            AppendState::Empty => AppendStateSnapshot::Empty,
            AppendState::Open {
                file_id, offset, ..
            } => AppendStateSnapshot::Open {
                file_id: *file_id,
                offset: *offset,
            },
            AppendState::AtFileLimit { last_file_id } => AppendStateSnapshot::AtFileLimit {
                last_file_id: *last_file_id,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn active_file_descriptor(&self) -> Option<RawFd> {
        match &self.state {
            AppendState::Open { file, .. } => Some(file.as_raw_fd()),
            AppendState::Empty | AppendState::AtFileLimit { .. } => None,
        }
    }

    pub(crate) fn dirty_state(&self) -> &VLogDirtyState {
        &self.dirty
    }

    pub(crate) fn append(&mut self, envelope: &PreparedEnvelope) -> Result<()> {
        if self.append_failed {
            return Err(writer_stopped(envelope.commit_seq, envelope.vlog_begin));
        }
        if self.pending_sync_id.is_some() {
            return Err(writer_busy(envelope.commit_seq, envelope.vlog_begin));
        }
        if self
            .last_appended
            .is_some_and(|previous| envelope.commit_seq <= previous.commit_seq)
        {
            return Err(append_layout(
                envelope.vlog_begin.file_id,
                envelope.vlog_begin.offset,
            ));
        }
        self.validate_envelope_chunks(envelope)?;

        for chunk in &envelope.chunks {
            if let Err(error) = self.write_chunk(envelope.commit_seq, envelope.vlog_begin, chunk) {
                let retryable_without_progress = error.kind == StorageErrorKind::ResourceExhausted
                    && self.position() == envelope.vlog_begin;
                if !retryable_without_progress {
                    self.append_failed = true;
                }
                return Err(error);
            }
        }
        if self.position() != envelope.vlog_end {
            self.append_failed = true;
            return Err(append_layout(
                envelope.vlog_end.file_id,
                envelope.vlog_end.offset,
            ));
        }
        self.last_appended = Some(AppendedTail {
            commit_seq: envelope.commit_seq,
            end: envelope.vlog_end,
        });
        Ok(())
    }

    pub(crate) fn sync_through(
        &mut self,
        target_seq: u64,
        target_end: Option<VLogPosition>,
    ) -> Result<FrontierSync> {
        if self.append_failed {
            return Err(sync_stopped(target_seq, target_end));
        }
        if self.pending_sync_id.is_some() {
            return Err(sync_state_error(target_seq, target_end));
        }
        match (target_seq, target_end) {
            (0, None) => {
                if !matches!(&self.state, AppendState::Empty)
                    || !self.dirty.dirty_files.is_empty()
                    || !self.dirty.pending_directory_entries.is_empty()
                {
                    return Err(sync_state_error(target_seq, target_end));
                }
            }
            (0, Some(_)) | (_, None) => {
                return Err(sync_state_error(target_seq, target_end));
            }
            (_, Some(end)) => {
                LayoutPlanner::from_position(self.geometry, end)
                    .map_err(|error| sync_context(error, target_seq, target_end, end.file_id))?;
                if matches!(&self.state, AppendState::Empty) || end != self.position() {
                    return Err(sync_state_error(target_seq, target_end));
                }
            }
        }
        if let Some(appended) = self.last_appended
            && (target_seq != appended.commit_seq || target_end != Some(appended.end))
        {
            return Err(sync_state_error(target_seq, target_end));
        }
        if self
            .dirty
            .pending_directory_entries
            .values()
            .any(|created_by| *created_by > target_seq)
        {
            return Err(sync_state_error(target_seq, target_end));
        }

        let files: BTreeSet<u32> = self
            .dirty
            .dirty_files
            .iter()
            .copied()
            .filter(|file_id| target_end.is_some_and(|end| *file_id <= end.file_id))
            .collect();
        let directory_entries: BTreeSet<u32> = self
            .dirty
            .pending_directory_entries
            .iter()
            .filter_map(|(file_id, created_by)| {
                (*created_by <= target_seq && target_end.is_some_and(|end| *file_id <= end.file_id))
                    .then_some(*file_id)
            })
            .collect();

        for file_id in &files {
            if let Err(error) = self.sync_one_file(*file_id, target_seq) {
                self.append_failed = true;
                return Err(error);
            }
        }
        if !directory_entries.is_empty() {
            if let Err(error) = self
                .io
                .sync_directory(&self.directory)
                .map_err(|error| sync_directory_io(target_seq, target_end, error))
            {
                self.append_failed = true;
                return Err(error);
            }
        }

        let id = self.next_sync_id;
        let Some(next_sync_id) = self.next_sync_id.checked_add(1) else {
            self.append_failed = true;
            return Err(sync_state_error(target_seq, target_end));
        };
        self.next_sync_id = next_sync_id;
        self.pending_sync_id = Some(id);
        Ok(FrontierSync {
            id,
            target_seq,
            target_end,
            files,
            directory_entries,
        })
    }

    pub(crate) fn frontier_succeeded(&mut self, synced: FrontierSync) -> Result<()> {
        self.consume_sync(&synced)?;
        for file_id in synced.files {
            self.dirty.dirty_files.remove(&file_id);
        }
        for file_id in synced.directory_entries {
            self.dirty.pending_directory_entries.remove(&file_id);
        }
        self.pending_sync_id = None;
        Ok(())
    }

    pub(crate) fn frontier_failed(&mut self, synced: FrontierSync) -> Result<()> {
        self.consume_sync(&synced)?;
        self.pending_sync_id = None;
        self.append_failed = true;
        Ok(())
    }

    fn consume_sync(&self, synced: &FrontierSync) -> Result<()> {
        if self.pending_sync_id != Some(synced.id) {
            Err(sync_state_error(synced.target_seq, synced.target_end))
        } else {
            Ok(())
        }
    }

    fn validate_envelope_chunks(&self, envelope: &PreparedEnvelope) -> Result<()> {
        if envelope.commit_seq == 0
            || envelope.tx_uuid == [0; 16]
            || envelope.vlog_begin != self.position()
            || envelope.vlog_begin >= envelope.vlog_end
            || envelope.chunks.is_empty()
        {
            return Err(append_layout(
                envelope.vlog_begin.file_id,
                envelope.vlog_begin.offset,
            ));
        }

        let mut expected = next_chunk_start(envelope.vlog_begin, self.geometry)?;
        for (index, chunk) in envelope.chunks.iter().enumerate() {
            if chunk.position != expected || chunk.bytes.is_empty() {
                return Err(append_layout(chunk.position.file_id, chunk.position.offset));
            }
            self.validate_structural_chunk(chunk)?;
            expected = chunk_end(chunk, self.geometry)?;
            if expected.offset == self.geometry.max_file_size && index + 1 < envelope.chunks.len() {
                expected = next_chunk_start(expected, self.geometry)?;
            }
        }
        if expected != envelope.vlog_end {
            return Err(append_layout(expected.file_id, expected.offset));
        }
        Ok(())
    }

    fn validate_structural_chunk(&self, chunk: &PhysicalChunk) -> Result<()> {
        let page_offset = chunk.position.offset % self.geometry.page_size;
        if page_offset == 0 {
            if chunk.bytes.len() != PAGE_HEADER_ENCODED_LEN {
                return Err(append_layout(chunk.position.file_id, chunk.position.offset));
            }
            let header = PageHeader::decode(&chunk.bytes)
                .map_err(|_| append_layout(chunk.position.file_id, chunk.position.offset))?;
            let page_no = u32::try_from(chunk.position.offset / self.geometry.page_size)
                .map_err(|_| append_layout(chunk.position.file_id, chunk.position.offset))?;
            if header.file_id != chunk.position.file_id || header.page_no != page_no {
                return Err(append_layout(chunk.position.file_id, chunk.position.offset));
            }
        } else if chunk.position.offset == PAGE_HEADER_ENCODED_LEN as u64 {
            if chunk.bytes.len() != FILE_HEADER_ENCODED_LEN {
                return Err(append_layout(chunk.position.file_id, chunk.position.offset));
            }
            let header = VLogFileHeader::decode(&chunk.bytes)
                .map_err(|_| append_layout(chunk.position.file_id, chunk.position.offset))?;
            if header.file_id != chunk.position.file_id
                || header.database_uuid != self.database_uuid
                || u64::from(header.page_size) != VLOG_PAGE_SIZE
                || header.max_file_size != MAX_VLOG_FILE_SIZE
            {
                return Err(append_layout(chunk.position.file_id, chunk.position.offset));
            }
        }
        Ok(())
    }

    fn write_chunk(
        &mut self,
        commit_seq: u64,
        envelope_begin: VLogPosition,
        chunk: &PhysicalChunk,
    ) -> Result<()> {
        let previous_state = self.ensure_file_for_chunk(commit_seq, chunk.position)?;
        let created_file = previous_state.is_some();
        let geometry = self.geometry;
        let position_before_creation = previous_state.as_ref().map(|state| match state {
            AppendState::Empty => VLogPosition {
                file_id: 0,
                offset: 0,
            },
            AppendState::AtFileLimit { last_file_id } => VLogPosition {
                file_id: *last_file_id,
                offset: geometry.max_file_size,
            },
            AppendState::Open { .. } => unreachable!("only terminal states create VLog files"),
        });
        let io = Arc::clone(&self.io);
        let write_result = match &mut self.state {
            AppendState::Open {
                file_id,
                offset,
                file,
            } if *file_id == chunk.position.file_id && *offset == chunk.position.offset => {
                let result = write_all_positioned(&*io, file, &chunk.bytes, *offset);
                let written = match &result {
                    Ok(written) | Err((written, _)) => *written,
                };
                if written > 0 {
                    *offset = offset
                        .checked_add(written)
                        .ok_or_else(|| append_layout(*file_id, *offset))?;
                    self.dirty.dirty_files.insert(*file_id);
                }
                let transient_zero_progress = matches!(
                    &result,
                    Err((0, error))
                        if matches!(
                            error.kind(),
                            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                        )
                );
                let physical_progress = if created_file && written == 0 {
                    position_before_creation != Some(envelope_begin)
                } else {
                    *file_id != envelope_begin.file_id || *offset != envelope_begin.offset
                };
                result.map_err(|(_, error)| {
                    (
                        append_io(commit_seq, *file_id, *offset, physical_progress, error),
                        transient_zero_progress,
                    )
                })
            }
            _ => Err((
                append_layout(chunk.position.file_id, chunk.position.offset),
                false,
            )),
        };
        if let Err((error, transient_zero_progress)) = write_result {
            if transient_zero_progress && let Some(previous_state) = previous_state {
                let file_id = match &self.state {
                    AppendState::Open {
                        file_id, offset: 0, ..
                    } => *file_id,
                    _ => return Err(error),
                };
                if let Err(rollback_error) = self.rollback_new_empty_file(file_id, previous_state) {
                    return Err(append_io(commit_seq, file_id, 0, true, rollback_error));
                }
            }
            return Err(error);
        }

        let mut reached_limit = None;
        if let AppendState::Open {
            file_id,
            offset,
            file,
        } = &self.state
        {
            if *offset >= (PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN) as u64 {
                if let Err(error) = self.catalog.register(*file_id, file) {
                    return Err(append_context(error, commit_seq, *file_id, *offset));
                }
            }
            if *offset == geometry.max_file_size {
                reached_limit = Some(*file_id);
            }
        }
        if let Some(last_file_id) = reached_limit {
            self.state = AppendState::AtFileLimit { last_file_id };
        }
        Ok(())
    }

    fn ensure_file_for_chunk(
        &mut self,
        commit_seq: u64,
        position: VLogPosition,
    ) -> Result<Option<AppendState>> {
        let (file_id, previous_state) = match &self.state {
            AppendState::Empty => (0, AppendState::Empty),
            AppendState::Open {
                file_id, offset, ..
            } if *file_id == position.file_id && *offset == position.offset => return Ok(None),
            AppendState::AtFileLimit { last_file_id } => (
                last_file_id
                    .checked_add(1)
                    .ok_or_else(|| append_layout(position.file_id, position.offset))?,
                AppendState::AtFileLimit {
                    last_file_id: *last_file_id,
                },
            ),
            _ => return Err(append_layout(position.file_id, position.offset)),
        };
        if file_id != position.file_id
            || position.offset != 0
            || file_id > self.geometry.max_file_id
        {
            return Err(append_layout(position.file_id, position.offset));
        }
        let file = self
            .directory
            .create_new(&self.file_capability, file_id)
            .map_err(|error| append_io(commit_seq, file_id, 0, false, error))?;
        self.dirty
            .pending_directory_entries
            .insert(file_id, commit_seq);
        self.state = AppendState::Open {
            file_id,
            offset: 0,
            file,
        };
        Ok(Some(previous_state))
    }

    fn rollback_new_empty_file(
        &mut self,
        file_id: u32,
        previous_state: AppendState,
    ) -> io::Result<()> {
        let is_same_empty_file = match &self.state {
            AppendState::Open {
                file_id: current_file_id,
                offset,
                file,
            } if *current_file_id == file_id && *offset == 0 => file.metadata()?.len() == 0,
            _ => false,
        };
        if !is_same_empty_file {
            return Err(io::Error::other(
                "cannot roll back a VLog file after physical progress",
            ));
        }

        #[cfg(test)]
        self.io.before_remove_new_file(file_id)?;
        self.directory.remove_file(&self.file_capability, file_id)?;
        let removed_state = std::mem::replace(&mut self.state, previous_state);
        drop(removed_state);
        self.dirty.pending_directory_entries.remove(&file_id);
        self.dirty.dirty_files.remove(&file_id);
        Ok(())
    }

    fn sync_one_file(&self, file_id: u32, target_seq: u64) -> Result<()> {
        if let AppendState::Open {
            file_id: active_id,
            file,
            ..
        } = &self.state
        {
            if *active_id == file_id {
                return self.io.sync_file(file).map_err(|error| {
                    sync_file_io(target_seq, file_id, Some(self.position().offset), error)
                });
            }
        }

        let file = self
            .directory
            .open_writable(&self.file_capability, file_id)
            .map_err(|error| sync_file_io(target_seq, file_id, None, error))?;
        self.catalog
            .verify(file_id, &file)
            .map_err(|error| sync_context(error, target_seq, Some(self.position()), file_id))?;
        self.io
            .sync_file(&file)
            .map_err(|error| sync_file_io(target_seq, file_id, None, error))
    }
}

fn write_all_positioned(
    io: &dyn WriterIo,
    file: &File,
    mut bytes: &[u8],
    start: u64,
) -> std::result::Result<u64, (u64, io::Error)> {
    let mut written = 0_u64;
    let mut interrupted_attempts = 0_usize;
    let mut transient_attempts = 0_usize;
    while !bytes.is_empty() {
        let offset = match start.checked_add(written) {
            Some(offset) => offset,
            None => return Err((written, io::Error::other("VLog write offset overflow"))),
        };
        match io.write_at(file, bytes, offset) {
            Ok(0) => {
                return Err((
                    written,
                    io::Error::new(io::ErrorKind::WriteZero, "zero-progress VLog write"),
                ));
            }
            Ok(count) if count <= bytes.len() => {
                written = match written.checked_add(count as u64) {
                    Some(total) => total,
                    None => return Err((written, io::Error::other("VLog write length overflow"))),
                };
                bytes = &bytes[count..];
                interrupted_attempts = 0;
                transient_attempts = 0;
            }
            Ok(_) => return Err((written, io::Error::other("invalid VLog short-write result"))),
            Err(error)
                if error.kind() == io::ErrorKind::Interrupted
                    && interrupted_attempts < EINTR_RETRY_LIMIT =>
            {
                interrupted_attempts += 1;
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && transient_attempts < EAGAIN_RETRY_LIMIT =>
            {
                transient_attempts += 1;
            }
            Err(error) => return Err((written, error)),
        }
    }
    Ok(written)
}

fn validate_writer_identity(database_uuid: [u8; 16], geometry: VLogGeometry) -> Result<()> {
    if database_uuid == [0; 16] {
        return Err(append_layout(0, 0));
    }
    LayoutPlanner::empty(geometry)?;
    Ok(())
}

fn validate_accepted_catalog(
    directory: &VLogDirectory,
    catalog: &FileCatalog,
    database_uuid: [u8; 16],
    geometry: VLogGeometry,
    accepted_end: VLogPosition,
) -> Result<()> {
    let expected_count = usize::try_from(accepted_end.file_id.checked_add(1).ok_or_else(|| {
        open_context(
            append_layout(accepted_end.file_id, accepted_end.offset),
            Some(accepted_end),
        )
    })?)
    .map_err(|_| {
        open_context(
            append_layout(accepted_end.file_id, accepted_end.offset),
            Some(accepted_end),
        )
    })?;
    let file_ids = catalog
        .file_ids()
        .map_err(|error| open_context(error, Some(accepted_end)))?;
    if file_ids.len() != expected_count
        || file_ids
            .iter()
            .copied()
            .enumerate()
            .any(|(expected, actual)| usize::try_from(actual).ok() != Some(expected))
    {
        return Err(open_context(
            append_layout(accepted_end.file_id, accepted_end.offset),
            Some(accepted_end),
        ));
    }

    for file_id in file_ids {
        let file = directory
            .open_read_only(file_id)
            .map_err(|error| open_io(file_id, 0, error))?;
        catalog
            .verify(file_id, &file)
            .map_err(|error| open_context(error, None))?;
        let expected_len = if file_id < accepted_end.file_id {
            geometry.max_file_size
        } else {
            accepted_end.offset
        };
        let actual_len = file
            .metadata()
            .map_err(|error| open_io(file_id, expected_len, error))?
            .len();
        if actual_len != expected_len {
            return Err(open_context(
                append_layout(file_id, actual_len),
                Some(VLogPosition {
                    file_id,
                    offset: actual_len,
                }),
            ));
        }

        let mut encoded_headers = [0_u8; PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN];
        read_exact_at(&file, &mut encoded_headers, 0, file_id)
            .map_err(|error| open_context(error, None))?;
        let page_header = PageHeader::decode(&encoded_headers[..PAGE_HEADER_ENCODED_LEN])
            .map_err(|error| open_context(error, Some(VLogPosition { file_id, offset: 0 })))?;
        if page_header.file_id != file_id || page_header.page_no != 0 {
            return Err(accepted_header_corruption(file_id, 0));
        }
        let file_header = VLogFileHeader::decode(&encoded_headers[PAGE_HEADER_ENCODED_LEN..])
            .map_err(|error| {
                open_context(
                    error,
                    Some(VLogPosition {
                        file_id,
                        offset: PAGE_HEADER_ENCODED_LEN as u64,
                    }),
                )
            })?;
        if file_header.database_uuid != database_uuid
            || file_header.file_id != file_id
            || u64::from(file_header.page_size) != VLOG_PAGE_SIZE
            || file_header.max_file_size != MAX_VLOG_FILE_SIZE
        {
            return Err(accepted_header_corruption(
                file_id,
                PAGE_HEADER_ENCODED_LEN as u64,
            ));
        }
    }
    Ok(())
}

fn next_chunk_start(position: VLogPosition, geometry: VLogGeometry) -> Result<VLogPosition> {
    if position.offset < geometry.max_file_size {
        return Ok(position);
    }
    if position.offset != geometry.max_file_size || position.file_id >= geometry.max_file_id {
        return Err(append_layout(position.file_id, position.offset));
    }
    Ok(VLogPosition {
        file_id: position
            .file_id
            .checked_add(1)
            .ok_or_else(|| append_layout(position.file_id, position.offset))?,
        offset: 0,
    })
}

fn chunk_end(chunk: &PhysicalChunk, geometry: VLogGeometry) -> Result<VLogPosition> {
    let len = u64::try_from(chunk.bytes.len())
        .map_err(|_| append_layout(chunk.position.file_id, chunk.position.offset))?;
    let offset = chunk
        .position
        .offset
        .checked_add(len)
        .ok_or_else(|| append_layout(chunk.position.file_id, chunk.position.offset))?;
    if chunk.position.file_id > geometry.max_file_id || offset > geometry.max_file_size {
        return Err(append_layout(chunk.position.file_id, chunk.position.offset));
    }
    Ok(VLogPosition {
        file_id: chunk.position.file_id,
        offset,
    })
}

fn append_layout(file_id: u32, offset: u64) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::InvalidLayout,
        Operation::WriteBatch,
        ProtocolStage::VLogAppend,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::RestoreOrRepair,
    );
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn accepted_header_corruption(file_id: u32, offset: u64) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Corruption,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::RestoreOrRepair,
    );
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn writer_claim_busy() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Busy,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::RetrySameInstance,
    )
}

fn append_io(
    commit_seq: u64,
    file_id: u32,
    offset: u64,
    physical_progress: bool,
    source: io::Error,
) -> StorageError {
    let transient_without_progress = !physical_progress
        && matches!(
            source.kind(),
            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
        );
    let kind = if source.kind() == io::ErrorKind::StorageFull {
        StorageErrorKind::CapacityExceeded
    } else if transient_without_progress {
        StorageErrorKind::ResourceExhausted
    } else {
        StorageErrorKind::Io
    };
    let mut error = StorageError::codec_error(
        kind,
        Operation::WriteBatch,
        ProtocolStage::VLogAppend,
        Some(WriteOutcome::NotCommitted),
        if transient_without_progress {
            RetryAdvice::RetrySameInstance
        } else {
            RetryAdvice::FixEnvironmentAndReopen
        },
    );
    error.os_code = source.raw_os_error();
    error.commit_seq = Some(commit_seq);
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn open_io(file_id: u32, offset: u64, source: io::Error) -> StorageError {
    let kind = if source.kind() == io::ErrorKind::NotFound || source.raw_os_error() == Some(ELOOP) {
        StorageErrorKind::Corruption
    } else {
        StorageErrorKind::Io
    };
    let mut error = StorageError::codec_error(
        kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        if kind == StorageErrorKind::Corruption {
            RetryAdvice::RestoreOrRepair
        } else {
            RetryAdvice::FixEnvironmentAndReopen
        },
    );
    error.os_code = source.raw_os_error();
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn open_context(mut error: StorageError, position: Option<VLogPosition>) -> StorageError {
    error.operation = Operation::Open;
    error.protocol_stage = ProtocolStage::Recovery;
    error.write_outcome = None;
    error.instance_state = None;
    if let Some(position) = position {
        error.vlog_file_id = Some(position.file_id);
        error.vlog_offset = Some(position.offset);
    }
    error
}

fn append_context(
    mut error: StorageError,
    commit_seq: u64,
    file_id: u32,
    offset: u64,
) -> StorageError {
    error.operation = Operation::WriteBatch;
    error.protocol_stage = ProtocolStage::VLogAppend;
    error.write_outcome = Some(WriteOutcome::NotCommitted);
    error.instance_state = None;
    error.commit_seq = Some(commit_seq);
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn sync_context(
    mut error: StorageError,
    target_seq: u64,
    target_end: Option<VLogPosition>,
    file_id: u32,
) -> StorageError {
    error.operation = Operation::Sync;
    error.protocol_stage = ProtocolStage::VLogSync;
    error.write_outcome = Some(WriteOutcome::NotCommitted);
    error.instance_state = None;
    error.commit_seq = Some(target_seq);
    error.vlog_file_id = Some(file_id);
    if let Some(position) = target_end.filter(|position| position.file_id == file_id) {
        error.vlog_offset = Some(position.offset);
    } else {
        error.vlog_offset = None;
    }
    error
}

fn writer_stopped(commit_seq: u64, position: VLogPosition) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::StorageWriteStopped,
        Operation::WriteBatch,
        ProtocolStage::Admission,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.commit_seq = Some(commit_seq);
    error.vlog_file_id = Some(position.file_id);
    error.vlog_offset = Some(position.offset);
    error
}

fn writer_busy(commit_seq: u64, position: VLogPosition) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Busy,
        Operation::WriteBatch,
        ProtocolStage::Admission,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::RetrySameInstance,
    );
    error.commit_seq = Some(commit_seq);
    error.vlog_file_id = Some(position.file_id);
    error.vlog_offset = Some(position.offset);
    error
}

fn sync_stopped(target_seq: u64, target_end: Option<VLogPosition>) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::StorageWriteStopped,
        Operation::Sync,
        ProtocolStage::Admission,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.commit_seq = Some(target_seq);
    if let Some(position) = target_end {
        error.vlog_file_id = Some(position.file_id);
        error.vlog_offset = Some(position.offset);
    }
    error
}

fn sync_state_error(target_seq: u64, target_end: Option<VLogPosition>) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::InvalidArgument,
        Operation::Sync,
        ProtocolStage::VLogSync,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::DoNotRetry,
    );
    error.commit_seq = Some(target_seq);
    if let Some(position) = target_end {
        error.vlog_file_id = Some(position.file_id);
        error.vlog_offset = Some(position.offset);
    }
    error
}

fn sync_file_io(
    target_seq: u64,
    file_id: u32,
    offset: Option<u64>,
    source: io::Error,
) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Sync,
        ProtocolStage::VLogSync,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.os_code = source.raw_os_error();
    error.commit_seq = Some(target_seq);
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = offset;
    error
}

fn sync_directory_io(
    target_seq: u64,
    target_end: Option<VLogPosition>,
    source: io::Error,
) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Sync,
        ProtocolStage::VLogSync,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.os_code = source.raw_os_error();
    error.commit_seq = Some(target_seq);
    if let Some(position) = target_end {
        error.vlog_file_id = Some(position.file_id);
        error.vlog_offset = Some(position.offset);
    }
    error
}
