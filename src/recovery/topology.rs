//! Managed-file inventory, identity, and physical-topology validation.
#![allow(dead_code)] // Stage 7 inventory; later recovery stages consume the result.

use std::fs;
use std::io::Read;

use crate::commit::{DurableVLogEnd, VLogPos};
use crate::db::{ManagedInventory, VLogInventoryEntry};
use crate::format::{
    FORMAT_ENCODED_LEN, FORMAT_FILE_NAME, FORMAT_TEMP_FILE_NAME, FormatMetadataV0,
};
use crate::lock::{ManagedEntryKind, RootLock, layout_error, open_io_error, open_regular_nofollow};
use crate::vlog::format::{
    FILE_HEADER_ENCODED_LEN, MAX_VLOG_FILE_ID, MAX_VLOG_FILE_SIZE, PAGE_HEADER_ENCODED_LEN,
    PageHeader, VLogFileHeader, VLogGeometry,
};
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

const LOCK_FILE_NAME: &str = "LOCK";
const INDEX_DIRECTORY_NAME: &str = "index";
const VLOG_DIRECTORY_NAME: &str = "vlog";
const VLOG_PREFIX_LEN: usize = 1;
const VLOG_DIGITS_LEN: usize = 6;
const VLOG_SUFFIX: &str = ".data";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PhysicalTail {
    Empty,
    Position(VLogPos),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryTopology {
    pub(crate) physical_tail: PhysicalTail,
    pub(crate) file_count: usize,
}

impl RecoveryTopology {
    pub(crate) fn analyze(
        inventory: &ManagedInventory,
        stable_end: DurableVLogEnd,
    ) -> Result<Self> {
        Self::analyze_with_geometry(inventory, stable_end, VLogGeometry::PRODUCTION)
    }

    #[cfg(test)]
    pub(crate) fn analyze_with_test_geometry(
        inventory: &ManagedInventory,
        stable_end: DurableVLogEnd,
        geometry: VLogGeometry,
    ) -> Result<Self> {
        Self::analyze_with_geometry(inventory, stable_end, geometry)
    }

    fn analyze_with_geometry(
        inventory: &ManagedInventory,
        stable_end: DurableVLogEnd,
        geometry: VLogGeometry,
    ) -> Result<Self> {
        validate_inventory_order(inventory, geometry)?;
        validate_stable_files(inventory, stable_end, geometry)?;
        let physical_tail = inventory
            .vlog_files
            .last()
            .map_or(PhysicalTail::Empty, |entry| {
                PhysicalTail::Position(VLogPos {
                    file_id: entry.file_id,
                    offset: entry.len,
                })
            });
        Ok(Self {
            physical_tail,
            file_count: inventory.vlog_files.len(),
        })
    }

    pub(crate) fn contains_end(&self, inventory: &ManagedInventory, end: DurableVLogEnd) -> bool {
        match end {
            DurableVLogEnd::Empty => true,
            DurableVLogEnd::Position(position) => inventory
                .vlog_files
                .binary_search_by_key(&position.file_id, |entry| entry.file_id)
                .ok()
                .and_then(|index| inventory.vlog_files.get(index))
                .is_some_and(|entry| entry.len >= position.offset),
        }
    }

    pub(crate) fn has_suffix_after(
        &self,
        inventory: &ManagedInventory,
        accepted_end: DurableVLogEnd,
    ) -> bool {
        match accepted_end {
            DurableVLogEnd::Empty => !inventory.vlog_files.is_empty(),
            DurableVLogEnd::Position(position) => inventory.vlog_files.iter().any(|entry| {
                entry.file_id > position.file_id
                    || (entry.file_id == position.file_id && entry.len > position.offset)
            }),
        }
    }
}

impl ManagedInventory {
    pub(crate) fn inspect(root: &RootLock, format: &FormatMetadataV0) -> Result<Self> {
        require_regular(root, LOCK_FILE_NAME, None)?;
        require_regular(root, FORMAT_FILE_NAME, Some(FORMAT_ENCODED_LEN as u64))?;
        if !matches!(
            root.inspect_child(FORMAT_TEMP_FILE_NAME)?,
            ManagedEntryKind::Missing
        ) {
            return Err(layout_error());
        }
        require_directory(root, INDEX_DIRECTORY_NAME)?;
        require_directory(root, VLOG_DIRECTORY_NAME)?;

        let vlog_path = root.canonical_path().join(VLOG_DIRECTORY_NAME);
        let mut vlog_files = Vec::new();
        for entry in root.read_directory(VLOG_DIRECTORY_NAME)? {
            let entry = entry.map_err(open_io_error)?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(layout_error)?;
            let file_id = parse_vlog_file_name(name).ok_or_else(layout_error)?;
            let file_type = entry.file_type().map_err(open_io_error)?;
            if !file_type.is_file() {
                return Err(layout_error());
            }
            let path = vlog_path.join(name);
            let metadata = fs::symlink_metadata(&path).map_err(open_io_error)?;
            if !metadata.file_type().is_file() || metadata.len() > MAX_VLOG_FILE_SIZE {
                return Err(layout_error());
            }
            if metadata.len() >= (PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN) as u64 {
                validate_file_identity(&path, file_id, format)?;
            }
            vlog_files
                .try_reserve(1)
                .map_err(|_| resource_exhausted_error())?;
            vlog_files.push(VLogInventoryEntry {
                file_id,
                len: metadata.len(),
                path,
            });
        }
        vlog_files.sort_unstable_by_key(|entry| entry.file_id);
        Ok(Self { vlog_files })
    }
}

fn validate_inventory_order(inventory: &ManagedInventory, geometry: VLogGeometry) -> Result<()> {
    let mut previous = None;
    for entry in &inventory.vlog_files {
        if entry.file_id > geometry.max_file_id
            || entry.len > geometry.max_file_size
            || previous.is_some_and(|file_id| file_id >= entry.file_id)
        {
            return Err(corruption_error());
        }
        previous = Some(entry.file_id);
    }
    Ok(())
}

fn validate_stable_files(
    inventory: &ManagedInventory,
    stable_end: DurableVLogEnd,
    geometry: VLogGeometry,
) -> Result<()> {
    let DurableVLogEnd::Position(stable_end) = stable_end else {
        return Ok(());
    };
    if stable_end.file_id > geometry.max_file_id || stable_end.offset > geometry.max_file_size {
        return Err(corruption_error());
    }

    let mut expected_file_id = 0_u32;
    for entry in inventory
        .vlog_files
        .iter()
        .take_while(|entry| entry.file_id <= stable_end.file_id)
    {
        if entry.file_id != expected_file_id {
            return Err(corruption_error());
        }
        if entry.file_id < stable_end.file_id {
            if entry.len != geometry.max_file_size {
                return Err(corruption_error());
            }
            expected_file_id = expected_file_id
                .checked_add(1)
                .ok_or_else(corruption_error)?;
        } else {
            if entry.len < stable_end.offset {
                return Err(corruption_error());
            }
            return Ok(());
        }
    }
    Err(corruption_error())
}

fn validate_file_identity(
    path: &std::path::Path,
    file_id: u32,
    format: &FormatMetadataV0,
) -> Result<()> {
    let mut file = open_regular_nofollow(path, false).map_err(open_io_error)?;
    let mut encoded = [0_u8; PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN];
    file.read_exact(&mut encoded).map_err(open_io_error)?;
    let page_header = PageHeader::decode(&encoded[..PAGE_HEADER_ENCODED_LEN])
        .map_err(open_header_decode_error)?;
    let file_header = VLogFileHeader::decode(&encoded[PAGE_HEADER_ENCODED_LEN..])
        .map_err(open_header_decode_error)?;
    if page_header.file_id != file_id
        || page_header.page_no != 0
        || file_header.file_id != file_id
        || file_header.database_uuid != format.database_uuid
        || file_header.format_version != format.format_version
        || file_header.page_size != format.page_size
        || file_header.max_file_size != MAX_VLOG_FILE_SIZE
    {
        return Err(corruption_error());
    }
    Ok(())
}

fn parse_vlog_file_name(name: &str) -> Option<u32> {
    let expected_len = VLOG_PREFIX_LEN + VLOG_DIGITS_LEN + VLOG_SUFFIX.len();
    if name.len() != expected_len || !name.starts_with('D') || !name.ends_with(VLOG_SUFFIX) {
        return None;
    }
    let digits = name.get(VLOG_PREFIX_LEN..VLOG_PREFIX_LEN + VLOG_DIGITS_LEN)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let file_id = digits.parse::<u32>().ok()?;
    (file_id <= MAX_VLOG_FILE_ID).then_some(file_id)
}

fn require_regular(root: &RootLock, name: &str, expected_len: Option<u64>) -> Result<()> {
    match root.inspect_child(name)? {
        ManagedEntryKind::RegularFile { len }
            if expected_len.is_none_or(|expected| expected == len) =>
        {
            Ok(())
        }
        _ => Err(layout_error()),
    }
}

fn require_directory(root: &RootLock, name: &str) -> Result<()> {
    if matches!(root.inspect_child(name)?, ManagedEntryKind::Directory) {
        Ok(())
    } else {
        Err(layout_error())
    }
}

fn corruption_error() -> StorageError {
    open_inventory_error(StorageErrorKind::Corruption)
}

fn open_header_decode_error(error: StorageError) -> StorageError {
    open_inventory_error(error.kind)
}

fn open_inventory_error(kind: StorageErrorKind) -> StorageError {
    let retry_advice = if kind == StorageErrorKind::IncompatibleFormat {
        RetryAdvice::DoNotRetry
    } else {
        RetryAdvice::RestoreOrRepair
    };
    StorageError::codec_error(
        kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        retry_advice,
    )
}

fn resource_exhausted_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::RetrySameInstance,
    )
}
