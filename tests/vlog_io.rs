#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crc32c::crc32c;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, HandleOpener, VLogDirectory};
use vlog::format::{
    DecodedRecord, LogicalOperationRef, PageHeader, PhysicalChunk, PreparedEnvelope,
    VALUE_POINTER_FORMAT_VERSION, VLogFileHeader, VLogGeometry, VLogPosition, ValuePointer,
    decode_record_at, prepare_envelope, scan_prepared_envelope,
};
use vlog::reader::ValueLogReader;
use vlog::writer::{AppendStateSnapshot, ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn database_uuid() -> [u8; 16] {
    [7, 3, 5, 9, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53]
}

fn tx_uuid(sequence: u8) -> [u8; 16] {
    [sequence; 16]
}

struct StorageHarness {
    _temporary: TempDir,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
}

impl StorageHarness {
    fn new() -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        Ok(Self {
            _temporary: temporary,
            directory,
            catalog: Arc::new(FileCatalog::new()),
        })
    }

    fn file_path(&self, file_id: u32) -> std::path::PathBuf {
        self.directory.path().join(format!("D{file_id:06}.data"))
    }
}

fn first_put_envelope(
    geometry: VLogGeometry,
    key: &[u8],
    value: &[u8],
) -> Result<PreparedEnvelope> {
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    prepare_envelope(
        &mut planner,
        database_uuid(),
        1,
        tx_uuid(1),
        &[LogicalOperationRef::Put { key, value }],
    )
}

fn expected_file_bytes(envelope: &PreparedEnvelope) -> BTreeMap<u32, Vec<u8>> {
    let mut files = BTreeMap::<u32, Vec<u8>>::new();
    for chunk in &envelope.chunks {
        let file = files.entry(chunk.position.file_id).or_default();
        assert_eq!(chunk.position.offset, file.len() as u64);
        file.extend_from_slice(&chunk.bytes);
    }
    files
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ChunkInventory {
    page_headers: usize,
    file_headers: usize,
    page_ends: usize,
    tx_begins: usize,
    kv_records: usize,
    delete_records: usize,
    tx_prepared_ends: usize,
}

fn inventory_chunks(
    chunks: &[PhysicalChunk],
    geometry: VLogGeometry,
) -> TestResult<ChunkInventory> {
    let mut inventory = ChunkInventory::default();
    for chunk in chunks {
        if chunk.position.offset.is_multiple_of(geometry.page_size) {
            let header = PageHeader::decode(&chunk.bytes)?;
            assert_eq!(header.file_id, chunk.position.file_id);
            assert_eq!(
                u64::from(header.page_no),
                chunk.position.offset / geometry.page_size
            );
            inventory.page_headers += 1;
            continue;
        }
        if chunk.position.offset == 16 {
            let header = VLogFileHeader::decode(&chunk.bytes)?;
            assert_eq!(header.database_uuid, database_uuid());
            assert_eq!(header.file_id, chunk.position.file_id);
            inventory.file_headers += 1;
            continue;
        }

        match decode_record_at(&chunk.bytes, chunk.position, geometry)? {
            DecodedRecord::PageEnd => inventory.page_ends += 1,
            DecodedRecord::TxBegin(_) => inventory.tx_begins += 1,
            DecodedRecord::KvRecord(_) => inventory.kv_records += 1,
            DecodedRecord::DeleteRecord(_) => inventory.delete_records += 1,
            DecodedRecord::TxPreparedEnd(_) => inventory.tx_prepared_ends += 1,
        }
    }
    Ok(inventory)
}

fn write_and_read_variable_puts(
    geometry: VLogGeometry,
    commit_seq: u64,
    values: &[Vec<u8>],
) -> TestResult<(PreparedEnvelope, ChunkInventory)> {
    assert!(values.len() <= 26);
    let harness = StorageHarness::new()?;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let keys: Vec<[u8; 1]> = (0..values.len())
        .map(|index| [b'a' + u8::try_from(index).expect("at most 26 keys")])
        .collect();
    let operations: Vec<LogicalOperationRef<'_>> = keys
        .iter()
        .zip(values)
        .map(|(key, value)| LogicalOperationRef::Put {
            key,
            value: value.as_slice(),
        })
        .collect();
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    let envelope = prepare_envelope(
        &mut planner,
        database_uuid(),
        commit_seq,
        tx_uuid(u8::try_from(commit_seq).expect("test sequence fits in u8")),
        &operations,
    )?;

    writer.append(&envelope)?;
    assert_eq!(writer.position(), envelope.vlog_end);

    let expected_files = expected_file_bytes(&envelope);
    let mut actual_files = BTreeMap::new();
    for (file_id, expected) in &expected_files {
        let actual = std::fs::read(harness.file_path(*file_id))?;
        assert_eq!(
            &actual, expected,
            "file {file_id} differs from its chunk stream"
        );
        actual_files.insert(*file_id, actual);
    }
    let disk_chunks: Vec<PhysicalChunk> = envelope
        .chunks
        .iter()
        .map(|expected| {
            let file = actual_files
                .get(&expected.position.file_id)
                .expect("every chunk file was read");
            let start = usize::try_from(expected.position.offset).expect("test offset fits usize");
            let end = start
                .checked_add(expected.bytes.len())
                .expect("test chunk end fits usize");
            PhysicalChunk {
                position: expected.position,
                bytes: file
                    .get(start..end)
                    .expect("chunk range exists in the real file")
                    .to_vec(),
            }
        })
        .collect();
    assert_eq!(disk_chunks, envelope.chunks);
    let inventory = inventory_chunks(&disk_chunks, geometry)?;
    let scanned = scan_prepared_envelope(
        &disk_chunks,
        geometry,
        database_uuid(),
        envelope.vlog_begin,
        envelope.vlog_end,
        Some(envelope.envelope_crc32c),
    )?;
    assert_eq!(scanned.logical_op_count, values.len() as u64);
    assert_eq!(scanned.kv_record_count, values.len() as u64);
    assert_eq!(scanned.delete_record_count, 0);

    drop(writer);
    let files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        2,
    )?);
    let reader = ValueLogReader::new(files, geometry)?;
    for (((pointer, key), expected_value), expected_index) in envelope
        .value_pointers
        .iter()
        .zip(&keys)
        .zip(values)
        .zip(0_u64..)
    {
        let pointer = pointer.expect("every operation is a Put");
        assert_eq!(usize::from(pointer.value_len), expected_value.len());
        let value = reader.read_value(&pointer.encode()?, key)?;
        assert_eq!(
            value, *expected_value,
            "value mismatch at op {expected_index}"
        );
    }

    Ok((envelope, inventory))
}

#[test]
fn real_files_are_lazy_exact_and_reopen_for_independent_position_reads() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    assert_eq!(writer.state_snapshot(), AppendStateSnapshot::Empty);
    assert!(!harness.file_path(0).exists());

    let operations = [
        LogicalOperationRef::Put {
            key: b"alpha",
            value: b"first-value",
        },
        LogicalOperationRef::Delete { key: b"gone" },
        LogicalOperationRef::Put {
            key: b"binary",
            value: b"\0\xff\0",
        },
    ];
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    let envelope = prepare_envelope(&mut planner, database_uuid(), 1, tx_uuid(1), &operations)?;
    assert_eq!(
        envelope.vlog_begin,
        VLogPosition {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(envelope.vlog_end.file_id, 0);
    assert!(envelope.vlog_end.offset < geometry.page_size);
    assert_eq!(
        inventory_chunks(&envelope.chunks, geometry)?,
        ChunkInventory {
            page_headers: 1,
            file_headers: 1,
            page_ends: 0,
            tx_begins: 1,
            kv_records: 2,
            delete_records: 1,
            tx_prepared_ends: 1,
        }
    );
    writer.append(&envelope)?;

    assert_eq!(writer.position(), envelope.vlog_end);
    for (file_id, expected) in expected_file_bytes(&envelope) {
        assert_eq!(std::fs::read(harness.file_path(file_id))?, expected);
    }
    let files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        4,
    )?);
    let reader = ValueLogReader::new(Arc::clone(&files), geometry)?;
    let alpha_pointer = envelope.value_pointers[0].expect("put pointer");
    let binary_pointer = envelope.value_pointers[2].expect("put pointer");
    assert_eq!(
        reader.read_value(&alpha_pointer.encode()?, b"alpha")?,
        b"first-value"
    );
    assert_eq!(
        reader.read_value(&binary_pointer.encode()?, b"binary")?,
        b"\0\xff\0"
    );

    let read_handle = files.handle(0)?;
    let writer_fd = writer
        .active_file_descriptor()
        .expect("writer must keep the active append handle");
    assert_ne!(writer_fd, read_handle.as_raw_fd());

    let end = writer.position();
    drop(reader);
    drop(files);
    drop(writer);
    let reopened = ValueLogWriter::open(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Some(end),
    )?;
    assert_eq!(reopened.position(), end);
    let reopened_files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        1,
    )?);
    let reopened_reader = ValueLogReader::new(reopened_files, geometry)?;
    assert_eq!(
        reopened_reader.read_pointer(alpha_pointer, b"alpha")?,
        b"first-value"
    );
    Ok(())
}

#[test]
fn single_page_variable_kv_records_round_trip_empty_middle_and_near_limit() -> TestResult {
    let geometry = VLogGeometry::PRODUCTION;
    let values = vec![
        Vec::new(),
        vec![0x11; 1],
        vec![0x22; 257],
        vec![0x33; 4_096],
        vec![0x44; 59_999],
    ];
    let (envelope, inventory) = write_and_read_variable_puts(geometry, 2, &values)?;

    assert_eq!(envelope.vlog_end.file_id, 0);
    assert!(envelope.vlog_end.offset < geometry.page_size);
    assert_eq!(inventory.page_headers, 1);
    assert_eq!(inventory.file_headers, 1);
    assert_eq!(inventory.page_ends, 0);
    assert_eq!(inventory.kv_records, values.len());
    Ok(())
}

#[test]
fn variable_kv_records_round_trip_across_page_without_crossing_file() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 2)?;
    let values = vec![
        Vec::new(),
        vec![0x11; 1],
        vec![0x22; 257],
        vec![0x33; 4_096],
        vec![0x44; 59_999],
        vec![0x55; 1_024],
    ];
    let (envelope, inventory) = write_and_read_variable_puts(geometry, 3, &values)?;

    assert_eq!(envelope.vlog_end.file_id, 0);
    assert!(envelope.vlog_end.offset > geometry.page_size);
    assert!(envelope.vlog_end.offset < geometry.max_file_size);
    assert_eq!(inventory.page_headers, 2);
    assert_eq!(inventory.file_headers, 1);
    assert_eq!(inventory.page_ends, 1);
    assert_eq!(inventory.kv_records, values.len());
    Ok(())
}

#[test]
fn variable_kv_records_round_trip_across_a_single_page_file_boundary() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 65_536, 2)?;
    let values = vec![
        Vec::new(),
        vec![0x11; 1],
        vec![0x22; 257],
        vec![0x33; 4_096],
        vec![0x44; 59_999],
        vec![0x55; 1_024],
    ];
    let (envelope, inventory) = write_and_read_variable_puts(geometry, 4, &values)?;

    assert_eq!(envelope.vlog_end.file_id, 1);
    assert!(envelope.vlog_end.offset < geometry.page_size);
    assert_eq!(inventory.page_headers, 2);
    assert_eq!(inventory.file_headers, 2);
    assert_eq!(inventory.page_ends, 1);
    assert_eq!(inventory.kv_records, values.len());
    Ok(())
}

#[test]
fn variable_kv_records_round_trip_across_pages_and_files() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 2)?;
    let values = vec![
        Vec::new(),
        vec![0x11; 59_999],
        vec![0x22; 4_096],
        vec![0x33; 1_024],
        vec![0x44; 59_999],
        vec![0x55; 4_096],
        vec![0x66; 1_024],
        vec![0x77; 1_024],
    ];
    let (envelope, inventory) = write_and_read_variable_puts(geometry, 5, &values)?;

    assert_eq!(envelope.vlog_end.file_id, 1);
    assert!(envelope.vlog_end.offset < geometry.page_size);
    assert_eq!(inventory.page_headers, 3);
    assert_eq!(inventory.file_headers, 2);
    assert_eq!(inventory.page_ends, 2);
    assert_eq!(inventory.kv_records, values.len());
    Ok(())
}

#[test]
fn test_geometry_exercises_page_and_file_rolls_with_exact_stage_three_bytes() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 5)?;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let value = [0x5a; 80];
    let operations = [
        LogicalOperationRef::Put {
            key: b"k0",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k1",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k2",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k3",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k4",
            value: &value,
        },
        LogicalOperationRef::Put {
            key: b"k5",
            value: &value,
        },
    ];
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    let envelope = prepare_envelope(&mut planner, database_uuid(), 1, tx_uuid(1), &operations)?;
    assert!(envelope.vlog_end.file_id > 0, "test must cross a file");
    writer.append(&envelope)?;
    assert_eq!(writer.position(), envelope.vlog_end);

    let expected = expected_file_bytes(&envelope);
    assert!(expected.len() > 1);
    for (file_id, bytes) in expected {
        assert_eq!(std::fs::read(harness.file_path(file_id))?, bytes);
    }

    drop(writer);
    let files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        2,
    )?);
    let reader = ValueLogReader::new(files, geometry)?;
    for (index, pointer) in envelope.value_pointers.iter().enumerate() {
        let pointer = pointer.expect("every operation in this envelope is a Put");
        let key = format!("k{index}");
        assert_eq!(reader.read_pointer(pointer, key.as_bytes())?, value);
    }
    Ok(())
}

#[test]
fn accepted_file_limit_has_no_active_handle_and_next_append_uses_create_new() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 2)?;
    let sealed = harness.directory.create_new_for_test(0)?;
    sealed.write_all_at(
        &vlog::format::PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    sealed.write_all_at(
        &vlog::format::VLogFileHeader::new(database_uuid(), 0).encode()?,
        16,
    )?;
    sealed.set_len(geometry.max_file_size)?;
    harness.catalog.register(0, &sealed)?;
    drop(sealed);

    let accepted_end = VLogPosition {
        file_id: 0,
        offset: geometry.max_file_size,
    };
    let mut writer = ValueLogWriter::open(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Some(accepted_end),
    )?;
    assert!(matches!(
        writer.state_snapshot(),
        AppendStateSnapshot::AtFileLimit { last_file_id: 0 }
    ));
    let mut planner = vlog::format::LayoutPlanner::from_position(geometry, accepted_end)?;
    let envelope = prepare_envelope(
        &mut planner,
        database_uuid(),
        2,
        tx_uuid(2),
        &[LogicalOperationRef::Put {
            key: b"next",
            value: b"file",
        }],
    )?;
    assert_eq!(envelope.chunks[0].position.file_id, 1);
    writer.append(&envelope)?;
    assert!(harness.file_path(1).exists());
    assert_eq!(writer.position(), envelope.vlog_end);
    Ok(())
}

#[test]
fn writer_open_classifies_missing_accepted_files_as_corruption() -> TestResult {
    let active_harness = StorageHarness::new()?;
    let active = active_harness.directory.create_new_for_test(0)?;
    active.write_all_at(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    active.write_all_at(&VLogFileHeader::new(database_uuid(), 0).encode()?, 16)?;
    active_harness.catalog.register(0, &active)?;
    drop(active);
    std::fs::remove_file(active_harness.file_path(0))?;

    let active_error = match ValueLogWriter::open(
        Arc::clone(&active_harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&active_harness.catalog),
        Some(VLogPosition {
            file_id: 0,
            offset: 64,
        }),
    ) {
        Ok(_) => panic!("writer open accepted a missing active file"),
        Err(error) => error,
    };
    assert_eq!(active_error.kind, StorageErrorKind::Corruption);
    assert_eq!(active_error.operation, Operation::Open);
    assert_eq!(active_error.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(active_error.retry_advice, RetryAdvice::RestoreOrRepair);
    assert_eq!(active_error.vlog_file_id, Some(0));
    assert_eq!(active_error.vlog_offset, Some(64));

    let lower_harness = StorageHarness::new()?;
    let lower_geometry = VLogGeometry::test_only(256, 512, 2)?;
    let lower = lower_harness.directory.create_new_for_test(0)?;
    lower.write_all_at(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    lower.write_all_at(&VLogFileHeader::new(database_uuid(), 0).encode()?, 16)?;
    lower.set_len(lower_geometry.max_file_size)?;
    lower_harness.catalog.register(0, &lower)?;

    let current = lower_harness.directory.create_new_for_test(1)?;
    current.write_all_at(
        &PageHeader {
            file_id: 1,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    current.write_all_at(&VLogFileHeader::new(database_uuid(), 1).encode()?, 16)?;
    lower_harness.catalog.register(1, &current)?;
    drop(lower);
    drop(current);
    std::fs::remove_file(lower_harness.file_path(0))?;

    let lower_error = match ValueLogWriter::open(
        Arc::clone(&lower_harness.directory),
        database_uuid(),
        lower_geometry,
        Arc::clone(&lower_harness.catalog),
        Some(VLogPosition {
            file_id: 1,
            offset: 64,
        }),
    ) {
        Ok(_) => panic!("writer open accepted a missing lower sealed file"),
        Err(error) => error,
    };
    assert_eq!(lower_error.kind, StorageErrorKind::Corruption);
    assert_eq!(lower_error.operation, Operation::Open);
    assert_eq!(lower_error.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(lower_error.retry_advice, RetryAdvice::RestoreOrRepair);
    assert_eq!(lower_error.vlog_file_id, Some(0));
    assert_eq!(lower_error.vlog_offset, Some(0));
    Ok(())
}

#[test]
fn writer_open_does_not_adopt_an_accepted_file_missing_from_catalog() -> TestResult {
    for valid_header in [true, false] {
        let harness = StorageHarness::new()?;
        let accepted = harness.directory.create_new_for_test(0)?;
        if valid_header {
            accepted.write_all_at(
                &PageHeader {
                    file_id: 0,
                    page_no: 0,
                }
                .encode()?,
                0,
            )?;
            accepted.write_all_at(&VLogFileHeader::new(database_uuid(), 0).encode()?, 16)?;
        } else {
            accepted.set_len(64)?;
        }
        drop(accepted);
        let before = std::fs::read(harness.file_path(0))?;

        let error = match ValueLogWriter::open(
            Arc::clone(&harness.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&harness.catalog),
            Some(VLogPosition {
                file_id: 0,
                offset: 64,
            }),
        ) {
            Ok(_) => panic!("writer adopted an accepted file missing from its catalog"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::Corruption);
        assert_eq!(error.operation, Operation::Open);
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
        assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair);
        assert_eq!(error.vlog_file_id, Some(0));
        assert_eq!(error.vlog_offset, Some(64));
        assert!(harness.catalog.file_ids()?.is_empty());
        assert_eq!(std::fs::read(harness.file_path(0))?, before);
    }
    Ok(())
}

#[test]
fn writer_open_requires_every_accepted_file_in_the_catalog() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 2)?;
    let lower = harness.directory.create_new_for_test(0)?;
    lower.write_all_at(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    lower.write_all_at(&VLogFileHeader::new(database_uuid(), 0).encode()?, 16)?;
    lower.set_len(geometry.max_file_size)?;

    let active = harness.directory.create_new_for_test(1)?;
    active.write_all_at(
        &PageHeader {
            file_id: 1,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    active.write_all_at(&VLogFileHeader::new(database_uuid(), 1).encode()?, 16)?;
    harness.catalog.register(1, &active)?;
    drop(lower);
    drop(active);

    let error = match ValueLogWriter::open(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Some(VLogPosition {
            file_id: 1,
            offset: 64,
        }),
    ) {
        Ok(_) => panic!("writer accepted an incomplete catalog"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair);
    assert_eq!(error.vlog_file_id, Some(1));
    assert_eq!(error.vlog_offset, Some(64));
    assert_eq!(harness.catalog.file_ids()?, vec![1]);
    Ok(())
}

#[test]
fn writer_capability_is_unique_for_the_physical_vlog_directory() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut initial = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let duplicate_empty = match ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    ) {
        Ok(_) => panic!("a second empty writer acquired the same physical VLog directory"),
        Err(error) => error,
    };
    assert_eq!(duplicate_empty.kind, StorageErrorKind::Busy);
    assert_eq!(duplicate_empty.operation, Operation::Open);
    assert_eq!(duplicate_empty.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(duplicate_empty.retry_advice, RetryAdvice::RetrySameInstance);
    assert_eq!(duplicate_empty.vlog_file_id, None);
    assert_eq!(duplicate_empty.vlog_offset, None);

    let envelope = first_put_envelope(geometry, b"key", b"value")?;
    initial.append(&envelope)?;
    let accepted_end = envelope.vlog_end;
    drop(initial);

    let first = ValueLogWriter::open(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Some(accepted_end),
    )?;
    let before = std::fs::read(harness.file_path(0))?;

    for directory in [
        Arc::clone(&harness.directory),
        Arc::new(VLogDirectory::open(harness.directory.path())?),
    ] {
        let error = match ValueLogWriter::open(
            directory,
            database_uuid(),
            geometry,
            Arc::clone(&harness.catalog),
            Some(accepted_end),
        ) {
            Ok(_) => panic!("a second writer acquired the same physical VLog directory"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::Busy);
        assert_eq!(error.operation, Operation::Open);
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
        assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
        assert_eq!(error.vlog_file_id, Some(accepted_end.file_id));
        assert_eq!(error.vlog_offset, Some(accepted_end.offset));
        assert_eq!(std::fs::read(harness.file_path(0))?, before);
    }

    drop(first);
    let independent_directory = Arc::new(VLogDirectory::open(harness.directory.path())?);
    let reopened = ValueLogWriter::open(
        independent_directory,
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Some(accepted_end),
    )?;
    assert_eq!(reopened.position(), accepted_end);
    Ok(())
}

#[test]
fn writer_open_revalidates_registered_accepted_file_headers() -> TestResult {
    let mut cases = Vec::<(&str, [u8; 64], u64)>::new();

    cases.push(("bad page header", [0_u8; 64], 0));

    let mut bad_file_header = [0_u8; 64];
    bad_file_header[..16].copy_from_slice(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
    );
    cases.push(("bad file header", bad_file_header, 16));

    let mut wrong_page_identity = [0_u8; 64];
    wrong_page_identity[..16].copy_from_slice(
        &PageHeader {
            file_id: 1,
            page_no: 0,
        }
        .encode()?,
    );
    wrong_page_identity[16..].copy_from_slice(&VLogFileHeader::new(database_uuid(), 0).encode()?);
    cases.push(("wrong page identity", wrong_page_identity, 0));

    let mut wrong_database_identity = [0_u8; 64];
    wrong_database_identity[..16].copy_from_slice(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
    );
    wrong_database_identity[16..].copy_from_slice(&VLogFileHeader::new([0x55; 16], 0).encode()?);
    cases.push(("wrong database identity", wrong_database_identity, 16));

    for (name, bytes, expected_offset) in cases {
        let harness = StorageHarness::new()?;
        let accepted = harness.directory.create_new_for_test(0)?;
        accepted.write_all_at(&bytes, 0)?;
        harness.catalog.register(0, &accepted)?;
        drop(accepted);

        let error = match ValueLogWriter::open(
            Arc::clone(&harness.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&harness.catalog),
            Some(VLogPosition {
                file_id: 0,
                offset: 64,
            }),
        ) {
            Ok(_) => panic!("writer accepted {name}"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::Corruption, "{name}");
        assert_eq!(error.operation, Operation::Open, "{name}");
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery, "{name}");
        assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair, "{name}");
        assert_eq!(error.vlog_file_id, Some(0), "{name}");
        assert_eq!(error.vlog_offset, Some(expected_offset), "{name}");
        assert_eq!(std::fs::read(harness.file_path(0))?, bytes, "{name}");
    }
    Ok(())
}

#[test]
fn writer_open_classifies_every_invalid_accepted_position_as_layout_damage() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 2)?;
    let invalid_positions = [
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogPosition {
            file_id: 0,
            offset: 1,
        },
        VLogPosition {
            file_id: geometry.max_file_id + 1,
            offset: 64,
        },
        VLogPosition {
            file_id: 0,
            offset: geometry.max_file_size + 1,
        },
    ];

    for position in invalid_positions {
        let error = match ValueLogWriter::open(
            Arc::clone(&harness.directory),
            database_uuid(),
            geometry,
            Arc::clone(&harness.catalog),
            Some(position),
        ) {
            Ok(_) => panic!("writer accepted invalid position {position:?}"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::InvalidLayout, "{position:?}");
        assert_eq!(error.operation, Operation::Open, "{position:?}");
        assert_eq!(
            error.protocol_stage,
            ProtocolStage::Recovery,
            "{position:?}"
        );
        assert_eq!(
            error.retry_advice,
            RetryAdvice::RestoreOrRepair,
            "{position:?}"
        );
        assert_eq!(error.vlog_file_id, Some(position.file_id));
        assert_eq!(error.vlog_offset, Some(position.offset));
        assert!(harness.catalog.file_ids()?.is_empty());
        assert!(!harness.file_path(0).exists());
    }
    Ok(())
}

#[derive(Debug)]
struct LimitedWriterIo {
    maximum: usize,
    calls: AtomicUsize,
}

impl WriterIo for LimitedWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.maximum == 0 {
            return Ok(0);
        }
        file.write_at(&bytes[..bytes.len().min(self.maximum)], offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()
    }
}

#[test]
fn positive_short_writes_continue_and_zero_progress_fails_immediately() -> TestResult {
    let short_harness = StorageHarness::new()?;
    let short_io = Arc::new(LimitedWriterIo {
        maximum: 3,
        calls: AtomicUsize::new(0),
    });
    let mut short_writer = ValueLogWriter::empty_with_io(
        Arc::clone(&short_harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&short_harness.catalog),
        short_io.clone(),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    short_writer.append(&envelope)?;
    assert!(short_io.calls.load(Ordering::SeqCst) > envelope.chunks.len());
    assert_eq!(
        std::fs::read(short_harness.file_path(0))?,
        expected_file_bytes(&envelope).remove(&0).unwrap()
    );

    let zero_harness = StorageHarness::new()?;
    let zero_io = Arc::new(LimitedWriterIo {
        maximum: 0,
        calls: AtomicUsize::new(0),
    });
    let mut zero_writer = ValueLogWriter::empty_with_io(
        Arc::clone(&zero_harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&zero_harness.catalog),
        zero_io.clone(),
    )?;
    let error = zero_writer.append(&envelope).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogAppend);
    assert_eq!(zero_io.calls.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::metadata(zero_harness.file_path(0))?.len(), 0);
    Ok(())
}

#[derive(Debug, Default)]
struct RecordingWriterIo {
    events: Mutex<Vec<&'static str>>,
}

#[derive(Debug, Default)]
struct FailingSyncIo;

#[derive(Debug, Default)]
struct FailingDirectorySyncIo;

impl WriterIo for FailingSyncIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, _file: &File) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(5))
    }

    fn sync_directory(&self, _directory: &VLogDirectory) -> io::Result<()> {
        panic!("directory sync must not run after file sync failure")
    }
}

impl WriterIo for FailingDirectorySyncIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&self, _directory: &VLogDirectory) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(5))
    }
}

impl WriterIo for RecordingWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        self.events.lock().unwrap().push("file");
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        self.events.lock().unwrap().push("directory");
        directory.sync()
    }
}

#[test]
fn dirty_and_directory_entries_clear_only_after_frontier_success() -> TestResult {
    let harness = StorageHarness::new()?;
    let io = Arc::new(RecordingWriterIo::default());
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        io.clone(),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    writer.append(&envelope)?;
    assert_eq!(
        writer
            .dirty_state()
            .dirty_files
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(
        writer.dirty_state().pending_directory_entries.get(&0),
        Some(&1)
    );

    let first = writer.sync_through(1, Some(envelope.vlog_end))?;
    assert_eq!(*io.events.lock().unwrap(), vec!["file", "directory"]);
    assert!(!writer.dirty_state().dirty_files.is_empty());
    assert!(!writer.dirty_state().pending_directory_entries.is_empty());
    writer.frontier_succeeded(first)?;
    assert!(writer.dirty_state().dirty_files.is_empty());
    assert!(writer.dirty_state().pending_directory_entries.is_empty());
    Ok(())
}

#[test]
fn pending_frontier_rejects_append_as_busy_and_resumes_after_success() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let first = first_put_envelope(geometry, b"key", b"value")?;
    writer.append(&first)?;
    let pending = writer.sync_through(1, Some(first.vlog_end))?;
    let second = next_put_envelope(geometry, first.vlog_end, 2)?;

    let error = writer.append(&second).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Busy);
    assert_eq!(error.operation, Operation::WriteBatch);
    assert_eq!(error.protocol_stage, ProtocolStage::Admission);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
    assert_eq!(writer.position(), first.vlog_end);

    writer.frontier_succeeded(pending)?;
    writer.append(&second)?;
    assert_eq!(writer.position(), second.vlog_end);
    Ok(())
}

#[test]
fn multi_file_sync_covers_every_dirty_file_before_the_directory() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 5)?;
    let io = Arc::new(RecordingWriterIo::default());
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        io.clone(),
    )?;
    let value = [0x5a; 80];
    let keys = (0..6).map(|index| [b'k', b'0' + index]).collect::<Vec<_>>();
    let operations = keys
        .iter()
        .map(|key| LogicalOperationRef::Put { key, value: &value })
        .collect::<Vec<_>>();
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    let envelope = prepare_envelope(&mut planner, database_uuid(), 1, tx_uuid(1), &operations)?;
    assert!(envelope.vlog_end.file_id > 0);
    writer.append(&envelope)?;

    let dirty_files = writer.dirty_state().dirty_files.len();
    assert!(dirty_files > 1);
    assert_eq!(
        writer.dirty_state().pending_directory_entries.len(),
        dirty_files
    );
    let synced = writer.sync_through(1, Some(envelope.vlog_end))?;
    let mut expected_events = vec!["file"; dirty_files];
    expected_events.push("directory");
    assert_eq!(*io.events.lock().unwrap(), expected_events);
    assert_eq!(writer.dirty_state().dirty_files.len(), dirty_files);
    assert_eq!(
        writer.dirty_state().pending_directory_entries.len(),
        dirty_files
    );

    writer.frontier_succeeded(synced)?;
    assert!(writer.dirty_state().dirty_files.is_empty());
    assert!(writer.dirty_state().pending_directory_entries.is_empty());
    Ok(())
}

fn next_put_envelope(
    geometry: VLogGeometry,
    begin: VLogPosition,
    commit_seq: u64,
) -> Result<PreparedEnvelope> {
    let mut planner = vlog::format::LayoutPlanner::from_position(geometry, begin)?;
    prepare_envelope(
        &mut planner,
        database_uuid(),
        commit_seq,
        tx_uuid(u8::try_from(commit_seq).unwrap_or(0xff)),
        &[LogicalOperationRef::Put {
            key: b"next-key",
            value: b"next-value",
        }],
    )
}

fn assert_append_is_stopped(
    writer: &mut ValueLogWriter,
    geometry: VLogGeometry,
    begin: VLogPosition,
) -> TestResult {
    let next = next_put_envelope(geometry, begin, 2)?;
    let error = writer.append(&next).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::StorageWriteStopped);
    assert_eq!(error.protocol_stage, ProtocolStage::Admission);
    Ok(())
}

fn assert_sync_is_stopped(
    writer: &mut ValueLogWriter,
    target_seq: u64,
    target_end: VLogPosition,
) -> TestResult {
    let error = writer
        .sync_through(target_seq, Some(target_end))
        .unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::StorageWriteStopped);
    assert_eq!(error.operation, Operation::Sync);
    assert_eq!(error.protocol_stage, ProtocolStage::Admission);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    Ok(())
}

#[test]
fn frontier_failure_keeps_dirty_state_and_stops_the_writer() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let envelope = first_put_envelope(geometry, b"key", b"value")?;
    writer.append(&envelope)?;
    let synced = writer.sync_through(1, Some(envelope.vlog_end))?;
    writer.frontier_failed(synced)?;

    assert!(!writer.dirty_state().dirty_files.is_empty());
    assert!(!writer.dirty_state().pending_directory_entries.is_empty());
    assert_sync_is_stopped(&mut writer, 1, envelope.vlog_end)?;
    assert_append_is_stopped(&mut writer, geometry, envelope.vlog_end)
}

#[test]
fn sync_failure_keeps_all_dirty_and_pending_state() -> TestResult {
    let harness = StorageHarness::new()?;
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        Arc::new(FailingSyncIo),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    writer.append(&envelope)?;
    let error = writer.sync_through(1, Some(envelope.vlog_end)).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogSync);
    assert_eq!(
        writer
            .dirty_state()
            .dirty_files
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(
        writer.dirty_state().pending_directory_entries.get(&0),
        Some(&1)
    );
    assert_sync_is_stopped(&mut writer, 1, envelope.vlog_end)?;
    assert_append_is_stopped(&mut writer, VLogGeometry::PRODUCTION, envelope.vlog_end)?;
    Ok(())
}

#[test]
fn directory_sync_failure_keeps_all_dirty_state_and_stops_the_writer() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
        Arc::new(FailingDirectorySyncIo),
    )?;
    let envelope = first_put_envelope(geometry, b"key", b"value")?;
    writer.append(&envelope)?;
    let error = writer.sync_through(1, Some(envelope.vlog_end)).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogSync);
    assert!(!writer.dirty_state().dirty_files.is_empty());
    assert!(!writer.dirty_state().pending_directory_entries.is_empty());
    assert_sync_is_stopped(&mut writer, 1, envelope.vlog_end)?;
    assert_append_is_stopped(&mut writer, geometry, envelope.vlog_end)?;
    Ok(())
}

#[test]
fn sync_target_sequence_and_end_obey_the_durable_frontier_relation() -> TestResult {
    let harness = StorageHarness::new()?;
    let geometry = VLogGeometry::PRODUCTION;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&harness.catalog),
    )?;
    let empty_position = writer.position();

    assert_eq!(
        writer.sync_through(1, None).unwrap_err().kind,
        StorageErrorKind::InvalidArgument
    );
    assert_eq!(
        writer
            .sync_through(0, Some(empty_position))
            .unwrap_err()
            .kind,
        StorageErrorKind::InvalidArgument
    );
    assert_eq!(
        writer
            .sync_through(1, Some(empty_position))
            .unwrap_err()
            .kind,
        StorageErrorKind::InvalidArgument
    );

    let empty = writer.sync_through(0, None)?;
    assert_eq!(empty.target_seq, 0);
    assert_eq!(empty.target_end, None);
    writer.frontier_succeeded(empty)?;

    let envelope = first_put_envelope(geometry, b"key", b"value")?;
    writer.append(&envelope)?;
    assert_eq!(
        writer.sync_through(1, None).unwrap_err().kind,
        StorageErrorKind::InvalidArgument
    );
    assert_eq!(
        writer
            .sync_through(0, Some(envelope.vlog_end))
            .unwrap_err()
            .kind,
        StorageErrorKind::InvalidArgument
    );
    assert_eq!(
        writer
            .sync_through(2, Some(envelope.vlog_end))
            .unwrap_err()
            .kind,
        StorageErrorKind::InvalidArgument
    );
    let nonempty = writer.sync_through(1, Some(envelope.vlog_end))?;
    writer.frontier_succeeded(nonempty)?;

    let second = next_put_envelope(geometry, envelope.vlog_end, 2)?;
    writer.append(&second)?;
    for wrong_seq in [1, 3] {
        assert_eq!(
            writer
                .sync_through(wrong_seq, Some(second.vlog_end))
                .unwrap_err()
                .kind,
            StorageErrorKind::InvalidArgument
        );
    }
    let second_sync = writer.sync_through(2, Some(second.vlog_end))?;
    writer.frontier_succeeded(second_sync)?;
    Ok(())
}

#[derive(Debug)]
struct TransientThenSuccessIo {
    remaining_failures: AtomicUsize,
    error_kind: io::ErrorKind,
    calls: AtomicUsize,
}

impl WriterIo for TransientThenSuccessIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(io::Error::from(self.error_kind));
        }
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()
    }
}

#[test]
fn exhausted_zero_progress_eintr_and_eagain_are_retryable_resource_errors() -> TestResult {
    for (error_kind, failures, expected_calls) in [
        (io::ErrorKind::Interrupted, 9, 9),
        (io::ErrorKind::WouldBlock, 4, 4),
    ] {
        let harness = StorageHarness::new()?;
        let io = Arc::new(TransientThenSuccessIo {
            remaining_failures: AtomicUsize::new(failures),
            error_kind,
            calls: AtomicUsize::new(0),
        });
        let mut writer = ValueLogWriter::empty_with_io(
            Arc::clone(&harness.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&harness.catalog),
            io.clone(),
        )?;
        let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
        let error = writer.append(&envelope).unwrap_err();
        assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
        assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
        assert_eq!(io.calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(writer.position(), envelope.vlog_begin);

        writer.append(&envelope)?;
        assert_eq!(writer.position(), envelope.vlog_end);
    }
    Ok(())
}

#[test]
fn first_file_zero_progress_transient_failure_restores_empty_writer() -> TestResult {
    let harness = StorageHarness::new()?;
    let io = Arc::new(TransientThenSuccessIo {
        remaining_failures: AtomicUsize::new(4),
        error_kind: io::ErrorKind::WouldBlock,
        calls: AtomicUsize::new(0),
    });
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        io,
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;

    let error = writer.append(&envelope).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
    assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
    assert_eq!(writer.state_snapshot(), AppendStateSnapshot::Empty);
    assert_eq!(writer.position(), envelope.vlog_begin);
    assert!(writer.dirty_state().dirty_files.is_empty());
    assert!(writer.dirty_state().pending_directory_entries.is_empty());
    assert!(harness.catalog.file_ids()?.is_empty());
    assert!(!harness.file_path(0).exists());

    let empty_barrier = writer.sync_through(0, None)?;
    writer.frontier_succeeded(empty_barrier)?;
    writer.append(&envelope)?;
    assert_eq!(writer.position(), envelope.vlog_end);
    assert!(harness.file_path(0).is_file());
    Ok(())
}

#[derive(Debug, Default)]
struct RollbackFailureIo {
    calls: AtomicUsize,
}

impl WriterIo for RollbackFailureIo {
    fn write_at(&self, _file: &File, _bytes: &[u8], _offset: u64) -> io::Result<usize> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()
    }

    fn before_remove_new_file(&self, _file_id: u32) -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }
}

#[test]
fn failed_empty_file_rollback_stops_writer_and_preserves_evidence() -> TestResult {
    let harness = StorageHarness::new()?;
    let io = Arc::new(RollbackFailureIo::default());
    let mut writer = ValueLogWriter::empty_with_io(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        io.clone(),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;

    let error = writer.append(&envelope).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.retry_advice, RetryAdvice::FixEnvironmentAndReopen);
    assert_eq!(io.calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        writer.state_snapshot(),
        AppendStateSnapshot::Open {
            file_id: 0,
            offset: 0,
        }
    );
    assert!(writer.dirty_state().dirty_files.is_empty());
    assert_eq!(
        writer.dirty_state().pending_directory_entries.get(&0),
        Some(&1)
    );
    assert_eq!(std::fs::metadata(harness.file_path(0))?.len(), 0);

    assert_eq!(
        writer.append(&envelope).unwrap_err().kind,
        StorageErrorKind::StorageWriteStopped
    );
    assert_eq!(
        writer.sync_through(0, None).unwrap_err().kind,
        StorageErrorKind::StorageWriteStopped
    );
    Ok(())
}

#[test]
fn catalog_failures_are_remapped_to_the_active_writer_operation() -> TestResult {
    let append_harness = StorageHarness::new()?;
    let mut append_writer = ValueLogWriter::empty(
        Arc::clone(&append_harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&append_harness.catalog),
    )?;
    let foreign = tempfile::tempfile()?;
    append_harness.catalog.register(0, &foreign)?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    let error = append_writer.append(&envelope).unwrap_err();
    assert_eq!(error.operation, Operation::WriteBatch);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogAppend);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.commit_seq, Some(1));

    let open_harness = StorageHarness::new()?;
    let accepted = open_harness.directory.create_new_for_test(0)?;
    accepted.write_all_at(
        &vlog::format::PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    accepted.write_all_at(
        &vlog::format::VLogFileHeader::new(database_uuid(), 0).encode()?,
        16,
    )?;
    let foreign = tempfile::tempfile()?;
    open_harness.catalog.register(0, &foreign)?;
    let open_error = match ValueLogWriter::open(
        Arc::clone(&open_harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&open_harness.catalog),
        Some(VLogPosition {
            file_id: 0,
            offset: 64,
        }),
    ) {
        Ok(_) => panic!("writer open accepted a catalog identity mismatch"),
        Err(error) => error,
    };
    assert_eq!(open_error.operation, Operation::Open);
    assert_eq!(open_error.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(open_error.write_outcome, None);

    let lower_harness = StorageHarness::new()?;
    let lower_geometry = VLogGeometry::test_only(256, 512, 2)?;
    let lower_file = lower_harness.directory.create_new_for_test(0)?;
    lower_file.write_all_at(
        &vlog::format::PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    lower_file.write_all_at(
        &vlog::format::VLogFileHeader::new(database_uuid(), 0).encode()?,
        16,
    )?;
    lower_file.set_len(lower_geometry.max_file_size)?;
    let active_file = lower_harness.directory.create_new_for_test(1)?;
    active_file.write_all_at(
        &vlog::format::PageHeader {
            file_id: 1,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    active_file.write_all_at(
        &vlog::format::VLogFileHeader::new(database_uuid(), 1).encode()?,
        16,
    )?;
    let foreign = tempfile::tempfile()?;
    lower_harness.catalog.register(0, &foreign)?;
    lower_harness.catalog.register(1, &active_file)?;
    let lower_error = match ValueLogWriter::open(
        Arc::clone(&lower_harness.directory),
        database_uuid(),
        lower_geometry,
        Arc::clone(&lower_harness.catalog),
        Some(VLogPosition {
            file_id: 1,
            offset: 64,
        }),
    ) {
        Ok(_) => panic!("writer open accepted a lower sealed-file identity mismatch"),
        Err(error) => error,
    };
    assert_eq!(lower_error.operation, Operation::Open);
    assert_eq!(lower_error.protocol_stage, ProtocolStage::Recovery);
    assert_eq!(lower_error.vlog_file_id, Some(0));

    let sync_harness = StorageHarness::new()?;
    let geometry = VLogGeometry::test_only(256, 512, 5)?;
    let mut sync_writer = ValueLogWriter::empty(
        Arc::clone(&sync_harness.directory),
        database_uuid(),
        geometry,
        Arc::clone(&sync_harness.catalog),
    )?;
    let value = [0x5a; 80];
    let operations = (0..6)
        .map(|index| format!("sync-key-{index}"))
        .collect::<Vec<_>>();
    let logical = operations
        .iter()
        .map(|key| LogicalOperationRef::Put {
            key: key.as_bytes(),
            value: &value,
        })
        .collect::<Vec<_>>();
    let mut planner = vlog::format::LayoutPlanner::empty(geometry)?;
    let envelope = prepare_envelope(&mut planner, database_uuid(), 1, tx_uuid(1), &logical)?;
    assert!(envelope.vlog_end.file_id > 0);
    sync_writer.append(&envelope)?;
    sync_harness.catalog.unregister(0)?;
    let foreign = tempfile::tempfile()?;
    sync_harness.catalog.register(0, &foreign)?;

    let error = sync_writer
        .sync_through(1, Some(envelope.vlog_end))
        .unwrap_err();
    assert_eq!(error.operation, Operation::Sync);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogSync);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.commit_seq, Some(1));
    assert_append_is_stopped(&mut sync_writer, geometry, envelope.vlog_end)?;
    Ok(())
}

fn reader_case() -> TestResult<(StorageHarness, ValueLogReader, ValuePointer)> {
    let harness = StorageHarness::new()?;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    writer.append(&envelope)?;
    let pointer = envelope.value_pointers[0].unwrap();
    let files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        0,
    )?);
    let reader = ValueLogReader::new(files, VLogGeometry::PRODUCTION)?;
    drop(writer);
    Ok((harness, reader, pointer))
}

#[derive(Debug, Default)]
struct CountingReaderOpener {
    calls: AtomicUsize,
}

impl HandleOpener for CountingReaderOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        directory.open_read_only(file_id)
    }
}

#[test]
fn invalid_in_memory_pointer_is_rejected_before_any_file_io() -> TestResult {
    let harness = StorageHarness::new()?;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    writer.append(&envelope)?;
    drop(writer);

    let opener = Arc::new(CountingReaderOpener::default());
    let files = Arc::new(FileSet::with_opener(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        0,
        opener.clone(),
    )?);
    let reader = ValueLogReader::new(files, VLogGeometry::PRODUCTION)?;
    let mut invalid = envelope.value_pointers[0].expect("put pointer");
    invalid.record_len = 0;
    let error = reader.read_pointer(invalid, b"key").unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.vlog_file_id, Some(invalid.file_id));
    assert_eq!(error.vlog_offset, Some(u64::from(invalid.record_offset)));
    assert_eq!(opener.calls.load(Ordering::SeqCst), 0);

    assert_eq!(
        reader.read_pointer(envelope.value_pointers[0].unwrap(), b"key")?,
        b"value"
    );
    assert_eq!(opener.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn reader_rejects_a_geometry_that_differs_from_its_file_set() -> TestResult {
    let harness = StorageHarness::new()?;
    let files = Arc::new(FileSet::new(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
        0,
    )?);
    let different = VLogGeometry::test_only(256, 512, 2)?;
    let error = match ValueLogReader::new(files, different) {
        Ok(_) => panic!("reader accepted a geometry different from its FileSet"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(error.operation, Operation::Get);
    assert_eq!(error.protocol_stage, ProtocolStage::Read);
    assert_eq!(
        error.retry_advice,
        RetryAdvice::FixRequestAndRetrySameInstance
    );
    Ok(())
}

fn assert_corruption<T: std::fmt::Debug>(result: Result<T>) {
    assert_eq!(result.unwrap_err().kind, StorageErrorKind::Corruption);
}

fn assert_corruption_at<T: std::fmt::Debug>(result: Result<T>, file_id: u32, offset: u64) {
    let error = result.unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.vlog_file_id, Some(file_id));
    assert_eq!(error.vlog_offset, Some(offset));
}

#[test]
fn reader_rejects_missing_header_middle_truncated_and_key_mismatch_pointers() -> TestResult {
    let (_harness, reader, pointer) = reader_case()?;
    let mut missing = pointer;
    missing.file_id = 1;
    assert_corruption(reader.read_pointer(missing, b"key"));

    let mut header = pointer;
    header.record_offset = 0;
    assert_corruption(reader.read_pointer(header, b"key"));

    let mut middle = pointer;
    middle.record_offset += 1;
    assert_corruption_at(
        reader.read_pointer(middle, b"key"),
        middle.file_id,
        u64::from(middle.record_offset),
    );
    assert_corruption_at(
        reader.read_pointer(pointer, b"different-key"),
        pointer.file_id,
        u64::from(pointer.record_offset),
    );

    let (harness, reader, pointer) = reader_case()?;
    let file = harness.directory.open_writable_for_test(0)?;
    file.set_len(u64::from(pointer.record_offset + pointer.record_len - 1))?;
    assert_corruption(reader.read_pointer(pointer, b"key"));
    Ok(())
}

#[test]
fn reader_rejects_record_length_type_crc_and_user_key_damage() -> TestResult {
    let (_harness, reader, pointer) = reader_case()?;
    let mut wrong_len = pointer;
    wrong_len.record_len += 1;
    assert_corruption_at(
        reader.read_pointer(wrong_len, b"key"),
        wrong_len.file_id,
        u64::from(wrong_len.record_offset),
    );

    let (harness, reader, pointer) = reader_case()?;
    let file = harness.directory.open_writable_for_test(0)?;
    let mut header = [0_u8; 39];
    file.read_exact_at(&mut header, u64::from(pointer.record_offset))?;
    header[6] = 0x03;
    let checksum = crc32c(&header[..35]);
    header[35..39].copy_from_slice(&checksum.to_le_bytes());
    file.write_all_at(&header, u64::from(pointer.record_offset))?;
    assert_corruption_at(
        reader.read_pointer(pointer, b"key"),
        pointer.file_id,
        u64::from(pointer.record_offset),
    );

    let (harness, reader, pointer) = reader_case()?;
    let file = harness.directory.open_writable_for_test(0)?;
    let crc_offset = u64::from(pointer.record_offset + pointer.record_len - 1);
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, crc_offset)?;
    byte[0] ^= 0x80;
    file.write_all_at(&byte, crc_offset)?;
    assert_corruption_at(
        reader.read_pointer(pointer, b"key"),
        pointer.file_id,
        u64::from(pointer.record_offset),
    );

    let (harness, reader, pointer) = reader_case()?;
    let file = harness.directory.open_writable_for_test(0)?;
    let key_offset = u64::from(pointer.record_offset) + 51;
    file.write_all_at(b"X", key_offset)?;
    assert_corruption_at(
        reader.read_pointer(pointer, b"key"),
        pointer.file_id,
        u64::from(pointer.record_offset),
    );
    Ok(())
}

#[test]
fn create_new_never_overwrites_an_existing_file_number() -> TestResult {
    let harness = StorageHarness::new()?;
    std::fs::write(harness.file_path(0), b"preexisting")?;
    let before = std::fs::read(harness.file_path(0))?;
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&harness.directory),
        database_uuid(),
        VLogGeometry::PRODUCTION,
        Arc::clone(&harness.catalog),
    )?;
    let envelope = first_put_envelope(VLogGeometry::PRODUCTION, b"key", b"value")?;
    assert_eq!(
        writer.append(&envelope).unwrap_err().kind,
        StorageErrorKind::Io
    );
    assert_eq!(std::fs::read(harness.file_path(0))?, before);
    Ok(())
}
