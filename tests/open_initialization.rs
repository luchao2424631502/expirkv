#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

pub struct Snapshot;
pub struct DbIterator;
pub struct RangeCursor;
pub struct WriteBatch;
pub struct KeyRange<'a>(std::marker::PhantomData<&'a ()>);

#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};

#[path = "../src/commit/descriptor.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/options.rs"]
mod options;
pub(crate) use options::{Options, ReadOptions, WriteOptions};

#[path = "../src/db.rs"]
mod db;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/recovery/mod.rs"]
mod recovery;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use std::ffi::{CString, c_char, c_int};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use db::{
    INITIALIZATION_CRASH_EXIT_CODE, InitializationFault, prepare_open, prepare_open_with_fault,
    validate_interrupted_index_for_test,
};
use format::{FORMAT_FILE_NAME, FORMAT_TEMP_FILE_NAME, FormatMetadataV0};
use index::{
    DATABASE_IDENTITY_KEY, DURABLE_FRONTIER_KEY, DatabaseIdentityV0, FjallBackend, HEAD_SEQ_KEY,
    IndexAtomicBatch, IndexBackend, IndexCommitMode, IndexMutation, InternalIndexSpace,
    initialization_batch, is_encoded_empty_durable_frontier, is_encoded_head_seq_zero,
};
use tempfile::TempDir;
use vlog::format::{MAX_VLOG_FILE_SIZE, PageHeader, VLOG_PAGE_SIZE, VLogFileHeader};

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

const INITIALIZATION_CHILD_ENV: &str = "RUSTKV_INITIALIZATION_CRASH_CHILD";
const INITIALIZATION_PATH_ENV: &str = "RUSTKV_INITIALIZATION_CRASH_PATH";
const INITIALIZATION_MODE_ENV: &str = "RUSTKV_INITIALIZATION_CRASH_MODE";

#[cfg(target_os = "linux")]
type ModeT = u32;
#[cfg(target_os = "macos")]
type ModeT = u16;

unsafe extern "C" {
    fn mkfifo(path: *const c_char, mode: ModeT) -> c_int;
}

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn expect_prepare_error(
    result: Result<db::OpenPreparation>,
    expected: StorageErrorKind,
) -> StorageError {
    let error = match result {
        Ok(_) => panic!("Open preparation unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind, expected);
    assert_eq!(error.operation, Operation::Open);
    assert!(error.write_outcome.is_none());
    assert!(error.instance_state.is_none());
    error
}

fn run_child_bounded(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "initialization crash child exceeded 10 seconds",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_initialization_triple(
    index: &FjallBackend,
    expected: bool,
    format: &FormatMetadataV0,
) -> TestResult {
    let identity = index.get_database_identity()?;
    let head = index.get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?;
    let frontier = index.get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?;
    assert_eq!(identity.is_some(), expected, "database identity presence");
    assert_eq!(head.is_some(), expected, "head sequence presence");
    assert_eq!(frontier.is_some(), expected, "durable frontier presence");

    if expected {
        DatabaseIdentityV0::decode(identity.as_deref().expect("identity present"))?
            .validate_against(format.format_version, format.database_uuid)?;
        assert!(is_encoded_head_seq_zero(
            head.as_deref().expect("head sequence present")
        ));
        assert!(is_encoded_empty_durable_frontier(
            frontier.as_deref().expect("durable frontier present")
        ));
    }
    Ok(())
}

fn complete_vlog_header(database_uuid: [u8; 16], file_id: u32) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(
        &PageHeader {
            file_id,
            page_no: 0,
        }
        .encode()?,
    );
    encoded.extend_from_slice(&VLogFileHeader::new(database_uuid, file_id).encode()?);
    Ok(encoded)
}

fn refresh_page_header_crc(encoded: &mut [u8]) {
    let checksum = crc32c::crc32c(&encoded[..12]);
    encoded[12..16].copy_from_slice(&checksum.to_le_bytes());
}

fn refresh_file_header_crc(encoded: &mut [u8]) {
    let checksum = crc32c::crc32c(&encoded[16..60]);
    encoded[60..64].copy_from_slice(&checksum.to_le_bytes());
}

fn assert_flush_queue_is_not_serviced(index: &FjallBackend, key: &[u8]) -> TestResult {
    assert_eq!(index.outstanding_flushes(), 0, "unexpected pending flush");
    index.insert_without_keyspace_durability(None, key, b"value")?;
    assert!(
        index.rotate_user_memtable_without_wait()?,
        "the non-empty user memtable should have been rotated"
    );
    assert!(index.outstanding_flushes() > 0, "flush was not enqueued");

    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        assert!(
            index.outstanding_flushes() > 0,
            "a Fjall background worker serviced the preparation-only backend"
        );
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn create_fifo(path: &Path) -> io::Result<()> {
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "FIFO test path contains a NUL byte",
        )
    })?;
    // SAFETY: `path` is a live, NUL-terminated C string for the duration of
    // the call, and `mode` is valid on each supported Unix target.
    if unsafe { mkfifo(path.as_ptr(), 0o600) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[test]
fn create_if_missing_and_error_if_exists_follow_the_frozen_matrix() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");

    let missing_options = Options::default();
    expect_prepare_error(
        prepare_open(&missing_options, &root),
        StorageErrorKind::NotFound,
    );
    assert!(!root.exists());

    let mut missing_error_if_exists = Options::default();
    missing_error_if_exists.error_if_exists = true;
    expect_prepare_error(
        prepare_open(&missing_error_if_exists, &root),
        StorageErrorKind::NotFound,
    );
    assert!(!root.exists());

    let empty_root = folder.path().join("empty-root");
    fs::create_dir(&empty_root)?;
    expect_prepare_error(
        prepare_open(&missing_options, &empty_root),
        StorageErrorKind::NotFound,
    );
    assert!(empty_root.join("LOCK").is_file());

    let unexplained_root = folder.path().join("unexplained-root");
    fs::create_dir(&unexplained_root)?;
    fs::create_dir(unexplained_root.join("index"))?;
    expect_prepare_error(
        prepare_open(&create_options(), &unexplained_root),
        StorageErrorKind::InvalidLayout,
    );
    assert!(!unexplained_root.join(FORMAT_FILE_NAME).exists());

    let mut create = create_options();
    create.error_if_exists = true;
    let created = prepare_open(&create, &root)?;
    assert!(root.join(FORMAT_FILE_NAME).is_file());
    drop(created);

    let mut existing_error = Options::default();
    existing_error.error_if_exists = true;
    expect_prepare_error(
        prepare_open(&existing_error, &root),
        StorageErrorKind::InvalidArgument,
    );

    let reopened_with_create = prepare_open(&create_options(), &root)?;
    drop(reopened_with_create);

    let reopened = prepare_open(&Options::default(), &root)?;
    assert_eq!(reopened.format().format_version, 0);
    Ok(())
}

#[test]
fn successful_initialization_publishes_matching_format_identity_head_and_frontier() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let prepared = prepare_open(&create_options(), &root)?;

    let format_bytes = fs::read(root.join(FORMAT_FILE_NAME))?;
    assert_eq!(format_bytes, prepared.format().encode()?);
    assert!(!root.join(FORMAT_TEMP_FILE_NAME).exists());
    assert!(root.join("LOCK").is_file());
    assert!(root.join("index").is_dir());
    assert!(root.join("vlog").is_dir());
    assert!(prepared.inventory().vlog_files.is_empty());

    let identity_bytes = prepared
        .index()
        .get_database_identity()?
        .expect("database identity");
    let identity = DatabaseIdentityV0::decode(&identity_bytes)?;
    identity.validate_against(
        prepared.format().format_version,
        prepared.format().database_uuid,
    )?;
    assert_eq!(identity.identity_format_version, 0);
    assert_eq!(identity.keyspace_layout_version, 0);

    let head = prepared
        .index()
        .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
        .expect("head sequence");
    let frontier = prepared
        .index()
        .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
        .expect("durable frontier");
    assert!(is_encoded_head_seq_zero(&head));
    assert!(is_encoded_empty_durable_frontier(&frontier));
    assert_eq!(
        prepared.root_lock().identity().canonical_path,
        fs::canonicalize(&root)?
    );
    Ok(())
}

#[test]
fn create_and_reopen_preparation_do_not_run_fjall_background_workers() -> TestResult {
    let folder = TempDir::new()?;

    let create_root = folder.path().join("create");
    let created = prepare_open(&create_options(), &create_root)?;
    assert_flush_queue_is_not_serviced(created.index(), b"created")?;
    drop(created);

    let reopen_root = folder.path().join("reopen");
    drop(prepare_open(&create_options(), &reopen_root)?);
    let reopened = prepare_open(&Options::default(), &reopen_root)?;
    assert_flush_queue_is_not_serviced(reopened.index(), b"reopened")?;
    Ok(())
}

#[test]
fn open_preparation_holds_the_root_lock_across_path_aliases() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let alias = folder.path().join("db-alias");
    let prepared = prepare_open(&create_options(), &root)?;
    std::os::unix::fs::symlink(&root, &alias)?;

    expect_prepare_error(
        prepare_open(&Options::default(), &alias),
        StorageErrorKind::Busy,
    );
    drop(prepared);
    assert!(prepare_open(&Options::default(), &alias).is_ok());
    Ok(())
}

#[test]
fn public_db_open_does_not_publish_or_initialize_a_stage_seven_preparation() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let error = match db::Db::open(&create_options(), &root) {
        Ok(_) => panic!("stage 7 published a public Db"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Unsupported);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn format_and_format_temp_together_fail_closed() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    drop(prepare_open(&create_options(), &root)?);
    fs::copy(
        root.join(FORMAT_FILE_NAME),
        root.join(FORMAT_TEMP_FILE_NAME),
    )?;

    expect_prepare_error(
        prepare_open(&Options::default(), &root),
        StorageErrorKind::InvalidLayout,
    );
    assert!(root.join(FORMAT_FILE_NAME).exists());
    assert!(root.join(FORMAT_TEMP_FILE_NAME).exists());
    Ok(())
}

#[test]
fn format_and_format_temp_presence_wins_over_malformed_contents() -> TestResult {
    let folder = TempDir::new()?;

    let malformed_temp_root = folder.path().join("malformed-temp");
    drop(prepare_open(&create_options(), &malformed_temp_root)?);
    fs::write(
        malformed_temp_root.join(FORMAT_TEMP_FILE_NAME),
        b"malformed temporary format",
    )?;
    expect_prepare_error(
        prepare_open(&Options::default(), &malformed_temp_root),
        StorageErrorKind::InvalidLayout,
    );
    assert!(malformed_temp_root.join(FORMAT_FILE_NAME).exists());
    assert!(malformed_temp_root.join(FORMAT_TEMP_FILE_NAME).exists());

    let malformed_final_root = folder.path().join("malformed-final");
    drop(prepare_open(&create_options(), &malformed_final_root)?);
    fs::copy(
        malformed_final_root.join(FORMAT_FILE_NAME),
        malformed_final_root.join(FORMAT_TEMP_FILE_NAME),
    )?;
    fs::write(
        malformed_final_root.join(FORMAT_FILE_NAME),
        b"malformed final format",
    )?;
    expect_prepare_error(
        prepare_open(&Options::default(), &malformed_final_root),
        StorageErrorKind::InvalidLayout,
    );
    assert!(malformed_final_root.join(FORMAT_FILE_NAME).exists());
    assert!(malformed_final_root.join(FORMAT_TEMP_FILE_NAME).exists());
    Ok(())
}

#[test]
fn managed_root_objects_and_vlog_entries_never_follow_symlinks() -> TestResult {
    for managed_name in [FORMAT_FILE_NAME, "index", "vlog"] {
        let folder = TempDir::new()?;
        let root = folder.path().join("db");
        drop(prepare_open(&create_options(), &root)?);
        let managed = root.join(managed_name);
        let outside = folder.path().join(format!("outside-{managed_name}"));
        fs::rename(&managed, &outside)?;
        std::os::unix::fs::symlink(&outside, &managed)?;

        expect_prepare_error(
            prepare_open(&Options::default(), &root),
            StorageErrorKind::InvalidLayout,
        );
        assert!(outside.exists());
    }

    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    drop(prepare_open(&create_options(), &root)?);
    let outside = folder.path().join("outside-vlog");
    fs::write(&outside, b"outside")?;
    std::os::unix::fs::symlink(&outside, root.join("vlog/D000000.data"))?;
    expect_prepare_error(
        prepare_open(&Options::default(), &root),
        StorageErrorKind::InvalidLayout,
    );
    assert_eq!(fs::read(outside)?, b"outside");
    Ok(())
}

#[test]
fn managed_objects_reject_wrong_types_and_unknown_vlog_names() -> TestResult {
    let folder = TempDir::new()?;
    let directory_root = folder.path().join("directory-root");
    fs::create_dir(&directory_root)?;
    fs::create_dir(directory_root.join(FORMAT_FILE_NAME))?;
    expect_prepare_error(
        prepare_open(&Options::default(), &directory_root),
        StorageErrorKind::InvalidLayout,
    );

    let file_root = folder.path().join("file-root");
    drop(prepare_open(&create_options(), &file_root)?);
    fs::remove_dir_all(file_root.join("index"))?;
    fs::write(file_root.join("index"), b"not a directory")?;
    expect_prepare_error(
        prepare_open(&Options::default(), &file_root),
        StorageErrorKind::InvalidLayout,
    );

    let unknown_root = folder.path().join("unknown-vlog-root");
    drop(prepare_open(&create_options(), &unknown_root)?);
    fs::write(unknown_root.join("vlog/not-a-vlog-file"), b"")?;
    expect_prepare_error(
        prepare_open(&Options::default(), &unknown_root),
        StorageErrorKind::InvalidLayout,
    );

    let special_root = folder.path().join("special-root");
    drop(prepare_open(&create_options(), &special_root)?);
    fs::remove_file(special_root.join(FORMAT_FILE_NAME))?;
    create_fifo(&special_root.join(FORMAT_FILE_NAME))?;
    expect_prepare_error(
        prepare_open(&Options::default(), &special_root),
        StorageErrorKind::InvalidLayout,
    );
    Ok(())
}

#[test]
fn unrelated_regular_files_in_the_root_are_preserved() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    fs::create_dir(&root)?;
    let unrelated = root.join("operator-notes.txt");
    let contents = b"not managed by rustkv";
    fs::write(&unrelated, contents)?;

    drop(prepare_open(&create_options(), &root)?);
    assert_eq!(fs::read(&unrelated)?, contents);

    drop(prepare_open(&Options::default(), &root)?);
    assert_eq!(fs::read(&unrelated)?, contents);
    Ok(())
}

#[test]
fn complete_vlog_headers_are_inventoried_and_cross_checked_with_format() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let prepared = prepare_open(&create_options(), &root)?;
    let database_uuid = prepared.format().database_uuid;
    drop(prepared);

    let valid = complete_vlog_header(database_uuid, 0)?;
    fs::write(root.join("vlog/D000000.data"), &valid)?;
    let prepared = prepare_open(&Options::default(), &root)?;
    assert_eq!(prepared.inventory().vlog_files.len(), 1);
    assert_eq!(prepared.inventory().vlog_files[0].file_id, 0);
    assert_eq!(prepared.inventory().vlog_files[0].len, 64);
    drop(prepared);

    let mut foreign = Vec::new();
    foreign.extend_from_slice(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
    );
    foreign.extend_from_slice(&VLogFileHeader::new([0xA5; 16], 0).encode()?);
    fs::write(root.join("vlog/D000000.data"), foreign)?;
    expect_prepare_error(
        prepare_open(&Options::default(), &root),
        StorageErrorKind::Corruption,
    );
    Ok(())
}

#[test]
fn vlog_inventory_defers_empty_partial_gapped_and_multiple_candidates() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    drop(prepare_open(&create_options(), &root)?);

    fs::write(root.join("vlog/D000000.data"), [])?;

    let partial_page = PageHeader {
        file_id: 2,
        page_no: 0,
    }
    .encode()?;
    fs::write(root.join("vlog/D000002.data"), &partial_page[..15])?;

    let complete_page = PageHeader {
        file_id: 4,
        page_no: 0,
    }
    .encode()?;
    fs::write(root.join("vlog/D000004.data"), complete_page)?;

    let mut partial_file_header = PageHeader {
        file_id: 6,
        page_no: 0,
    }
    .encode()?
    .to_vec();
    partial_file_header.resize(63, 0);
    fs::write(root.join("vlog/D000006.data"), partial_file_header)?;

    fs::write(root.join("vlog/D999999.data"), [])?;

    let prepared = prepare_open(&Options::default(), &root)?;
    let inventoried = prepared
        .inventory()
        .vlog_files
        .iter()
        .map(|entry| (entry.file_id, entry.len))
        .collect::<Vec<_>>();
    assert_eq!(
        inventoried,
        vec![(0, 0), (2, 15), (4, 16), (6, 63), (999_999, 0)]
    );
    Ok(())
}

#[test]
fn vlog_inventory_accepts_a_file_exactly_at_the_format_limit() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let prepared = prepare_open(&create_options(), &root)?;
    let database_uuid = prepared.format().database_uuid;
    drop(prepared);

    let path = root.join("vlog/D000000.data");
    fs::write(&path, complete_vlog_header(database_uuid, 0)?)?;
    let file = fs::OpenOptions::new().write(true).open(&path)?;
    file.set_len(MAX_VLOG_FILE_SIZE)?;
    drop(file);

    let prepared = prepare_open(&Options::default(), &root)?;
    assert_eq!(prepared.inventory().vlog_files.len(), 1);
    assert_eq!(prepared.inventory().vlog_files[0].file_id, 0);
    assert_eq!(prepared.inventory().vlog_files[0].len, MAX_VLOG_FILE_SIZE);
    Ok(())
}

#[test]
fn damaged_vlog_headers_report_open_recovery_context() -> TestResult {
    for (case, expected, damage) in [
        ("page-header-crc", StorageErrorKind::Corruption, 0_u8),
        ("file-header-crc", StorageErrorKind::Corruption, 1_u8),
        (
            "file-header-version",
            StorageErrorKind::IncompatibleFormat,
            2_u8,
        ),
    ] {
        let folder = TempDir::new()?;
        let root = folder.path().join(case);
        let prepared = prepare_open(&create_options(), &root)?;
        let database_uuid = prepared.format().database_uuid;
        drop(prepared);

        let mut encoded = complete_vlog_header(database_uuid, 0)?;
        match damage {
            0 => encoded[0] ^= 0xFF,
            1 => encoded[60] ^= 0xFF,
            2 => {
                encoded[24..28].copy_from_slice(&1_u32.to_le_bytes());
                let checksum = crc32c::crc32c(&encoded[16..60]);
                encoded[60..64].copy_from_slice(&checksum.to_le_bytes());
            }
            _ => unreachable!(),
        }
        fs::write(root.join("vlog/D000000.data"), encoded)?;

        let error = expect_prepare_error(prepare_open(&Options::default(), &root), expected);
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery, "{case}");
        assert_eq!(
            error.retry_advice,
            if expected == StorageErrorKind::IncompatibleFormat {
                RetryAdvice::DoNotRetry
            } else {
                RetryAdvice::RestoreOrRepair
            },
            "{case}"
        );
    }
    Ok(())
}

#[test]
fn vlog_inventory_rejects_each_header_topology_mismatch() -> TestResult {
    for case in [
        "page-file-id",
        "first-page-number",
        "file-header-file-id",
        "page-size",
        "max-file-size",
    ] {
        let folder = TempDir::new()?;
        let root = folder.path().join(case);
        let prepared = prepare_open(&create_options(), &root)?;
        let database_uuid = prepared.format().database_uuid;
        drop(prepared);

        let mut encoded = complete_vlog_header(database_uuid, 0)?;
        match case {
            "page-file-id" => {
                encoded[4..8].copy_from_slice(&1_u32.to_le_bytes());
                refresh_page_header_crc(&mut encoded);
            }
            "first-page-number" => {
                encoded[8..12].copy_from_slice(&1_u32.to_le_bytes());
                refresh_page_header_crc(&mut encoded);
            }
            "file-header-file-id" => {
                encoded[44..48].copy_from_slice(&1_u32.to_le_bytes());
                refresh_file_header_crc(&mut encoded);
            }
            "page-size" => {
                encoded[48..52].copy_from_slice(&(VLOG_PAGE_SIZE as u32 / 2).to_le_bytes());
                refresh_file_header_crc(&mut encoded);
            }
            "max-file-size" => {
                encoded[52..60].copy_from_slice(&(MAX_VLOG_FILE_SIZE - 1).to_le_bytes());
                refresh_file_header_crc(&mut encoded);
            }
            _ => unreachable!(),
        }
        fs::write(root.join("vlog/D000000.data"), encoded)?;

        let error = expect_prepare_error(
            prepare_open(&Options::default(), &root),
            StorageErrorKind::Corruption,
        );
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery, "{case}");
        assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair, "{case}");
    }
    Ok(())
}

#[test]
fn vlog_inventory_rejects_files_larger_than_the_format_limit() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    drop(prepare_open(&create_options(), &root)?);

    let path = root.join("vlog/D000000.data");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.set_len(MAX_VLOG_FILE_SIZE + 1)?;
    drop(file);

    let error = expect_prepare_error(
        prepare_open(&Options::default(), &root),
        StorageErrorKind::InvalidLayout,
    );
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
    assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair);
    Ok(())
}

#[test]
fn final_format_requires_identity_before_any_other_open_state_is_accepted() -> TestResult {
    let folder = TempDir::new()?;

    let missing_root = folder.path().join("missing-identity");
    let prepared = prepare_open(&create_options(), &missing_root)?;
    prepared
        .index()
        .overwrite_database_identity_for_test(None)?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &missing_root),
        StorageErrorKind::Corruption,
    );
    assert!(!missing_root.join(FORMAT_TEMP_FILE_NAME).exists());

    let damaged_root = folder.path().join("damaged-identity");
    let prepared = prepare_open(&create_options(), &damaged_root)?;
    let mut damaged = prepared.index().get_database_identity()?.expect("identity");
    damaged[0] ^= 0xFF;
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&damaged))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &damaged_root),
        StorageErrorKind::Corruption,
    );

    let short_root = folder.path().join("short-identity");
    let prepared = prepare_open(&create_options(), &short_root)?;
    let mut short = prepared.index().get_database_identity()?.expect("identity");
    short.pop();
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&short))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &short_root),
        StorageErrorKind::Corruption,
    );

    let bad_crc_root = folder.path().join("bad-crc-identity");
    let prepared = prepare_open(&create_options(), &bad_crc_root)?;
    let mut bad_crc = prepared.index().get_database_identity()?.expect("identity");
    bad_crc[31] ^= 0xFF;
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&bad_crc))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &bad_crc_root),
        StorageErrorKind::Corruption,
    );

    let mismatched_root = folder.path().join("mismatched-identity");
    let prepared = prepare_open(&create_options(), &mismatched_root)?;
    let foreign_batch = initialization_batch(0, [0x5A; 16]).expect("foreign identity batch");
    let foreign_identity = match &foreign_batch.operations()[0] {
        IndexMutation::InitializeDatabaseIdentity { encoded_identity } => encoded_identity,
        _ => panic!("initialization batch identity must be first"),
    };
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(foreign_identity))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &mismatched_root),
        StorageErrorKind::InvalidLayout,
    );

    let incompatible_root = folder.path().join("incompatible-identity");
    let prepared = prepare_open(&create_options(), &incompatible_root)?;
    let mut incompatible = prepared.index().get_database_identity()?.expect("identity");
    incompatible[4..6].copy_from_slice(&1_u16.to_le_bytes());
    let checksum = crc32c::crc32c(&incompatible[..28]);
    incompatible[28..32].copy_from_slice(&checksum.to_le_bytes());
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&incompatible))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &incompatible_root),
        StorageErrorKind::IncompatibleFormat,
    );

    let zero_uuid_root = folder.path().join("zero-uuid-identity");
    let prepared = prepare_open(&create_options(), &zero_uuid_root)?;
    let mut zero_uuid = prepared.index().get_database_identity()?.expect("identity");
    zero_uuid[10..26].fill(0);
    let checksum = crc32c::crc32c(&zero_uuid[..28]);
    zero_uuid[28..32].copy_from_slice(&checksum.to_le_bytes());
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&zero_uuid))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &zero_uuid_root),
        StorageErrorKind::Corruption,
    );

    let unknown_layout_root = folder.path().join("unknown-keyspace-layout");
    let prepared = prepare_open(&create_options(), &unknown_layout_root)?;
    let mut unknown_layout = prepared.index().get_database_identity()?.expect("identity");
    unknown_layout[26..28].copy_from_slice(&1_u16.to_le_bytes());
    let checksum = crc32c::crc32c(&unknown_layout[..28]);
    unknown_layout[28..32].copy_from_slice(&checksum.to_le_bytes());
    prepared
        .index()
        .overwrite_database_identity_for_test(Some(&unknown_layout))?;
    drop(prepared);
    expect_prepare_error(
        prepare_open(&Options::default(), &unknown_layout_root),
        StorageErrorKind::IncompatibleFormat,
    );
    Ok(())
}

#[test]
fn final_identity_with_any_missing_initialization_companion_is_corruption() -> TestResult {
    let folder = TempDir::new()?;
    for (case, missing_key) in [
        ("missing-head", HEAD_SEQ_KEY),
        ("missing-frontier", DURABLE_FRONTIER_KEY),
    ] {
        let root = folder.path().join(case);
        let prepared = prepare_open(&create_options(), &root)?;
        let mut batch = IndexAtomicBatch::new();
        batch
            .try_push(IndexMutation::DeleteInternal {
                space: InternalIndexSpace::System,
                key: missing_key.to_vec(),
            })
            .expect("valid test mutation");
        prepared
            .index()
            .commit_atomic(batch, IndexCommitMode::SyncAll)
            .expect("remove initialization companion for corruption test");
        drop(prepared);

        expect_prepare_error(
            prepare_open(&Options::default(), &root),
            StorageErrorKind::Corruption,
        );
    }
    Ok(())
}

#[test]
fn interrupted_initialization_rejects_invalid_companion_values_as_corruption() -> TestResult {
    for case in ["nonzero-head", "damaged-frontier-crc"] {
        let folder = TempDir::new()?;
        let root = folder.path().join(case);
        expect_prepare_error(
            prepare_open_with_fault(
                &create_options(),
                &root,
                InitializationFault::AfterCommitBeforeFormat,
            ),
            StorageErrorKind::Io,
        );

        let index = FjallBackend::open_existing_for_open_preparation(
            &root.join("index"),
            Options::default().fjall_index_options(),
        )?;
        let (key, replacement) = if case == "nonzero-head" {
            (HEAD_SEQ_KEY, 1_u64.to_le_bytes().to_vec())
        } else {
            let mut frontier = index
                .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
                .expect("initialized frontier");
            let last = frontier.last_mut().expect("nonempty frontier encoding");
            *last ^= 0xFF;
            (DURABLE_FRONTIER_KEY, frontier)
        };
        index.insert_for_test_sync_all(Some(InternalIndexSpace::System), key, &replacement)?;
        drop(index);

        let error = expect_prepare_error(
            prepare_open(&create_options(), &root),
            StorageErrorKind::Corruption,
        );
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery, "{case}");
        assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file());
        assert!(!root.join(FORMAT_FILE_NAME).exists());
    }
    Ok(())
}

#[test]
fn interrupted_initialization_iterator_errors_use_open_recovery_context() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    expect_prepare_error(
        prepare_open_with_fault(
            &create_options(),
            &root,
            InitializationFault::AfterCommitBeforeFormat,
        ),
        StorageErrorKind::Io,
    );
    let temporary_format = FormatMetadataV0::decode(&fs::read(root.join(FORMAT_TEMP_FILE_NAME))?)?;
    let index = FjallBackend::open_existing_for_open_preparation(
        &root.join("index"),
        Options::default().fjall_index_options(),
    )?;
    for successful_entries in [0, 1] {
        // 0 fails in the user iterator (normally Iterator/Read); 1 passes the
        // two empty scans and fails midway through the system scan (normally
        // Recovery/Recovery). Both are part of the enclosing Open operation.
        index.inject_iterator_error_after(Some(successful_entries));
        let error = match validate_interrupted_index_for_test(&index, &temporary_format) {
            Ok(()) => panic!("injected interrupted-initialization iterator error was ignored"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::Io);
        assert_eq!(error.operation, Operation::Open);
        assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
        assert_eq!(error.retry_advice, RetryAdvice::FixEnvironmentAndReopen);
        assert!(error.write_outcome.is_none());
        assert!(error.instance_state.is_none());
    }
    Ok(())
}

#[test]
fn swapping_real_index_directories_between_databases_fails_closed() -> TestResult {
    let folder = TempDir::new()?;
    let first_root = folder.path().join("first");
    let second_root = folder.path().join("second");
    let first = prepare_open(&create_options(), &first_root)?;
    let second = prepare_open(&create_options(), &second_root)?;
    assert_ne!(first.format().database_uuid, second.format().database_uuid);
    drop(first);
    drop(second);

    let staging = folder.path().join("index-staging");
    fs::rename(first_root.join("index"), &staging)?;
    fs::rename(second_root.join("index"), first_root.join("index"))?;
    fs::rename(staging, second_root.join("index"))?;

    for root in [&first_root, &second_root] {
        expect_prepare_error(
            prepare_open(&Options::default(), root),
            StorageErrorKind::InvalidLayout,
        );
    }
    Ok(())
}

#[test]
fn initialization_interruptions_preserve_temp_and_are_reinitialized_only_when_proven() -> TestResult
{
    for (fault, triple_applied) in [
        (InitializationFault::BeforeCommit, false),
        (InitializationFault::CommitUnknown, true),
        (InitializationFault::AfterCommitBeforeFormat, true),
    ] {
        let folder = TempDir::new()?;
        let root = folder.path().join("db");
        expect_prepare_error(
            prepare_open_with_fault(&create_options(), &root, fault),
            StorageErrorKind::Io,
        );
        assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file());
        assert!(!root.join(FORMAT_FILE_NAME).exists());

        let temporary_format =
            FormatMetadataV0::decode(&fs::read(root.join(FORMAT_TEMP_FILE_NAME))?)?;

        let index = FjallBackend::open_existing_for_open_preparation(
            &root.join("index"),
            Options::default().fjall_index_options(),
        )?;
        assert_initialization_triple(&index, triple_applied, &temporary_format)?;
        drop(index);

        expect_prepare_error(
            prepare_open(&Options::default(), &root),
            StorageErrorKind::NotFound,
        );
        assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file());

        let mut recreate = create_options();
        recreate.error_if_exists = true;
        let prepared = prepare_open(&recreate, &root)?;
        assert!(root.join(FORMAT_FILE_NAME).is_file());
        assert!(!root.join(FORMAT_TEMP_FILE_NAME).exists());
        assert!(prepared.index().get_database_identity()?.is_some());
    }
    Ok(())
}

#[test]
fn interrupted_initialization_with_identity_rejects_every_noninitial_state() -> TestResult {
    let cases: [(&str, Option<InternalIndexSpace>, &[u8]); 4] = [
        ("user-state", None, b"unexpected-user-key"),
        (
            "transaction-state",
            Some(InternalIndexSpace::Transaction),
            b"unexpected-transaction-key",
        ),
        (
            "recovery-state",
            Some(InternalIndexSpace::System),
            b"recovery_state",
        ),
        (
            "extra-system-state",
            Some(InternalIndexSpace::System),
            b"unexpected-system-key",
        ),
    ];

    for (case, space, key) in cases {
        let folder = TempDir::new()?;
        let root = folder.path().join(case);
        expect_prepare_error(
            prepare_open_with_fault(
                &create_options(),
                &root,
                InitializationFault::AfterCommitBeforeFormat,
            ),
            StorageErrorKind::Io,
        );

        let index = FjallBackend::open_existing_for_open_preparation(
            &root.join("index"),
            Options::default().fjall_index_options(),
        )?;
        assert!(
            index.get_database_identity()?.is_some(),
            "{case}: initialization identity should already exist"
        );
        index.insert_for_test_sync_all(space, key, b"pollution")?;
        drop(index);

        let error = expect_prepare_error(
            prepare_open(&create_options(), &root),
            StorageErrorKind::InvalidLayout,
        );
        assert_eq!(error.protocol_stage, ProtocolStage::Preflight, "{case}");
        assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file(), "{case}");
        assert!(!root.join(FORMAT_FILE_NAME).exists(), "{case}");
        assert!(root.join("index").is_dir(), "{case}");

        let index = FjallBackend::open_existing_for_open_preparation(
            &root.join("index"),
            Options::default().fjall_index_options(),
        )?;
        let persisted = match space {
            None => index.get_user(key, None)?,
            Some(space) => index.get_internal(space, key)?,
        };
        assert_eq!(persisted.as_deref(), Some(&b"pollution"[..]), "{case}");
    }
    Ok(())
}

#[test]
fn initialization_crash_boundaries_preserve_an_atomic_triple_and_recover() -> TestResult {
    for (mode, triple_applied) in [
        ("before_commit", false),
        ("commit_unknown", true),
        ("after_commit", true),
    ] {
        let folder = TempDir::new()?;
        let root = folder.path().join("db");
        let status = run_child_bounded(
            Command::new(std::env::current_exe()?)
                .args(["--exact", "initialization_crash_child", "--nocapture"])
                .env(INITIALIZATION_CHILD_ENV, "1")
                .env(INITIALIZATION_PATH_ENV, &root)
                .env(INITIALIZATION_MODE_ENV, mode),
        )?;
        assert_eq!(status.code(), Some(INITIALIZATION_CRASH_EXIT_CODE));
        assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file());
        assert!(!root.join(FORMAT_FILE_NAME).exists());

        let temporary_format =
            FormatMetadataV0::decode(&fs::read(root.join(FORMAT_TEMP_FILE_NAME))?)?;
        let index = FjallBackend::open_existing_for_open_preparation(
            &root.join("index"),
            Options::default().fjall_index_options(),
        )?;
        assert_initialization_triple(&index, triple_applied, &temporary_format)?;
        drop(index);

        let prepared = prepare_open(&create_options(), &root)?;
        assert!(root.join(FORMAT_FILE_NAME).is_file());
        assert!(!root.join(FORMAT_TEMP_FILE_NAME).exists());
        assert_initialization_triple(prepared.index(), true, prepared.format())?;
    }
    Ok(())
}

#[test]
fn initialization_crash_child() -> TestResult {
    if std::env::var_os(INITIALIZATION_CHILD_ENV).is_none() {
        return Ok(());
    }

    let path = std::env::var_os(INITIALIZATION_PATH_ENV)
        .map(std::path::PathBuf::from)
        .expect("initialization crash path must be provided");
    let mode =
        std::env::var(INITIALIZATION_MODE_ENV).expect("initialization crash mode must be provided");
    let fault = match mode.as_str() {
        "before_commit" => InitializationFault::CrashBeforeCommit,
        "commit_unknown" => InitializationFault::CrashCommitUnknown,
        "after_commit" => InitializationFault::CrashAfterCommitBeforeFormat,
        _ => panic!("unknown initialization crash mode: {mode}"),
    };

    let _unexpected = prepare_open_with_fault(&create_options(), &path, fault);
    panic!("initialization crash fault returned instead of exiting");
}

#[test]
fn unexplained_state_under_format_temp_is_not_deleted_or_reinitialized() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    expect_prepare_error(
        prepare_open_with_fault(&create_options(), &root, InitializationFault::BeforeCommit),
        StorageErrorKind::Io,
    );

    let index = FjallBackend::open_existing(
        &root.join("index"),
        Options::default().fjall_index_options(),
    )?;
    index.insert_for_test_sync_all(None, b"unexpected-user-key", b"pointer")?;
    drop(index);

    expect_prepare_error(
        prepare_open(&create_options(), &root),
        StorageErrorKind::InvalidLayout,
    );
    assert!(root.join(FORMAT_TEMP_FILE_NAME).is_file());
    assert!(root.join("index").is_dir());
    assert!(!root.join(FORMAT_FILE_NAME).exists());
    Ok(())
}
