use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use rustkv::{
    Db, DestroyStage, ManagedObject, Operation, Options, ProtocolStage, StorageError,
    StorageErrorKind, WriteOptions,
};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn create_database(path: &Path, with_value: bool) -> TestResult {
    let db = Db::open(&create_options(), path)?;
    if with_value {
        db.put(&WriteOptions { sync: true }, b"key", &vec![0x5A; 40_000])?;
    }
    drop(db);
    Ok(())
}

fn add_orphan_manifest(
    index: &Path,
) -> std::result::Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    for entry in fs::read_dir(index.join("keyspaces"))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let current = fs::read(entry.path().join("current"))?;
        let version = u64::from_le_bytes(current[0..8].try_into()?);
        let source = entry.path().join(format!("v{version}"));
        let relative = PathBuf::from("keyspaces")
            .join(entry.file_name())
            .join(format!("v{}", u64::MAX));
        fs::copy(source, index.join(&relative))?;
        return Ok(relative);
    }
    Err("Fjall index has no keyspace tree".into())
}

fn snapshot_tree(
    root: &Path,
) -> std::result::Result<BTreeMap<PathBuf, Option<Vec<u8>>>, Box<dyn std::error::Error + Send + Sync>>
{
    fn visit(
        root: &Path,
        current: &Path,
        snapshot: &mut BTreeMap<PathBuf, Option<Vec<u8>>>,
    ) -> TestResult {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let relative = entry.path().strip_prefix(root)?.to_path_buf();
            if file_type.is_dir() {
                snapshot.insert(relative, None);
                visit(root, &entry.path(), snapshot)?;
            } else if file_type.is_file() {
                snapshot.insert(relative, Some(fs::read(entry.path())?));
            }
        }
        Ok(())
    }

    let mut snapshot = BTreeMap::new();
    visit(root, root, &mut snapshot)?;
    Ok(snapshot)
}

fn expect_destroy_error(path: &Path) -> StorageError {
    Db::destroy(path, &Options::default()).expect_err("Destroy unexpectedly succeeded")
}

fn assert_context(
    error: &StorageError,
    object: fn(&ManagedObject) -> bool,
    stage: fn(&DestroyStage) -> bool,
    partially_deleted: bool,
) {
    assert_eq!(error.operation, Operation::Destroy);
    assert_eq!(error.protocol_stage, ProtocolStage::Lifecycle);
    assert_eq!(error.write_outcome, None);
    assert_eq!(error.instance_state, None);
    let context = error.destroy_failure.as_ref().expect("destroy context");
    assert!(object(&context.failed_object));
    assert!(stage(&context.stage));
    assert_eq!(context.partially_deleted, partially_deleted);
}

#[test]
fn destroy_removes_only_managed_objects_and_never_follows_index_symlinks() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("db");
    create_database(&root, true)?;

    let unmanaged = root.join("application-note.txt");
    fs::write(&unmanaged, b"keep")?;
    let outside = temporary.path().join("outside.txt");
    fs::write(&outside, b"outside must survive")?;
    symlink(&outside, root.join("index").join("external-link"))?;

    Db::destroy(&root, &Options::default())?;
    assert!(root.is_dir());
    assert!(root.join("LOCK").is_file());
    assert_eq!(fs::read(&unmanaged)?, b"keep");
    assert_eq!(fs::read(&outside)?, b"outside must survive");
    assert!(!root.join("FORMAT").exists());
    assert!(!root.join("FORMAT.tmp").exists());
    assert!(!root.join("index").exists());
    assert!(!root.join("vlog").exists());

    Db::destroy(&root, &Options::default())?;
    let recreated = Db::open(&create_options(), &root)?;
    drop(recreated);
    Ok(())
}

#[test]
fn destroy_is_busy_while_any_database_handle_holds_the_root_lock() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let clone = db.clone();
    drop(db);

    let error = expect_destroy_error(&root);
    assert_eq!(error.kind, StorageErrorKind::Busy);
    assert_context(
        &error,
        |object| matches!(object, ManagedObject::Lock),
        |stage| matches!(stage, DestroyStage::AcquireLock),
        false,
    );
    drop(clone);
    Db::destroy(&root, &Options::default())?;
    Ok(())
}

#[test]
fn missing_path_and_root_with_only_unmanaged_files_are_idempotent_success() -> TestResult {
    let temporary = TempDir::new()?;
    let missing = temporary.path().join("missing");
    Db::destroy(&missing, &Options::default())?;
    assert!(!missing.exists());

    let root = temporary.path().join("ordinary");
    fs::create_dir(&root)?;
    fs::write(root.join("keep.bin"), b"keep")?;
    Db::destroy(&root, &Options::default())?;
    assert_eq!(fs::read(root.join("keep.bin"))?, b"keep");
    assert!(root.join("LOCK").is_file());
    Ok(())
}

#[test]
fn swapped_index_directories_fail_identity_validation_before_any_deletion() -> TestResult {
    let temporary = TempDir::new()?;
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    create_database(&first, true)?;
    create_database(&second, true)?;
    let parked = temporary.path().join("parked-index");
    fs::rename(first.join("index"), &parked)?;
    fs::rename(second.join("index"), first.join("index"))?;
    fs::rename(&parked, second.join("index"))?;

    for root in [&first, &second] {
        let error = expect_destroy_error(root);
        assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
        assert_context(
            &error,
            |object| matches!(object, ManagedObject::DatabaseIdentity),
            |stage| matches!(stage, DestroyStage::Inventory),
            false,
        );
        assert!(root.join("FORMAT").is_file());
        assert!(root.join("index").is_dir());
        assert!(root.join("vlog").is_dir());
        assert!(root.join("vlog").join("D000000.data").is_file());
    }
    Ok(())
}

#[test]
fn destroy_identity_validation_never_runs_recovery_against_the_source_index() -> TestResult {
    let temporary = TempDir::new()?;
    let first = temporary.path().join("first-read-only");
    let second = temporary.path().join("second-read-only");
    create_database(&first, true)?;
    create_database(&second, true)?;
    let orphan = add_orphan_manifest(&second.join("index"))?;
    let source_before = snapshot_tree(&second.join("index"))?;

    let parked = temporary.path().join("parked-read-only-index");
    fs::rename(first.join("index"), &parked)?;
    fs::rename(second.join("index"), first.join("index"))?;

    let error = expect_destroy_error(&first);
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_context(
        &error,
        |object| matches!(object, ManagedObject::DatabaseIdentity),
        |stage| matches!(stage, DestroyStage::Inventory),
        false,
    );
    assert!(
        first.join("index").join(orphan).is_file(),
        "Destroy validation ran Fjall recovery against the source index"
    );
    assert!(first.join("FORMAT").is_file());
    assert!(first.join("vlog").is_dir());
    assert_eq!(snapshot_tree(&first.join("index"))?, source_before);
    Ok(())
}

#[derive(Clone, Copy)]
enum IdentityDamage {
    Missing,
    BadCrc,
    UnknownVersion,
}

fn damage_identity(root: &Path, damage: IdentityDamage) -> TestResult {
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let system = database.keyspace("rustkv_system_metadata", KeyspaceCreateOptions::default)?;
    let key = b"database_identity";
    match damage {
        IdentityDamage::Missing => system.remove(key)?,
        IdentityDamage::BadCrc | IdentityDamage::UnknownVersion => {
            let mut identity = system.get(key)?.expect("identity exists").to_vec();
            if matches!(damage, IdentityDamage::BadCrc) {
                identity[10] ^= 0xFF;
            } else {
                identity[4..6].copy_from_slice(&1_u16.to_le_bytes());
                let checksum = crc32c::crc32c(&identity[..28]);
                identity[28..32].copy_from_slice(&checksum.to_le_bytes());
            }
            system.insert(key, identity)?;
        }
    }
    database.persist(PersistMode::SyncAll)?;
    Ok(())
}

#[test]
fn missing_corrupt_and_unknown_identity_all_fail_before_first_delete() -> TestResult {
    let temporary = TempDir::new()?;
    for (name, damage, expected_kind) in [
        (
            "missing",
            IdentityDamage::Missing,
            StorageErrorKind::Corruption,
        ),
        (
            "bad-crc",
            IdentityDamage::BadCrc,
            StorageErrorKind::Corruption,
        ),
        (
            "unknown-version",
            IdentityDamage::UnknownVersion,
            StorageErrorKind::IncompatibleFormat,
        ),
    ] {
        let root = temporary.path().join(name);
        create_database(&root, true)?;
        damage_identity(&root, damage)?;
        let error = expect_destroy_error(&root);
        assert_eq!(error.kind, expected_kind);
        assert_context(
            &error,
            |object| matches!(object, ManagedObject::DatabaseIdentity),
            |stage| matches!(stage, DestroyStage::Inventory),
            false,
        );
        assert!(root.join("FORMAT").is_file());
        assert!(root.join("index").is_dir());
        assert!(root.join("vlog").join("D000000.data").is_file());
    }
    Ok(())
}

#[test]
fn foreign_vlog_header_and_missing_final_index_fail_before_first_delete() -> TestResult {
    let temporary = TempDir::new()?;

    let foreign_vlog = temporary.path().join("foreign-vlog");
    create_database(&foreign_vlog, true)?;
    let vlog_file = foreign_vlog.join("vlog").join("D000000.data");
    let file = OpenOptions::new().read(true).write(true).open(&vlog_file)?;
    let mut header = [0_u8; 48];
    file.read_exact_at(&mut header, 16)?;
    header[12] ^= 0xFF;
    let checksum = crc32c::crc32c(&header[..44]);
    header[44..48].copy_from_slice(&checksum.to_le_bytes());
    file.write_all_at(&header, 16)?;
    file.sync_all()?;
    let error = expect_destroy_error(&foreign_vlog);
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_context(
        &error,
        |object| matches!(object, ManagedObject::VLogFile { file_id: 0 }),
        |stage| matches!(stage, DestroyStage::Inventory),
        false,
    );
    assert!(foreign_vlog.join("FORMAT").is_file());
    assert!(foreign_vlog.join("index").is_dir());
    assert!(vlog_file.is_file());

    let missing_index = temporary.path().join("missing-index");
    create_database(&missing_index, true)?;
    fs::remove_dir_all(missing_index.join("index"))?;
    let error = expect_destroy_error(&missing_index);
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_context(
        &error,
        |object| matches!(object, ManagedObject::DatabaseIdentity),
        |stage| matches!(stage, DestroyStage::Inventory),
        false,
    );
    assert!(missing_index.join("FORMAT").is_file());
    assert!(missing_index.join("vlog").join("D000000.data").is_file());
    Ok(())
}

#[test]
fn temporary_format_with_three_empty_keyspaces_and_no_identity_is_destroyable() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("temporary-format");
    create_database(&root, false)?;
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let system = database.keyspace("rustkv_system_metadata", KeyspaceCreateOptions::default)?;
    for key in [
        b"database_identity".as_slice(),
        b"head_seq".as_slice(),
        b"durable_frontier".as_slice(),
    ] {
        system.remove(key)?;
    }
    database.persist(PersistMode::SyncAll)?;
    drop(system);
    drop(database);
    fs::rename(root.join("FORMAT"), root.join("FORMAT.tmp"))?;

    Db::destroy(&root, &Options::default())?;
    assert!(root.join("LOCK").is_file());
    assert!(!root.join("FORMAT.tmp").exists());
    assert!(!root.join("index").exists());
    assert!(!root.join("vlog").exists());
    Ok(())
}

#[test]
fn temporary_format_with_missing_vlog_still_requires_an_interrupted_empty_index() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("temporary-missing-vlog");
    create_database(&root, true)?;
    fs::rename(root.join("FORMAT"), root.join("FORMAT.tmp"))?;
    fs::remove_dir_all(root.join("vlog"))?;

    let error = expect_destroy_error(&root);
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_context(
        &error,
        |object| matches!(object, ManagedObject::DatabaseIdentity),
        |stage| matches!(stage, DestroyStage::Inventory),
        false,
    );
    assert!(root.join("FORMAT.tmp").is_file());
    assert!(root.join("index").is_dir());
    Ok(())
}
