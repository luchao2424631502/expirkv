#![allow(dead_code, unused_imports)]

use std::path::PathBuf;

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};
#[path = "../src/commit/descriptor.rs"]
mod commit;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/vlog/mod.rs"]
mod vlog;

mod db {
    use std::path::PathBuf;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct VLogInventoryEntry {
        pub(crate) file_id: u32,
        pub(crate) len: u64,
        pub(crate) path: PathBuf,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub(crate) struct ManagedInventory {
        pub(crate) vlog_files: Vec<VLogInventoryEntry>,
    }
}

#[path = "../src/recovery/topology.rs"]
mod topology;

use commit::{DurableVLogEnd, VLogPos};
use db::{ManagedInventory, VLogInventoryEntry};
use format::FormatMetadataV0;
use lock::RootLock;
use tempfile::TempDir;
use topology::{PhysicalTail, RecoveryTopology};
use vlog::format::{
    MAX_VLOG_FILE_SIZE, PAGE_HEADER_ENCODED_LEN, PageHeader, VLogFileHeader, VLogGeometry,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x74; 16];

fn entry(file_id: u32, len: u64) -> VLogInventoryEntry {
    VLogInventoryEntry {
        file_id,
        len,
        path: PathBuf::from(format!("D{file_id:06}.data")),
    }
}

fn stable(file_id: u32, offset: u64) -> DurableVLogEnd {
    DurableVLogEnd::Position(VLogPos { file_id, offset })
}

fn assert_corruption(result: Result<RecoveryTopology>) {
    let error = result.expect_err("invalid stable topology must fail closed");
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
}

#[test]
fn empty_stable_prefix_classifies_arbitrary_files_as_physical_suffix() -> TestResult {
    let inventory = ManagedInventory {
        vlog_files: vec![entry(2, 17), entry(7, 91)],
    };
    let topology = RecoveryTopology::analyze(&inventory, DurableVLogEnd::Empty)?;
    assert_eq!(topology.file_count, 2);
    assert_eq!(
        topology.physical_tail,
        PhysicalTail::Position(VLogPos {
            file_id: 7,
            offset: 91,
        })
    );
    assert!(topology.contains_end(&inventory, DurableVLogEnd::Empty));
    assert!(topology.has_suffix_after(&inventory, DurableVLogEnd::Empty));
    Ok(())
}

#[test]
fn stable_prefix_requires_continuous_sealed_lower_files_and_long_enough_boundary() -> TestResult {
    let valid = ManagedInventory {
        vlog_files: vec![entry(0, MAX_VLOG_FILE_SIZE), entry(1, 9_000), entry(2, 70)],
    };
    let topology = RecoveryTopology::analyze(&valid, stable(1, 8_000))?;
    assert!(topology.contains_end(&valid, stable(1, 8_000)));
    assert!(topology.has_suffix_after(&valid, stable(1, 8_000)));

    assert_corruption(RecoveryTopology::analyze(
        &ManagedInventory {
            vlog_files: vec![entry(1, 9_000)],
        },
        stable(1, 8_000),
    ));
    assert_corruption(RecoveryTopology::analyze(
        &ManagedInventory {
            vlog_files: vec![entry(0, MAX_VLOG_FILE_SIZE - 1), entry(1, 9_000)],
        },
        stable(1, 8_000),
    ));
    assert_corruption(RecoveryTopology::analyze(
        &ManagedInventory {
            vlog_files: vec![entry(0, MAX_VLOG_FILE_SIZE), entry(1, 7_999)],
        },
        stable(1, 8_000),
    ));
    assert_corruption(RecoveryTopology::analyze(
        &ManagedInventory {
            vlog_files: vec![entry(0, 100), entry(0, 101)],
        },
        stable(0, 80),
    ));
    Ok(())
}

#[test]
fn accepted_end_and_physical_tail_are_kept_as_distinct_inputs() -> TestResult {
    let inventory = ManagedInventory {
        vlog_files: vec![entry(0, 1_000), entry(3, 200)],
    };
    let topology = RecoveryTopology::analyze(&inventory, stable(0, 500))?;
    assert_eq!(
        topology.physical_tail,
        PhysicalTail::Position(VLogPos {
            file_id: 3,
            offset: 200,
        })
    );
    assert!(topology.contains_end(&inventory, stable(0, 800)));
    assert!(!topology.contains_end(&inventory, stable(2, 1)));
    assert!(topology.has_suffix_after(&inventory, stable(0, 800)));
    assert!(!topology.has_suffix_after(&inventory, stable(3, 200)));
    Ok(())
}

#[test]
fn managed_inventory_rejects_wrong_uuid_and_illegal_object_type() -> TestResult {
    let wrong_uuid = prepared_root()?;
    write_headers(&wrong_uuid, "D000000.data", [0x99; 16], 0)?;
    let format = FormatMetadataV0::new(DATABASE_UUID)?;
    let lock = RootLock::acquire(wrong_uuid.path(), false)?.expect("root lock");
    let error = ManagedInventory::inspect(&lock, &format)
        .expect_err("existing VLog identity mismatch must fail closed");
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    drop(lock);

    let illegal_type = prepared_root()?;
    std::fs::create_dir(illegal_type.path().join("vlog/D000000.data"))?;
    let lock = RootLock::acquire(illegal_type.path(), false)?.expect("root lock");
    let error = ManagedInventory::inspect(&lock, &format)
        .expect_err("managed VLog name cannot designate a directory");
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    Ok(())
}

#[test]
fn managed_inventory_accepts_matching_headers_and_reports_owned_paths() -> TestResult {
    let root = prepared_root()?;
    write_headers(&root, "D000000.data", DATABASE_UUID, 0)?;
    let format = FormatMetadataV0::new(DATABASE_UUID)?;
    let lock = RootLock::acquire(root.path(), false)?.expect("root lock");
    let inventory = ManagedInventory::inspect(&lock, &format)?;
    assert_eq!(inventory.vlog_files.len(), 1);
    assert_eq!(inventory.vlog_files[0].file_id, 0);
    assert_eq!(
        inventory.vlog_files[0].len,
        (PAGE_HEADER_ENCODED_LEN + 48) as u64
    );
    assert!(inventory.vlog_files[0].path.is_absolute());
    Ok(())
}

fn prepared_root() -> TestResult<TempDir> {
    let root = tempfile::tempdir()?;
    {
        let lock = RootLock::acquire(root.path(), false)?.expect("root lock");
        std::fs::create_dir(root.path().join("index"))?;
        std::fs::create_dir(root.path().join("vlog"))?;
        std::fs::write(
            root.path().join("FORMAT"),
            FormatMetadataV0::new(DATABASE_UUID)?.encode()?,
        )?;
        drop(lock);
    }
    Ok(root)
}

fn write_headers(root: &TempDir, name: &str, database_uuid: [u8; 16], file_id: u32) -> TestResult {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(
        &PageHeader {
            file_id,
            page_no: 0,
        }
        .encode()?,
    );
    encoded.extend_from_slice(&VLogFileHeader::new(database_uuid, file_id).encode()?);
    std::fs::write(root.path().join("vlog").join(name), encoded)?;
    Ok(())
}
