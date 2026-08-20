//! Fjall 3.1.8 capability spike for RustKV's compound commit protocol.
//!
//! This file intentionally talks to Fjall directly. It proves the public API
//! shape and observable behavior that the future private `IndexBackend` will
//! rely on; it is not the production adapter.
//!
//! The spike cannot prove power-loss behavior on every supported filesystem,
//! nor can Fjall's public API inject an error at every internal commit stage.
//! Consequently, a Fjall commit error after `commit()` is invoked must remain
//! conservatively classified as `CommitUnknown` unless a stronger, version-
//! locked contract proves that the batch was not applied.

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use std::{
    error::Error,
    path::Path,
    process::Command,
    sync::{Arc, Barrier},
    thread,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const USER_INDEX_NAME: &str = "rustkv_user_index";
const TX_METADATA_NAME: &str = "rustkv_txn_metadata";
const SYSTEM_METADATA_NAME: &str = "rustkv_system_metadata";

const CHILD_ENV: &str = "RUSTKV_FJALL_COMPOUND_CHILD";
const CHILD_PATH_ENV: &str = "RUSTKV_FJALL_COMPOUND_PATH";
const CHILD_SCENARIO_ENV: &str = "RUSTKV_FJALL_COMPOUND_SCENARIO";
const CHILD_EXIT_CODE: i32 = 37;

const HEAD_SEQ_KEY: &[u8] = b"head_seq";
const DURABLE_FRONTIER_KEY: &[u8] = b"durable_frontier";

struct CompoundKeyspaces {
    database: Database,
    user: Keyspace,
    tx: Keyspace,
    system: Keyspace,
}

fn keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .manual_journal_persist(true)
        .with_kv_separation(None)
}

fn open_compound(path: &Path) -> fjall::Result<CompoundKeyspaces> {
    let database = Database::builder(path)
        .manual_journal_persist(true)
        .open()?;
    let user = database.keyspace(USER_INDEX_NAME, keyspace_options)?;
    let tx = database.keyspace(TX_METADATA_NAME, keyspace_options)?;
    let system = database.keyspace(SYSTEM_METADATA_NAME, keyspace_options)?;

    Ok(CompoundKeyspaces {
        database,
        user,
        tx,
        system,
    })
}

fn bytes(value: Option<fjall::UserValue>) -> Option<Vec<u8>> {
    value.map(|value| value.to_vec())
}

fn seq_bytes(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

fn user_key(seq: u64) -> Vec<u8> {
    format!("user/{seq:020}").into_bytes()
}

fn tx_meta_key(seq: u64) -> Vec<u8> {
    format!("tx/{seq:020}/meta").into_bytes()
}

fn pointer_value(seq: u64) -> Vec<u8> {
    format!("pointer/{seq:020}").into_bytes()
}

fn tx_meta_value(seq: u64) -> Vec<u8> {
    format!("tx-meta/{seq:020}").into_bytes()
}

fn commit_transaction(
    keyspaces: &CompoundKeyspaces,
    seq: u64,
    mode: PersistMode,
) -> fjall::Result<()> {
    let mut batch = keyspaces.database.batch().durability(Some(mode));
    batch.insert(&keyspaces.user, user_key(seq), pointer_value(seq));
    batch.insert(&keyspaces.tx, tx_meta_key(seq), tx_meta_value(seq));
    batch.insert(&keyspaces.system, HEAD_SEQ_KEY, seq_bytes(seq));
    batch.commit()
}

fn assert_transaction(keyspaces: &CompoundKeyspaces, seq: u64) -> fjall::Result<()> {
    assert_eq!(
        bytes(keyspaces.user.get(user_key(seq))?),
        Some(pointer_value(seq))
    );
    assert_eq!(
        bytes(keyspaces.tx.get(tx_meta_key(seq))?),
        Some(tx_meta_value(seq))
    );
    assert_eq!(
        bytes(keyspaces.system.get(HEAD_SEQ_KEY)?),
        Some(seq_bytes(seq).to_vec())
    );
    Ok(())
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn three_keyspaces_open_with_fixed_names_and_without_kv_separation() -> TestResult {
    let folder = TempDir::new()?;
    let keyspaces = open_compound(folder.path())?;

    assert_eq!(&**keyspaces.user.name(), USER_INDEX_NAME);
    assert_eq!(&**keyspaces.tx.name(), TX_METADATA_NAME);
    assert_eq!(&**keyspaces.system.name(), SYSTEM_METADATA_NAME);

    assert!(!keyspaces.user.is_kv_separated());
    assert!(!keyspaces.tx.is_kv_separated());
    assert!(!keyspaces.system.is_kv_separated());

    let mut names = keyspaces
        .database
        .list_keyspace_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        vec![
            SYSTEM_METADATA_NAME.to_owned(),
            TX_METADATA_NAME.to_owned(),
            USER_INDEX_NAME.to_owned(),
        ]
    );

    Ok(())
}

#[test]
fn database_and_all_three_keyspaces_are_send_sync() -> TestResult {
    assert_send_sync::<Database>();
    assert_send_sync::<Keyspace>();

    let folder = TempDir::new()?;
    let keyspaces = open_compound(folder.path())?;
    let keyspaces = Arc::new(keyspaces);
    let threads = 8;
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::with_capacity(threads);

    for worker in 0..threads {
        let keyspaces = Arc::clone(&keyspaces);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> fjall::Result<()> {
            barrier.wait();
            let key = format!("probe/{worker:02}");
            assert_eq!(keyspaces.user.get(key.as_bytes())?, None);
            assert_eq!(keyspaces.tx.get(key.as_bytes())?, None);
            assert_eq!(keyspaces.system.get(key.as_bytes())?, None);
            Ok(())
        }));
    }

    for handle in handles {
        handle.join().expect("compound reader should not panic")?;
    }

    Ok(())
}

#[test]
fn owned_batch_is_atomically_visible_across_three_keyspaces() -> TestResult {
    let folder = TempDir::new()?;
    let keyspaces = open_compound(folder.path())?;
    let seq = 1;

    let mut batch = keyspaces
        .database
        .batch()
        .durability(Some(PersistMode::Buffer));
    batch.insert(&keyspaces.user, user_key(seq), b"old-pointer");
    batch.insert(&keyspaces.user, user_key(seq), pointer_value(seq));
    batch.insert(&keyspaces.tx, tx_meta_key(seq), tx_meta_value(seq));
    batch.insert(&keyspaces.system, HEAD_SEQ_KEY, seq_bytes(seq));

    assert_eq!(keyspaces.user.get(user_key(seq))?, None);
    assert_eq!(keyspaces.tx.get(tx_meta_key(seq))?, None);
    assert_eq!(keyspaces.system.get(HEAD_SEQ_KEY)?, None);

    batch.commit()?;
    assert_transaction(&keyspaces, seq)?;

    Ok(())
}

#[test]
fn database_snapshot_captures_one_cross_keyspace_version() -> TestResult {
    let folder = TempDir::new()?;
    let keyspaces = open_compound(folder.path())?;

    commit_transaction(&keyspaces, 1, PersistMode::Buffer)?;
    let snapshot = keyspaces.database.snapshot();
    commit_transaction(&keyspaces, 2, PersistMode::Buffer)?;

    assert_eq!(
        bytes(snapshot.get(&keyspaces.user, user_key(1))?),
        Some(pointer_value(1))
    );
    assert_eq!(snapshot.get(&keyspaces.user, user_key(2))?, None);
    assert_eq!(
        bytes(snapshot.get(&keyspaces.tx, tx_meta_key(1))?),
        Some(tx_meta_value(1))
    );
    assert_eq!(snapshot.get(&keyspaces.tx, tx_meta_key(2))?, None);
    assert_eq!(
        bytes(snapshot.get(&keyspaces.system, HEAD_SEQ_KEY)?),
        Some(seq_bytes(1).to_vec())
    );

    Ok(())
}

#[test]
fn descriptor_cleanup_can_be_batched_without_touching_user_state() -> TestResult {
    let folder = TempDir::new()?;
    let keyspaces = open_compound(folder.path())?;
    commit_transaction(&keyspaces, 1, PersistMode::Buffer)?;

    let mut cleanup = keyspaces
        .database
        .batch()
        .durability(Some(PersistMode::Buffer));
    cleanup.remove(&keyspaces.tx, tx_meta_key(1));
    cleanup.commit()?;

    assert_eq!(keyspaces.tx.get(tx_meta_key(1))?, None);
    assert_eq!(
        bytes(keyspaces.user.get(user_key(1))?),
        Some(pointer_value(1))
    );
    assert_eq!(
        bytes(keyspaces.system.get(HEAD_SEQ_KEY)?),
        Some(seq_bytes(1).to_vec())
    );

    Ok(())
}

#[test]
fn buffer_batch_recovers_all_three_keyspaces_without_drop() -> TestResult {
    run_crash_child_and_verify("buffer")
}

#[test]
fn sync_all_batch_recovers_all_three_keyspaces_without_drop() -> TestResult {
    run_crash_child_and_verify("sync_all")
}

#[test]
fn sync_all_frontier_batch_covers_prior_buffer_batches_at_api_level() -> TestResult {
    let folder = TempDir::new()?;
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "fjall_compound_crash_child", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(CHILD_PATH_ENV, folder.path())
        .env(CHILD_SCENARIO_ENV, "frontier")
        .status()?;
    assert_eq!(status.code(), Some(CHILD_EXIT_CODE));

    let keyspaces = open_compound(folder.path())?;
    assert_eq!(
        bytes(keyspaces.user.get(user_key(1))?),
        Some(pointer_value(1))
    );
    assert_eq!(
        bytes(keyspaces.user.get(user_key(2))?),
        Some(pointer_value(2))
    );
    assert_eq!(
        bytes(keyspaces.tx.get(tx_meta_key(1))?),
        Some(tx_meta_value(1))
    );
    assert_eq!(
        bytes(keyspaces.tx.get(tx_meta_key(2))?),
        Some(tx_meta_value(2))
    );
    assert_eq!(
        bytes(keyspaces.system.get(HEAD_SEQ_KEY)?),
        Some(seq_bytes(2).to_vec())
    );
    assert_eq!(
        bytes(keyspaces.system.get(DURABLE_FRONTIER_KEY)?),
        Some(seq_bytes(2).to_vec())
    );

    Ok(())
}

fn run_crash_child_and_verify(scenario: &str) -> TestResult {
    let folder = TempDir::new()?;
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "fjall_compound_crash_child", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(CHILD_PATH_ENV, folder.path())
        .env(CHILD_SCENARIO_ENV, scenario)
        .status()?;
    assert_eq!(status.code(), Some(CHILD_EXIT_CODE));

    let keyspaces = open_compound(folder.path())?;
    assert_transaction(&keyspaces, 1)?;
    Ok(())
}

#[test]
fn fjall_compound_crash_child() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_none() {
        return Ok(());
    }

    let path = std::env::var_os(CHILD_PATH_ENV).expect("compound child path must be provided");
    let scenario =
        std::env::var(CHILD_SCENARIO_ENV).expect("compound child scenario must be provided");
    let keyspaces = open_compound(Path::new(&path))?;

    match scenario.as_str() {
        "buffer" => commit_transaction(&keyspaces, 1, PersistMode::Buffer)?,
        "sync_all" => commit_transaction(&keyspaces, 1, PersistMode::SyncAll)?,
        "frontier" => {
            commit_transaction(&keyspaces, 1, PersistMode::Buffer)?;
            commit_transaction(&keyspaces, 2, PersistMode::Buffer)?;

            let mut frontier = keyspaces
                .database
                .batch()
                .durability(Some(PersistMode::SyncAll));
            frontier.insert(&keyspaces.system, DURABLE_FRONTIER_KEY, seq_bytes(2));
            frontier.commit()?;
        }
        other => panic!("unknown compound child scenario: {other}"),
    }

    // `process::exit` deliberately skips Rust destructors, including Fjall's
    // database Drop implementation. Reopen therefore observes only behavior
    // guaranteed by the explicit batch durability mode.
    std::process::exit(CHILD_EXIT_CODE);
}
