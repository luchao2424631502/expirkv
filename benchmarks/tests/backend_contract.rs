use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{
    BackendError, BackendKind, BackendOperation, BackendResult, BatchItem, BenchBackend,
    BenchConfig, ExpectedRecord, LevelDbBackend, RustKvBackend, ScanRequest, encode_key,
    fixed_value,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum BackendChoice {
    RustKv,
    LevelDb,
}

impl BackendChoice {
    const ALL: [Self; 2] = [Self::RustKv, Self::LevelDb];

    const fn label(self) -> &'static str {
        match self {
            Self::RustKv => "rustkv",
            Self::LevelDb => "leveldb",
        }
    }
}

fn open_backend(
    choice: BackendChoice,
    path: &Path,
    config: &BenchConfig,
) -> BackendResult<Box<dyn BenchBackend>> {
    match choice {
        BackendChoice::RustKv => {
            RustKvBackend::open(path, config).map(|backend| Box::new(backend) as _)
        }
        BackendChoice::LevelDb => {
            LevelDbBackend::open(path, config).map(|backend| Box::new(backend) as _)
        }
    }
}

#[test]
fn both_backends_match_basic_operations_and_reopen_byte_for_byte() -> TestResult {
    let config = BenchConfig::test_only(500, 100, 100, 64, 32);
    for choice in BackendChoice::ALL {
        let temporary = TestDirectory::new(choice.label());
        let path = temporary.path().join("basic-db");
        {
            let backend = open_backend(choice, &path, &config)?;
            assert_eq!(
                backend.get(b"missing")?,
                kv_bench::GetResult {
                    found: false,
                    value_length: 0,
                }
            );
            backend.put(b"empty-value", b"")?;
            assert_eq!(backend.get(b"empty-value")?.value_length, 0);
            assert!(backend.get(b"empty-value")?.found);

            let one_kib = vec![0x5a; 1_024];
            backend.put(b"one-kib", &one_kib)?;
            assert_eq!(backend.get(b"one-kib")?.value_length, 1_024);
            backend.put(b"one-kib", b"replacement")?;
            assert_eq!(backend.get(b"one-kib")?.value_length, 11);

            backend.put(b"delete-me", b"present")?;
            backend.delete(b"delete-me")?;
            backend.delete(b"never-existed")?;
            assert!(!backend.get(b"delete-me")?.found);

            assert_terminal_state(
                backend.as_ref(),
                &[
                    (b"empty-value".as_slice(), b"".as_slice()),
                    (b"one-kib".as_slice(), b"replacement".as_slice()),
                ],
            )?;
        }

        let reopened = open_backend(choice, &path, &config)?;
        assert_terminal_state(
            reopened.as_ref(),
            &[
                (b"empty-value".as_slice(), b"".as_slice()),
                (b"one-kib".as_slice(), b"replacement".as_slice()),
            ],
        )?;
    }
    Ok(())
}

#[test]
fn both_backends_preserve_mixed_batch_order_and_handle_100_item_batches() -> TestResult {
    let config = BenchConfig::test_only(500, 100, 100, 64, 32);
    let value = fixed_value(&config);
    for choice in BackendChoice::ALL {
        let temporary = TestDirectory::new(choice.label());
        let mixed_path = temporary.path().join("mixed-db");
        let mixed = open_backend(choice, &mixed_path, &config)?;
        mixed.write_batch(&[])?;
        assert_terminal_state(mixed.as_ref(), &[])?;
        mixed.write_batch(&[
            BatchItem::Put {
                key: b"b",
                value: b"old",
            },
            BatchItem::Put {
                key: b"a",
                value: b"temporary",
            },
            BatchItem::Put {
                key: b"b",
                value: b"new",
            },
            BatchItem::Delete { key: b"a" },
            BatchItem::Put {
                key: b"c",
                value: b"final",
            },
            BatchItem::Delete { key: b"absent" },
            BatchItem::Put {
                key: b"a",
                value: b"reborn",
            },
        ])?;
        assert_terminal_state(
            mixed.as_ref(),
            &[
                (b"a".as_slice(), b"reborn".as_slice()),
                (b"b".as_slice(), b"new".as_slice()),
                (b"c".as_slice(), b"final".as_slice()),
            ],
        )?;

        let batch_path = temporary.path().join("batch-100-db");
        let batch = open_backend(choice, &batch_path, &config)?;
        let keys: Vec<_> = (0..100)
            .map(|id| encode_key(&config, id).unwrap())
            .collect();
        let puts: Vec<_> = keys
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect();
        batch.write_batch(&puts)?;
        let expected: Vec<_> = keys
            .iter()
            .map(|key| ExpectedRecord { key, value: &value })
            .collect();
        assert_eq!(
            batch.iterator_scan(ScanRequest::full(b"", 100, &expected))?,
            kv_bench::ScanResult {
                record_count: 100,
                value_bytes: 100 * value.len(),
            }
        );

        let deletes: Vec<_> = keys.iter().map(|key| BatchItem::Delete { key }).collect();
        batch.write_batch(&deletes)?;
        assert_terminal_state(batch.as_ref(), &[])?;
    }
    Ok(())
}

#[test]
fn both_backends_match_iterator_seek_limits_and_validation_modes() -> TestResult {
    let config = BenchConfig::test_only(500, 100, 100, 64, 32);
    let value = fixed_value(&config);
    for choice in BackendChoice::ALL {
        let temporary = TestDirectory::new(choice.label());
        let backend = open_backend(choice, &temporary.path().join("iterator-db"), &config)?;
        assert_eq!(
            backend.iterator_scan(ScanRequest::timed(b"", 100, value.len()))?,
            kv_bench::ScanResult {
                record_count: 0,
                value_bytes: 0,
            }
        );
        assert_terminal_state(backend.as_ref(), &[])?;

        let sparse_ids = [10_u64, 20, 30];
        let sparse_keys: Vec<_> = sparse_ids
            .into_iter()
            .map(|id| encode_key(&config, id).unwrap())
            .collect();
        for key in &sparse_keys {
            backend.put(key, &value)?;
        }
        assert_eq!(
            backend.iterator_scan(ScanRequest::timed(&sparse_keys[0], 1, value.len()))?,
            kv_bench::ScanResult {
                record_count: 1,
                value_bytes: value.len(),
            }
        );
        let between = encode_key(&config, 11).unwrap();
        let expected_sparse = [
            ExpectedRecord {
                key: &sparse_keys[1],
                value: &value,
            },
            ExpectedRecord {
                key: &sparse_keys[2],
                value: &value,
            },
        ];
        assert_eq!(
            backend.iterator_scan(ScanRequest::full(&between, 100, &expected_sparse))?,
            kv_bench::ScanResult {
                record_count: 2,
                value_bytes: 2 * value.len(),
            }
        );
        let beyond = encode_key(&config, 499).unwrap();
        assert_eq!(
            backend
                .iterator_scan(ScanRequest::timed(&beyond, 100, value.len()))?
                .record_count,
            0
        );
        assert_eq!(
            backend
                .iterator_scan(ScanRequest::timed(&sparse_keys[0], 0, value.len()))?
                .record_count,
            0
        );
        assert_eq!(
            backend.iterator_scan(ScanRequest::timed(b"", 100, value.len()))?,
            kv_bench::ScanResult {
                record_count: 3,
                value_bytes: 3 * value.len(),
            }
        );

        let wrong_record = [ExpectedRecord {
            key: &sparse_keys[0],
            value: b"wrong-value-bytes",
        }];
        let wrong_bytes = backend
            .iterator_scan(ScanRequest::full(&sparse_keys[0], 1, &wrong_record))
            .unwrap_err();
        assert_scan_error(&wrong_bytes, choice, "value differs from expected bytes");

        let wrong_key_record = [ExpectedRecord {
            key: &sparse_keys[1],
            value: &value,
        }];
        let wrong_key = backend
            .iterator_scan(ScanRequest::full(&sparse_keys[0], 1, &wrong_key_record))
            .unwrap_err();
        assert_scan_error(&wrong_key, choice, "key differs from expected bytes");

        let unexpected_extra = backend
            .iterator_scan(ScanRequest::full(&sparse_keys[0], 1, &[]))
            .unwrap_err();
        assert_scan_error(&unexpected_extra, choice, "unexpected extra record");

        let two_expected = [
            ExpectedRecord {
                key: &sparse_keys[0],
                value: &value,
            },
            ExpectedRecord {
                key: &sparse_keys[1],
                value: &value,
            },
        ];
        let too_few = backend
            .iterator_scan(ScanRequest::full(&sparse_keys[0], 1, &two_expected))
            .unwrap_err();
        assert_scan_error(&too_few, choice, "expected");

        let wrong_length = backend
            .iterator_scan(ScanRequest::timed(b"", 1, value.len() - 1))
            .unwrap_err();
        assert_scan_error(&wrong_length, choice, "value length");

        let consecutive_keys: Vec<_> = (100..200)
            .map(|id| encode_key(&config, id).unwrap())
            .collect();
        let puts: Vec<_> = consecutive_keys
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect();
        backend.write_batch(&puts)?;
        let expected: Vec<_> = consecutive_keys
            .iter()
            .map(|key| ExpectedRecord { key, value: &value })
            .collect();
        assert_eq!(
            backend.iterator_scan(ScanRequest::full(&consecutive_keys[0], 100, &expected))?,
            kv_bench::ScanResult {
                record_count: 100,
                value_bytes: 100 * value.len(),
            }
        );
    }
    Ok(())
}

fn assert_scan_error(error: &BackendError, choice: BackendChoice, expected_text: &str) {
    assert_eq!(error.operation(), BackendOperation::IteratorScan);
    assert_eq!(
        error.backend(),
        match choice {
            BackendChoice::RustKv => BackendKind::RustKv,
            BackendChoice::LevelDb => BackendKind::LevelDb,
        }
    );
    assert!(
        error.source_text().contains(expected_text),
        "unexpected scan error text: {}",
        error.source_text()
    );
}

#[test]
fn backend_and_operation_labels_are_complete_and_stable() {
    assert_eq!(BackendKind::RustKv.as_str(), "rustkv");
    assert_eq!(BackendKind::LevelDb.as_str(), "leveldb");
    for (operation, label) in [
        (BackendOperation::Open, "open"),
        (BackendOperation::Get, "get"),
        (BackendOperation::Put, "put"),
        (BackendOperation::Delete, "delete"),
        (BackendOperation::WriteBatch, "write_batch"),
        (BackendOperation::IteratorScan, "iterator_scan"),
    ] {
        assert_eq!(operation.as_str(), label);
    }
}

fn assert_terminal_state(backend: &dyn BenchBackend, records: &[(&[u8], &[u8])]) -> TestResult {
    let expected: Vec<_> = records
        .iter()
        .map(|(key, value)| ExpectedRecord { key, value })
        .collect();
    let result = backend.iterator_scan(ScanRequest::full(b"", records.len() + 1, &expected))?;
    assert_eq!(result.record_count, records.len());
    assert_eq!(
        result.value_bytes,
        records.iter().map(|(_, value)| value.len()).sum::<usize>()
    );
    Ok(())
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kv-bench-b2-contract-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique temporary directory must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
