use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{
    BackendKind, BackendResult, BatchItem, BenchBackend, BenchConfig, BenchmarkWorkspace,
    GetResult, ScanRequest, ScanResult, ScanValidation, TemplateErrorKind, Trace, ValidationError,
    Workload, build_test_template, encode_key, fixed_value, prepare_run, prewarm_full_dataset,
    validate_empty_dataset, validate_full_dataset,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn prewarm_is_one_complete_untimed_full_iterator_call() {
    let config = test_config();
    let backend = CountingFullScan::default();
    let summary = prewarm_full_dataset(&backend, &config).unwrap();
    assert_eq!(summary.record_count, 1_000);
    assert_eq!(summary.value_bytes, 1_000 * 1_024);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn validation_rejects_backend_summary_count_and_byte_mismatches() {
    let config = test_config();
    let count_mismatch = FixedScanResult {
        result: ScanResult {
            record_count: 999,
            value_bytes: 1_000 * 1_024,
        },
    };
    assert_eq!(
        validate_full_dataset(&count_mismatch, &config),
        Err(ValidationError::ResultCountMismatch {
            expected: 1_000,
            actual: 999,
        })
    );

    let byte_mismatch = FixedScanResult {
        result: ScanResult {
            record_count: 1_000,
            value_bytes: 1_000 * 1_024 - 1,
        },
    };
    assert_eq!(
        validate_full_dataset(&byte_mismatch, &config),
        Err(ValidationError::ResultValueBytesMismatch {
            expected: 1_000 * 1_024,
            actual: 1_000 * 1_024 - 1,
        })
    );

    let nonempty = FixedScanResult {
        result: ScanResult {
            record_count: 1,
            value_bytes: 1_024,
        },
    };
    assert_eq!(
        validate_empty_dataset(&nonempty),
        Err(ValidationError::ResultCountMismatch {
            expected: 0,
            actual: 1,
        })
    );
}

#[test]
fn both_backends_prepare_execute_close_reopen_and_validate_all_six_workloads() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("e2e-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;

        for workload in Workload::ALL {
            let label = format!("{}-{}", backend_kind.as_str(), workload.as_str());
            let needs_template = matches!(
                workload,
                Workload::RandomGet
                    | Workload::RangeScan
                    | Workload::SingleDelete
                    | Workload::BatchDelete
            );
            let prepared = prepare_run(
                &workspace,
                backend_kind,
                &config,
                workload,
                needs_template.then_some(&template),
                &label,
            )?;
            let trace = Trace::generate(&config, workload, 0).unwrap();
            let mut opened = prepared.open()?;
            assert_eq!(
                prepared.validate_after_close().unwrap_err().kind(),
                TemplateErrorKind::InvalidLifecycle,
                "an open run must not be reopened for terminal validation"
            );
            assert_eq!(opened.workload_for_test(), workload);
            assert_eq!(
                opened.prewarm_summary().is_some(),
                matches!(workload, Workload::RandomGet | Workload::RangeScan)
            );
            if let Some(prewarm) = opened.prewarm_summary() {
                assert_eq!(prewarm.record_count, 1_000);
                assert_eq!(prewarm.value_bytes, 1_000 * 1_024);
            }

            let result = opened.execute(&trace, 10)?;
            assert!(
                result.is_valid(),
                "{backend_kind:?} {workload} failed: {result:?}"
            );
            assert_eq!(result.completed_ops, trace.request_count() as u64);
            assert_eq!(
                result.metrics.as_ref().unwrap().latency().sample_count(),
                trace.request_count(),
                "prewarm must not add Runner latency samples"
            );
            opened.close();

            let terminal = prepared.validate_after_close()?;
            if matches!(workload, Workload::SingleDelete | Workload::BatchDelete) {
                assert_eq!(terminal.record_count, 0);
                assert_eq!(terminal.value_bytes, 0);
            } else {
                assert_eq!(terminal.record_count, 1_000);
                assert_eq!(terminal.value_bytes, 1_000 * 1_024);
            }
            prepared.cleanup(&workspace)?;
        }
        assert_eq!(template.validate()?.record_count, 1_000);
    }
    Ok(())
}

#[test]
fn both_backends_reject_missing_extra_wrong_key_wrong_value_and_residual_delete() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("faults-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;

        for fault in ValidationFault::ALL {
            let prepared = prepare_run(
                &workspace,
                backend_kind,
                &config,
                if fault == ValidationFault::ResidualDelete {
                    Workload::SingleDelete
                } else {
                    Workload::RandomGet
                },
                Some(&template),
                &format!("{}-{}", backend_kind.as_str(), fault.label()),
            )?;
            let trace = Trace::generate(
                &config,
                if fault == ValidationFault::ResidualDelete {
                    Workload::SingleDelete
                } else {
                    Workload::RandomGet
                },
                0,
            )
            .unwrap();
            let mut opened = prepared.open()?;
            assert!(opened.execute(&trace, 10)?.is_valid());
            let value = fixed_value(&config);
            match fault {
                ValidationFault::Missing => {
                    opened.delete_for_test(&encode_key(&config, 999).unwrap())?;
                }
                ValidationFault::Extra => {
                    let mut extra_key = [0_u8; 16];
                    extra_key[8..].copy_from_slice(&1_000_u64.to_be_bytes());
                    opened.put_for_test(&extra_key, &value)?;
                }
                ValidationFault::WrongKey => {
                    opened.delete_for_test(&encode_key(&config, 10).unwrap())?;
                    let mut wrong_key = encode_key(&config, 9).unwrap().to_vec();
                    wrong_key.push(0);
                    opened.put_for_test(&wrong_key, &value)?;
                }
                ValidationFault::WrongValue => {
                    opened.put_for_test(&encode_key(&config, 10).unwrap(), b"wrong-value")?;
                }
                ValidationFault::ResidualDelete => {
                    opened.put_for_test(&encode_key(&config, 999).unwrap(), &value)?;
                }
            }
            opened.close();

            let error = prepared.validate_after_close().unwrap_err();
            assert_eq!(error.kind(), TemplateErrorKind::Validation);
            prepared.cleanup(&workspace)?;
        }
    }
    Ok(())
}

#[test]
fn both_backends_bind_the_prepared_workload_and_allow_exactly_one_run() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("lifecycle-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;

        let unexecuted = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::RandomGet,
            Some(&template),
            "unexecuted",
        )?;
        unexecuted.open()?.close();
        assert_eq!(
            unexecuted.validate_after_close().unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle
        );
        unexecuted.cleanup(&workspace)?;

        let prepared = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::RandomGet,
            Some(&template),
            "single-use",
        )?;
        let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
        let mut opened = prepared.open()?;
        assert!(opened.execute(&trace, 10)?.is_valid());
        assert_eq!(
            opened.execute(&trace, 1).unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle
        );
        opened.close();
        let reopen_error = match prepared.open() {
            Ok(opened) => {
                opened.close();
                panic!("a closed run directory was reopened")
            }
            Err(error) => error,
        };
        assert_eq!(reopen_error.kind(), TemplateErrorKind::InvalidLifecycle);
        assert_eq!(
            prepared.cleanup(&workspace).unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle,
            "an executed run cannot be cleaned before terminal validation"
        );
        assert_eq!(prepared.validate_after_close()?.record_count, 1_000);
        assert_eq!(
            prepared.validate_after_close().unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle
        );
        prepared.cleanup(&workspace)?;
        assert_eq!(
            prepared.cleanup(&workspace).unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle
        );

        let prepared = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::SingleDelete,
            Some(&template),
            "wrong-trace",
        )?;
        let wrong_trace = Trace::generate(&config, Workload::BatchDelete, 0).unwrap();
        let correct_trace = Trace::generate(&config, Workload::SingleDelete, 0).unwrap();
        let mut opened = prepared.open()?;
        assert_eq!(
            opened.execute(&wrong_trace, 10).unwrap_err().kind(),
            TemplateErrorKind::TemplateMismatch
        );
        assert_eq!(
            opened.execute(&correct_trace, 10).unwrap_err().kind(),
            TemplateErrorKind::InvalidLifecycle
        );
        opened.close();
        assert_eq!(
            prepared.validate_after_close().unwrap_err().kind(),
            TemplateErrorKind::Validation,
            "the mismatched BatchDelete Trace must perform no deletion"
        );
        prepared.cleanup(&workspace)?;
    }
    Ok(())
}

#[test]
fn both_backends_refuse_to_recreate_a_missing_delete_database_during_validation() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("missing-layout-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;
        let prepared = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::SingleDelete,
            Some(&template),
            "missing-layout",
        )?;
        let trace = Trace::generate(&config, Workload::SingleDelete, 0).unwrap();
        let mut opened = prepared.open()?;
        assert!(opened.execute(&trace, 10)?.is_valid());
        opened.close();

        let identity = prepared.path_for_test().join(match backend_kind {
            BackendKind::RustKv => "FORMAT",
            BackendKind::LevelDb => "CURRENT",
        });
        std::fs::remove_file(&identity)?;
        assert!(!identity.exists());
        assert_eq!(
            prepared.validate_after_close().unwrap_err().kind(),
            TemplateErrorKind::FileSystem
        );
        assert!(
            !identity.exists(),
            "terminal validation must not recreate a missing database"
        );
        prepared.cleanup(&workspace)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValidationFault {
    Missing,
    Extra,
    WrongKey,
    WrongValue,
    ResidualDelete,
}

impl ValidationFault {
    const ALL: [Self; 5] = [
        Self::Missing,
        Self::Extra,
        Self::WrongKey,
        Self::WrongValue,
        Self::ResidualDelete,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Extra => "extra",
            Self::WrongKey => "wrong-key",
            Self::WrongValue => "wrong-value",
            Self::ResidualDelete => "residual-delete",
        }
    }
}

#[derive(Default)]
struct CountingFullScan {
    calls: AtomicUsize,
}

impl BenchBackend for CountingFullScan {
    fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
        panic!("prewarm must not call get")
    }

    fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
        panic!("prewarm must not call put")
    }

    fn delete(&self, _key: &[u8]) -> BackendResult<()> {
        panic!("prewarm must not call delete")
    }

    fn write_batch(&self, _items: &[BatchItem<'_>]) -> BackendResult<()> {
        panic!("prewarm must not call write_batch")
    }

    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.start, b"");
        assert_eq!(request.limit, 1_001);
        match request.validation {
            ScanValidation::Full { expected } => {
                assert_eq!(expected.len(), 1_000);
                let config = test_config();
                let value = fixed_value(&config);
                for (id, record) in expected.iter().enumerate() {
                    assert_eq!(record.key, encode_key(&config, id as u64).unwrap());
                    assert_eq!(record.value, value);
                }
            }
            ScanValidation::Timed { .. } => panic!("prewarm must use full validation mode"),
        }
        Ok(ScanResult {
            record_count: 1_000,
            value_bytes: 1_000 * 1_024,
        })
    }
}

struct FixedScanResult {
    result: ScanResult,
}

impl BenchBackend for FixedScanResult {
    fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
        panic!("validation must not call get")
    }

    fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
        panic!("validation must not call put")
    }

    fn delete(&self, _key: &[u8]) -> BackendResult<()> {
        panic!("validation must not call delete")
    }

    fn write_batch(&self, _items: &[BatchItem<'_>]) -> BackendResult<()> {
        panic!("validation must not call write_batch")
    }

    fn iterator_scan(&self, _request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        Ok(self.result)
    }
}

fn test_config() -> BenchConfig {
    BenchConfig::test_only(1_000, 100, 100, 100, 20)
}

struct TestArea {
    path: PathBuf,
}

impl TestArea {
    fn new(label: &str) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after Unix epoch")
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kv-bench-b5-validation-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique test area must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestArea {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
