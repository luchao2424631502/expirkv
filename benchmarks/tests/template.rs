use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{
    BackendKind, BenchConfig, BenchmarkWorkspace, TemplateBuildFault, TemplateErrorKind, Trace,
    Workload, build_test_template, build_test_template_with_fault, encode_key, prepare_run,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn both_real_templates_publish_closed_validate_and_restore_isolated_copies() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(backend_kind.as_str());
        let sibling = area.path().join("user-sentinel");
        std::fs::write(&sibling, b"preserve")?;
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;
        assert_eq!(template.backend_kind(), backend_kind);
        assert_eq!(template.config(), &config);
        assert_eq!(template.validate()?.record_count, 1_000);

        let first = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::RandomGet,
            Some(&template),
            "copy-one",
        )?;
        let second = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::RandomGet,
            Some(&template),
            "copy-two",
        )?;

        let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
        let mut opened = first.open()?;
        assert!(opened.execute(&trace, 10)?.is_valid());
        opened.put_for_test(&encode_key(&config, 0).unwrap(), b"changed")?;
        opened.delete_for_test(&encode_key(&config, 1).unwrap())?;
        opened.close();

        let mut opened = second.open()?;
        assert!(opened.execute(&trace, 10)?.is_valid());
        opened.close();
        assert_eq!(second.validate_after_close()?.record_count, 1_000);
        assert_eq!(template.validate()?.record_count, 1_000);
        assert!(first.validate_after_close().is_err());

        first.cleanup(&workspace)?;
        second.cleanup(&workspace)?;
        assert!(template.path_for_test().is_dir());
        assert_eq!(std::fs::read(&sibling)?, b"preserve");
    }
    Ok(())
}

#[test]
fn restore_rejects_preexisting_target_and_an_open_template() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("restore-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, backend_kind, &config)?;
        let first = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::SingleDelete,
            Some(&template),
            "existing",
        )?;
        let error = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::SingleDelete,
            Some(&template),
            "existing",
        )
        .unwrap_err();
        assert_eq!(error.kind(), TemplateErrorKind::FileSystem);

        let open_template = template.open_for_test()?;
        assert!(open_template.is_open_for_test());
        let error = prepare_run(
            &workspace,
            backend_kind,
            &config,
            Workload::RandomGet,
            Some(&template),
            "while-open",
        )
        .unwrap_err();
        assert_eq!(error.kind(), TemplateErrorKind::FileSystem);
        assert!(error.message().contains("open"));
        drop(open_template);

        first.cleanup(&workspace)?;
    }
    Ok(())
}

#[test]
fn interrupted_build_never_publishes_or_leaves_a_reusable_temporary_template() -> TestResult {
    let config = test_config();
    for backend_kind in [BackendKind::RustKv, BackendKind::LevelDb] {
        let area = TestArea::new(&format!("interrupted-{}", backend_kind.as_str()));
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let error = build_test_template_with_fault(
            &workspace,
            backend_kind,
            &config,
            TemplateBuildFault::BeforePublish,
        )
        .unwrap_err();
        assert_eq!(error.kind(), TemplateErrorKind::Injected);
        let names = std::fs::read_dir(workspace.root())?
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(
            names.is_empty(),
            "unexpected incomplete artifacts: {names:?}"
        );
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn changed_missing_truncated_and_symlinked_template_layouts_are_rejected() -> TestResult {
    use std::os::unix::fs::symlink;

    for fault in [
        LayoutFault::Missing,
        LayoutFault::Truncated,
        LayoutFault::Symlink,
    ] {
        let config = test_config();
        let area = TestArea::new(fault.label());
        let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
        let template = build_test_template(&workspace, BackendKind::RustKv, &config)?;
        match fault {
            LayoutFault::Missing => {
                let file = first_regular_file(template.path_for_test(), false)?;
                std::fs::remove_file(file)?;
            }
            LayoutFault::Truncated => {
                let file = first_regular_file(template.path_for_test(), true)?;
                let length = std::fs::metadata(&file)?.len();
                OpenOptions::new()
                    .write(true)
                    .open(file)?
                    .set_len(length - 1)?;
            }
            LayoutFault::Symlink => {
                let target = area.path().join("outside");
                std::fs::write(&target, b"outside")?;
                symlink(target, template.path_for_test().join("unexpected-link"))?;
            }
        }
        let error = prepare_run(
            &workspace,
            BackendKind::RustKv,
            &config,
            Workload::RandomGet,
            Some(&template),
            "must-fail",
        )
        .unwrap_err();
        assert_eq!(error.kind(), TemplateErrorKind::FileSystem);
        assert!(!workspace.root().join("run-must-fail").exists());
    }
    Ok(())
}

#[test]
fn template_and_run_configuration_roles_cannot_be_mixed() -> TestResult {
    let config = test_config();
    let area = TestArea::new("roles");
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
    let template = build_test_template(&workspace, BackendKind::RustKv, &config)?;
    let other_config = BenchConfig::test_only(2_000, 100, 100, 100, 20);

    assert_eq!(
        prepare_run(
            &workspace,
            BackendKind::RustKv,
            &other_config,
            Workload::RandomGet,
            Some(&template),
            "wrong-config",
        )
        .unwrap_err()
        .kind(),
        TemplateErrorKind::TemplateMismatch
    );
    assert_eq!(
        prepare_run(
            &workspace,
            BackendKind::LevelDb,
            &config,
            Workload::RandomGet,
            Some(&template),
            "wrong-backend",
        )
        .unwrap_err()
        .kind(),
        TemplateErrorKind::TemplateMismatch
    );
    assert_eq!(
        prepare_run(
            &workspace,
            BackendKind::RustKv,
            &config,
            Workload::SinglePut,
            Some(&template),
            "insert-with-template",
        )
        .unwrap_err()
        .kind(),
        TemplateErrorKind::TemplateMismatch
    );
    assert_eq!(
        prepare_run(
            &workspace,
            BackendKind::RustKv,
            &config,
            Workload::SingleDelete,
            None,
            "delete-without-template",
        )
        .unwrap_err()
        .kind(),
        TemplateErrorKind::MissingTemplate
    );
    Ok(())
}

#[test]
fn invalid_template_configs_and_duplicate_publish_are_rejected_atomically() -> TestResult {
    let area = TestArea::new("configuration-and-publish");
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;

    let error =
        build_test_template(&workspace, BackendKind::RustKv, &BenchConfig::formal()).unwrap_err();
    assert_eq!(error.kind(), TemplateErrorKind::InvalidConfiguration);
    assert!(std::fs::read_dir(workspace.root())?.next().is_none());

    let non_thousand_multiple = BenchConfig::test_only(1_500, 100, 100, 100, 20);
    let error =
        build_test_template(&workspace, BackendKind::RustKv, &non_thousand_multiple).unwrap_err();
    assert_eq!(error.kind(), TemplateErrorKind::InvalidConfiguration);
    assert!(std::fs::read_dir(workspace.root())?.next().is_none());

    let config = test_config();
    let published = build_test_template(&workspace, BackendKind::RustKv, &config)?;
    let error = build_test_template(&workspace, BackendKind::RustKv, &config).unwrap_err();
    assert_eq!(error.kind(), TemplateErrorKind::FileSystem);
    assert_eq!(published.validate()?.record_count, 1_000);
    let names = std::fs::read_dir(workspace.root())?
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(names, [std::ffi::OsString::from("template-rustkv")]);
    Ok(())
}

#[derive(Clone, Copy)]
enum LayoutFault {
    Missing,
    Truncated,
    Symlink,
}

impl LayoutFault {
    const fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Truncated => "truncated",
            Self::Symlink => "symlink",
        }
    }
}

fn test_config() -> BenchConfig {
    BenchConfig::test_only(1_000, 100, 100, 100, 20)
}

fn first_regular_file(root: &Path, require_nonempty: bool) -> TestResult<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() && (!require_nonempty || metadata.len() > 0) {
                return Ok(path);
            }
        }
    }
    Err("template contains no suitable regular file".into())
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
            "kv-bench-b5-template-{label}-{}-{time}-{sequence}",
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
