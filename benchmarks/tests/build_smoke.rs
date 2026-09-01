use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const EXPECTED_PROVENANCE: &str = concat!(
    "version=1.23\n",
    "commit=99b3c03b3284f5886f9ef9a4ef703d57373e61be\n",
    "archive_sha256=bc87b9bbc5674c91246a89813355e78401759761342cc049e1c3d56350a8a9d1\n",
    "build_type=Release\n",
    "shared_libraries=off\n",
    "crc32c_external=off\n",
    "snappy=off\n",
    "tcmalloc=off\n",
);

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn linked_leveldb_is_exactly_1_23() {
    assert_eq!(
        kv_bench::linked_leveldb_version(),
        kv_bench::EXPECTED_LEVELDB_VERSION
    );
}

#[test]
fn rustkv_public_api_is_available_through_the_path_dependency() {
    let _db: Option<rustkv::Db> = None;
    let _options = rustkv::Options::default();
    let _read_options = rustkv::ReadOptions::default();
    let _write_options = rustkv::WriteOptions::default();
    let _batch = rustkv::WriteBatch::new();
    let _iterator: Option<rustkv::DbIterator> = None;
}

#[test]
fn pinned_install_contains_headers_library_and_provenance() {
    let prefix = manifest_dir().join(".deps/leveldb-install");
    assert!(prefix.join("include/leveldb/c.h").is_file());
    assert!(prefix.join("include/leveldb/db.h").is_file());
    assert!(prefix.join("lib/libleveldb.a").is_file());

    let provenance = std::fs::read_to_string(prefix.join("share/kv_bench/leveldb-provenance.txt"))
        .expect("LevelDB provenance must be readable");
    assert_eq!(provenance, EXPECTED_PROVENANCE);
}

#[test]
fn bootstrap_reuses_an_exact_complete_install_on_repeated_runs() {
    let sandbox = TestDirectory::new("reusable-install");
    let fake_crate = sandbox.path().join("fake-benchmark");
    let scripts = fake_crate.join("scripts");
    let install = fake_crate.join(".deps/leveldb-install");
    std::fs::create_dir_all(&scripts).expect("fake scripts directory must be created");
    std::fs::create_dir_all(install.join("include/leveldb"))
        .expect("fake include directory must be created");
    std::fs::create_dir_all(install.join("lib")).expect("fake lib directory must be created");
    std::fs::create_dir_all(install.join("share/kv_bench"))
        .expect("fake provenance directory must be created");

    let copied_script = scripts.join("bootstrap_leveldb.sh");
    std::fs::copy(
        manifest_dir().join("scripts/bootstrap_leveldb.sh"),
        &copied_script,
    )
    .expect("bootstrap script must be copied");
    std::fs::write(install.join("include/leveldb/c.h"), "official C API")
        .expect("fake C header must be written");
    std::fs::write(
        install.join("include/leveldb/db.h"),
        "kMajorVersion = 1;\nkMinorVersion = 23;\n",
    )
    .expect("fake version header must be written");
    std::fs::write(install.join("lib/libleveldb.a"), "static library")
        .expect("fake library must be written");
    std::fs::write(
        install.join("share/kv_bench/leveldb-provenance.txt"),
        EXPECTED_PROVENANCE,
    )
    .expect("frozen provenance must be written");
    std::fs::write(install.join("untouched-marker"), "preserve me")
        .expect("marker must be written");

    for _ in 0..2 {
        let output = Command::new("sh")
            .arg(&copied_script)
            .env("PATH", "/usr/bin:/bin")
            .output()
            .expect("bootstrap subprocess must start");
        assert!(output.status.success());
        assert!(
            String::from_utf8(output.stdout)
                .expect("bootstrap stdout must be UTF-8")
                .contains("already bootstrapped")
        );
        assert_eq!(
            std::fs::read_to_string(install.join("untouched-marker"))
                .expect("marker must survive reuse"),
            "preserve me"
        );
    }
}

#[cfg(unix)]
#[test]
fn bootstrap_rejects_a_symlinked_deps_directory_without_touching_its_target() {
    let sandbox = TestDirectory::new("symlinked-deps");
    let fake_crate = sandbox.path().join("fake-benchmark");
    let scripts = fake_crate.join("scripts");
    let outside = sandbox.path().join("outside");
    std::fs::create_dir_all(&scripts).expect("fake scripts directory must be created");

    for name in ["leveldb-source", "leveldb-build", "leveldb-install"] {
        let target = outside.join(name);
        std::fs::create_dir_all(&target).expect("outside target must be created");
        std::fs::write(target.join("sentinel"), name).expect("sentinel must be written");
    }

    let copied_script = scripts.join("bootstrap_leveldb.sh");
    std::fs::copy(
        manifest_dir().join("scripts/bootstrap_leveldb.sh"),
        &copied_script,
    )
    .expect("bootstrap script must be copied");
    std::os::unix::fs::symlink(&outside, fake_crate.join(".deps"))
        .expect("symlinked .deps must be created");

    let output = Command::new("sh")
        .arg(&copied_script)
        .output()
        .expect("bootstrap subprocess must start");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("bootstrap stderr must be UTF-8");
    assert!(stderr.contains("must not be a symbolic link"));

    for name in ["leveldb-source", "leveldb-build", "leveldb-install"] {
        assert_eq!(
            std::fs::read_to_string(outside.join(name).join("sentinel"))
                .expect("outside sentinel must survive"),
            name
        );
    }
}

#[cfg(unix)]
#[test]
fn bootstrap_rejects_symlinked_downloads_before_registering_its_cleanup_trap() {
    let sandbox = TestDirectory::new("symlinked-downloads");
    let fake_crate = sandbox.path().join("fake-benchmark");
    let scripts = fake_crate.join("scripts");
    let deps = fake_crate.join(".deps");
    let outside_downloads = sandbox.path().join("outside-downloads");
    std::fs::create_dir_all(&scripts).expect("fake scripts directory must be created");
    std::fs::create_dir(&deps).expect("real .deps directory must be created");
    std::fs::create_dir(&outside_downloads).expect("outside downloads must be created");

    let copied_script = scripts.join("bootstrap_leveldb.sh");
    std::fs::copy(
        manifest_dir().join("scripts/bootstrap_leveldb.sh"),
        &copied_script,
    )
    .expect("bootstrap script must be copied");
    std::os::unix::fs::symlink(&outside_downloads, deps.join("downloads"))
        .expect("symlinked downloads must be created");

    let output = Command::new("sh")
        .arg("-c")
        .arg(
            r#"set -eu
trap_file="$OUTSIDE_DOWNLOADS/leveldb-99b3c03.tar.gz.tmp.$$"
printf 'must survive\n' > "$trap_file"
exec sh "$BOOTSTRAP_SCRIPT""#,
        )
        .env("OUTSIDE_DOWNLOADS", &outside_downloads)
        .env("BOOTSTRAP_SCRIPT", &copied_script)
        .output()
        .expect("bootstrap subprocess must start");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("bootstrap stderr must be UTF-8");
    assert!(stderr.contains("downloads must not be a symbolic link"));

    let outside_files: Vec<_> = std::fs::read_dir(&outside_downloads)
        .expect("outside downloads must remain readable")
        .collect::<Result<_, _>>()
        .expect("outside entries must remain readable");
    assert_eq!(outside_files.len(), 1);
    assert_eq!(
        std::fs::read_to_string(outside_files[0].path()).expect("trap sentinel must survive"),
        "must survive\n"
    );
}

#[test]
fn bootstrap_does_not_reuse_provenance_with_extra_bytes() {
    let sandbox = TestDirectory::new("provenance-extra-bytes");
    let fake_crate = sandbox.path().join("fake-benchmark");
    let scripts = fake_crate.join("scripts");
    let install = fake_crate.join(".deps/leveldb-install");
    std::fs::create_dir_all(&scripts).expect("fake scripts directory must be created");
    std::fs::create_dir_all(install.join("include/leveldb"))
        .expect("fake include directory must be created");
    std::fs::create_dir_all(install.join("lib")).expect("fake lib directory must be created");
    std::fs::create_dir_all(install.join("share/kv_bench"))
        .expect("fake provenance directory must be created");

    let copied_script = scripts.join("bootstrap_leveldb.sh");
    std::fs::copy(
        manifest_dir().join("scripts/bootstrap_leveldb.sh"),
        &copied_script,
    )
    .expect("bootstrap script must be copied");
    std::fs::write(install.join("include/leveldb/c.h"), "").expect("fake C header must be written");
    std::fs::write(
        install.join("include/leveldb/db.h"),
        "kMajorVersion = 1;\nkMinorVersion = 23;\n",
    )
    .expect("fake version header must be written");
    std::fs::write(install.join("lib/libleveldb.a"), "").expect("fake library must be written");
    std::fs::write(
        install.join("share/kv_bench/leveldb-provenance.txt"),
        format!("{EXPECTED_PROVENANCE}\n"),
    )
    .expect("fake provenance must be written");

    let output = Command::new("sh")
        .arg(&copied_script)
        .env("PATH", "/usr/bin:/bin")
        .output()
        .expect("bootstrap subprocess must start");
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("bootstrap stderr must be UTF-8");
    assert!(
        stderr.contains("required command not found: cmake"),
        "unexpected bootstrap error: {stderr}"
    );
}

#[test]
fn help_and_version_are_successful() {
    let help = run(&["--help"]);
    assert!(help.status.success());
    let help_stdout = std::str::from_utf8(&help.stdout).expect("help output must be UTF-8");
    assert!(help_stdout.contains("Usage:"));
    assert!(help_stdout.contains("kv_bench --help"));
    assert!(help_stdout.contains("not implemented in stage B0"));

    let version = run(&["--version"]);
    assert!(version.status.success());
    assert_eq!(
        std::str::from_utf8(&version.stdout)
            .expect("version output must be UTF-8")
            .trim(),
        "kv_bench 0.1.0 (LevelDB 1.23)"
    );

    let short_help = run(&["-h"]);
    assert!(short_help.status.success());
    assert_eq!(short_help.stdout, help.stdout);

    let short_version = run(&["-V"]);
    assert!(short_version.status.success());
    assert_eq!(short_version.stdout, version.stdout);
}

#[test]
fn unsupported_or_missing_commands_fail_clearly() {
    let unsupported = run(&["prepare"]);
    assert_eq!(unsupported.status.code(), Some(2));
    let unsupported_stderr =
        String::from_utf8(unsupported.stderr).expect("error output must be UTF-8");
    assert!(unsupported_stderr.contains("unsupported command in stage B0"));

    let extra = run(&["--version", "extra"]);
    assert_eq!(extra.status.code(), Some(2));

    let missing = run(&[]);
    assert_eq!(missing.status.code(), Some(2));
    let missing_stderr = String::from_utf8(missing.stderr).expect("error output must be UTF-8");
    assert!(missing_stderr.contains("a command is required"));
}

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kv_bench"))
        .args(arguments)
        .output()
        .expect("kv_bench process must start")
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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
            "kv-bench-b0-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique test directory must be created");
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
