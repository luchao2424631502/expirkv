//! Shared entry point for the repository-local RustKV/LevelDB benchmark.
//!
//! Stage B0 intentionally exposes only build metadata. Backend operations,
//! workloads, traces, and statistics belong to later reviewed stages.

mod backend;

pub use backend::linked_leveldb_version;

/// The pinned LevelDB version used by every benchmark build.
pub const EXPECTED_LEVELDB_VERSION: (i32, i32) = (1, 23);

/// Stable help output for the B0 command-line skeleton.
pub fn help_text() -> &'static str {
    concat!(
        "kv_bench ",
        env!("CARGO_PKG_VERSION"),
        "\n",
        "RustKV/LevelDB benchmark driver\n\n",
        "Usage:\n",
        "  kv_bench --help\n",
        "  kv_bench --version\n\n",
        "Benchmark commands are not implemented in stage B0.\n",
    )
}

/// Version text includes the dynamically queried LevelDB C API version so
/// `--version` also proves that the final binary is linked to the pinned lib.
pub fn version_text() -> String {
    let (major, minor) = linked_leveldb_version();
    format!(
        "kv_bench {} (LevelDB {major}.{minor})",
        env!("CARGO_PKG_VERSION")
    )
}
