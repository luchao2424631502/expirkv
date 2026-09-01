# kv_bench

`kv_bench` is the RustKV repository's benchmark-only sub-crate. Stage B0 contains only the build and linkage baseline: the RustKV path dependency, pinned LevelDB 1.23, a minimal CLI, and smoke tests. Workloads and performance reporting are added by later reviewed stages.

## Prerequisites on macOS

- Rust 1.90 or newer;
- Apple Command Line Tools;
- CMake;
- `curl` and `tar`.

## Bootstrap and verify

Run all commands from this directory:

```bash
./scripts/bootstrap_leveldb.sh
cargo build --locked
cargo test --locked
cargo build --release --locked
cargo run --locked -- --version
```

The bootstrap script downloads only the official Google LevelDB source archive for commit `99b3c03b3284f5886f9ef9a4ef703d57373e61be`, verifies its pinned SHA-256, and installs a Release static library under `.deps/leveldb-install`. Compression-library detection is disabled. `.deps` is generated state and is not committed.

`kv_bench --help` and `kv_bench --version` are the only successful CLI paths in B0. Benchmark commands deliberately return a non-zero unsupported-command result until their owning stages implement them.
