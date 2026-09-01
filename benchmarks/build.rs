use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const LEVELDB_MAJOR: u32 = 1;
const LEVELDB_MINOR: u32 = 23;
const LEVELDB_PROVENANCE: &str = concat!(
    "version=1.23\n",
    "commit=99b3c03b3284f5886f9ef9a4ef703d57373e61be\n",
    "archive_sha256=bc87b9bbc5674c91246a89813355e78401759761342cc049e1c3d56350a8a9d1\n",
    "build_type=Release\n",
    "shared_libraries=off\n",
    "crc32c_external=off\n",
    "snappy=off\n",
    "tcmalloc=off\n",
);

fn main() {
    let crate_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set by Cargo"),
    );
    let prefix = crate_dir.join(".deps/leveldb-install");
    let c_header = prefix.join("include/leveldb/c.h");
    let db_header = prefix.join("include/leveldb/db.h");
    let library = prefix.join("lib/libleveldb.a");
    let provenance = prefix.join("share/kv_bench/leveldb-provenance.txt");

    require_file(&c_header, "LevelDB C API header");
    require_file(&db_header, "LevelDB version header");
    require_file(&library, "LevelDB static library");
    require_file(&provenance, "LevelDB build provenance");
    validate_header_version(&db_header);
    validate_provenance(&provenance);

    let native_header = crate_dir.join("native/leveldb_aggregate.h");
    let native_source = crate_dir.join("native/leveldb_aggregate.c");
    require_file(&native_header, "benchmark LevelDB aggregate header");
    require_file(&native_source, "benchmark LevelDB aggregate source");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    let aggregate_library = compile_aggregate(&prefix, &native_source, &out_dir);

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", native_header.display());
    println!("cargo:rerun-if-changed={}", native_source.display());
    println!("cargo:rerun-if-changed={}", c_header.display());
    println!("cargo:rerun-if-changed={}", db_header.display());
    println!("cargo:rerun-if-changed={}", library.display());
    println!("cargo:rerun-if-changed={}", provenance.display());
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=AR");
    println!(
        "cargo:rustc-link-search=native={}",
        aggregate_library
            .parent()
            .expect("aggregate library must have a parent")
            .display()
    );
    println!("cargo:rustc-link-lib=static=kv_bench_leveldb_aggregate");
    println!(
        "cargo:rustc-link-search=native={}",
        prefix.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=leveldb");

    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") => println!("cargo:rustc-link-lib=dylib=c++"),
        Ok("linux") => println!("cargo:rustc-link-lib=dylib=stdc++"),
        Ok(target) => panic!("kv_bench does not define LevelDB C++ linkage for target OS {target}"),
        Err(error) => panic!("CARGO_CFG_TARGET_OS is unavailable: {error}"),
    }
}

fn compile_aggregate(prefix: &Path, source: &Path, out_dir: &Path) -> PathBuf {
    let object = out_dir.join("leveldb_aggregate.o");
    let library = out_dir.join("libkv_bench_leveldb_aggregate.a");
    let compiler = env::var_os("CC").unwrap_or_else(|| "cc".into());
    let archiver = env::var_os("AR").unwrap_or_else(|| "ar".into());

    let mut compile = Command::new(compiler);
    compile
        .arg("-std=c11")
        .arg("-O3")
        .arg("-DNDEBUG")
        .arg("-I")
        .arg(prefix.join("include"))
        .arg("-I")
        .arg(
            source
                .parent()
                .expect("aggregate source must have a parent"),
        )
        .arg("-c")
        .arg(source)
        .arg("-o")
        .arg(&object);
    run(&mut compile, "compile benchmark LevelDB aggregate");

    let mut archive = Command::new(archiver);
    archive.arg("crus").arg(&library).arg(&object);
    run(&mut archive, "archive benchmark LevelDB aggregate");
    library
}

fn run(command: &mut Command, description: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("failed to {description}: {error}"));
    assert!(status.success(), "failed to {description}: {status}");
}

fn require_file(path: &Path, description: &str) {
    if !path.is_file() {
        panic!(
            "missing {description} at {}; run ./scripts/bootstrap_leveldb.sh first",
            path.display()
        );
    }
}

fn validate_header_version(path: &Path) {
    let header = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    let expected_major = format!("kMajorVersion = {LEVELDB_MAJOR};");
    let expected_minor = format!("kMinorVersion = {LEVELDB_MINOR};");
    if !header.contains(&expected_major) || !header.contains(&expected_minor) {
        panic!(
            "LevelDB headers at {} are not version {LEVELDB_MAJOR}.{LEVELDB_MINOR}",
            path.display()
        );
    }
}

fn validate_provenance(path: &Path) {
    let provenance = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    if provenance != LEVELDB_PROVENANCE {
        panic!(
            "LevelDB provenance at {} does not exactly match the frozen B0 build",
            path.display()
        );
    }
}
