//! Minimal LevelDB C API boundary for the B0 linkage test.

use std::os::raw::c_int;

unsafe extern "C" {
    fn leveldb_major_version() -> c_int;
    fn leveldb_minor_version() -> c_int;
}

/// Returns the version reported by the linked official LevelDB C API.
pub fn linked_leveldb_version() -> (i32, i32) {
    // SAFETY: Both functions take no arguments, return plain integers, and are
    // exported by the pinned LevelDB C API. build.rs validates the matching
    // headers and static library before compiling this module.
    unsafe { (leveldb_major_version(), leveldb_minor_version()) }
}
