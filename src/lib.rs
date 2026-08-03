//! RustKV is an append-only, key-value separated embedded database.
//!
//! The public API is being built incrementally. Database operations are added
//! only after their supporting components have been implemented and tested.

mod batch;
mod error;
mod options;
mod vlog;

pub use batch::WriteBatch;
pub use error::{Error, Result};
pub use options::{Compression, Options, WriteOptions};
