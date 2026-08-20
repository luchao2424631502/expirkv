//! Consistent database snapshots.

use std::sync::Arc;

use crate::db::DbInner;

pub(crate) struct SnapshotInner {
    _db: Arc<DbInner>,
}

pub struct Snapshot {
    inner: Arc<SnapshotInner>,
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
