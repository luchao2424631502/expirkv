//! Consistent database snapshots.

use std::sync::Arc;

use crate::db::{DbInner, UserIndexSnapshot};
#[cfg(not(test))]
use crate::runtime::ExternalLease;

pub(crate) struct SnapshotInner {
    // Field order is intentional: close public admission first, release the
    // owned backend view next, and keep DbInner/root-lock resources alive last.
    #[cfg(not(test))]
    _lease: ExternalLease,
    #[cfg_attr(test, allow(dead_code))] // Used by source-assembled integration tests.
    view: Arc<dyn UserIndexSnapshot>,
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

impl Snapshot {
    #[cfg(not(test))]
    pub(crate) fn new(
        db: Arc<DbInner>,
        view: Arc<dyn UserIndexSnapshot>,
        lease: ExternalLease,
    ) -> Self {
        Self {
            inner: Arc::new(SnapshotInner {
                _lease: lease,
                view,
                _db: db,
            }),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)] // Used by source-assembled integration tests.
    pub(crate) fn new_for_test(db: Arc<DbInner>, view: Arc<dyn UserIndexSnapshot>) -> Self {
        Self {
            inner: Arc::new(SnapshotInner { view, _db: db }),
        }
    }

    pub(crate) fn belongs_to(&self, db: &Arc<DbInner>) -> bool {
        Arc::ptr_eq(&self.inner._db, db)
    }

    pub(crate) fn view(&self) -> Arc<dyn UserIndexSnapshot> {
        Arc::clone(&self.inner.view)
    }
}
