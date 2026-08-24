//! Operation guards, shutdown, and drop coordination.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleSnapshot {
    pub(crate) accepting_operations: bool,
    pub(crate) external_leases: usize,
    pub(crate) operation_guards: usize,
}

struct LifecycleInner {
    accepting_operations: bool,
    external_leases: usize,
    operation_guards: usize,
}

pub(crate) struct LifecycleController {
    inner: Mutex<LifecycleInner>,
    quiesced: Condvar,
}

impl LifecycleController {
    pub(crate) fn new_with_external_lease() -> (Arc<Self>, ExternalLease) {
        let controller = Arc::new(Self {
            inner: Mutex::new(LifecycleInner {
                accepting_operations: true,
                external_leases: 1,
                operation_guards: 0,
            }),
            quiesced: Condvar::new(),
        });
        let lease = ExternalLease {
            controller: Arc::clone(&controller),
        };
        (controller, lease)
    }

    // 写闸门
    pub(crate) fn acquire_operation(self: &Arc<Self>) -> Option<OperationGuard> {
        let mut inner = lock_inner(&self.inner);
        if !inner.accepting_operations {
            return None;
        }
        inner.operation_guards += 1;
        Some(OperationGuard {
            controller: Arc::clone(self),
        })
    }

    pub(crate) fn snapshot(&self) -> LifecycleSnapshot {
        let inner = lock_inner(&self.inner);
        LifecycleSnapshot {
            accepting_operations: inner.accepting_operations,
            external_leases: inner.external_leases,
            operation_guards: inner.operation_guards,
        }
    }

    pub(crate) fn wait_for_quiescence(&self, timeout: Duration) -> bool {
        let started_at = Instant::now();
        let mut inner = lock_inner(&self.inner);
        loop {
            if is_quiescent(&inner) {
                return true;
            }
            let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
                return false;
            };
            if remaining.is_zero() {
                return false;
            }
            let (next_inner, wait_result) = match self.quiesced.wait_timeout(inner, remaining) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            inner = next_inner;
            if wait_result.timed_out() && !is_quiescent(&inner) {
                return false;
            }
        }
    }

    fn clone_external_lease(self: &Arc<Self>) -> ExternalLease {
        let mut inner = lock_inner(&self.inner);
        debug_assert!(inner.external_leases > 0);
        inner.external_leases += 1;
        ExternalLease {
            controller: Arc::clone(self),
        }
    }

    fn release_external_lease(&self) {
        let notify = {
            let mut inner = lock_inner(&self.inner);
            debug_assert!(inner.external_leases > 0);
            inner.external_leases -= 1;
            if inner.external_leases == 0 {
                inner.accepting_operations = false;
            }
            is_quiescent(&inner)
        };
        if notify {
            self.quiesced.notify_all();
        }
    }

    fn release_operation_guard(&self) {
        let notify = {
            let mut inner = lock_inner(&self.inner);
            debug_assert!(inner.operation_guards > 0);
            inner.operation_guards -= 1;
            is_quiescent(&inner)
        };
        if notify {
            self.quiesced.notify_all();
        }
    }
}

pub(crate) struct ExternalLease {
    controller: Arc<LifecycleController>,
}

impl Clone for ExternalLease {
    fn clone(&self) -> Self {
        self.controller.clone_external_lease()
    }
}

impl Drop for ExternalLease {
    fn drop(&mut self) {
        self.controller.release_external_lease();
    }
}

pub(crate) struct OperationGuard {
    controller: Arc<LifecycleController>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.controller.release_operation_guard();
    }
}

fn lock_inner(inner: &Mutex<LifecycleInner>) -> std::sync::MutexGuard<'_, LifecycleInner> {
    match inner.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn is_quiescent(inner: &LifecycleInner) -> bool {
    inner.external_leases == 0 && inner.operation_guards == 0
}
