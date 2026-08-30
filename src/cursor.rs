//! Iteration, cursor, and range adaptation.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::{DbInner, UserIndexIterator, UserIndexSnapshot};
use crate::index::IndexEntry;
#[cfg(not(test))]
use crate::runtime::ExternalLease;
use crate::{InstanceState, Operation, RetryAdvice, StorageError, StorageErrorKind};

const MAX_KEY_SIZE: usize = 60_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorState {
    Unpositioned,
    Valid,
    Exhausted,
    Failed,
}

pub struct DbIterator {
    // Field order is intentional: close public admission first, then release
    // the backend iterator/view, and keep DbInner/root-lock resources last.
    #[cfg(not(test))]
    _lease: ExternalLease,
    initial_iterator: Option<UserIndexIterator>,
    view: Option<Arc<dyn UserIndexSnapshot>>,
    operation: Operation,
    state: CursorState,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    error: Option<StorageError>,
    not_sync: PhantomData<Cell<()>>,
    db: Arc<DbInner>,
}

enum CursorTarget<'a> {
    First,
    Last,
    LowerBound(&'a [u8]),
    GreaterThan(&'a [u8]),
    LessThan(&'a [u8]),
}

impl DbIterator {
    #[cfg(not(test))]
    pub(crate) fn new(
        db: Arc<DbInner>,
        view: Arc<dyn UserIndexSnapshot>,
        initial_iterator: UserIndexIterator,
        operation: Operation,
        lease: ExternalLease,
    ) -> Self {
        Self {
            _lease: lease,
            initial_iterator: Some(initial_iterator),
            view: Some(view),
            operation,
            state: CursorState::Unpositioned,
            current_key: None,
            current_value: None,
            error: None,
            not_sync: PhantomData,
            db,
        }
    }

    #[cfg(not(test))]
    pub(crate) fn empty(db: Arc<DbInner>, operation: Operation, lease: ExternalLease) -> Self {
        Self {
            _lease: lease,
            initial_iterator: None,
            view: None,
            operation,
            state: CursorState::Exhausted,
            current_key: None,
            current_value: None,
            error: None,
            not_sync: PhantomData,
            db,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)] // Used by source-assembled integration tests.
    pub(crate) fn new_for_test(
        db: Arc<DbInner>,
        view: Arc<dyn UserIndexSnapshot>,
        initial_iterator: UserIndexIterator,
        operation: Operation,
    ) -> Self {
        Self {
            initial_iterator: Some(initial_iterator),
            view: Some(view),
            operation,
            state: CursorState::Unpositioned,
            current_key: None,
            current_value: None,
            error: None,
            not_sync: PhantomData,
            db,
        }
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test(db: Arc<DbInner>, operation: Operation) -> Self {
        Self {
            initial_iterator: None,
            view: None,
            operation,
            state: CursorState::Exhausted,
            current_key: None,
            current_value: None,
            error: None,
            not_sync: PhantomData,
            db,
        }
    }

    pub fn valid(&self) -> bool {
        self.state == CursorState::Valid
            && self.current_key.is_some()
            && self.current_value.is_some()
    }

    pub fn seek_to_first(&mut self) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::First, None);
        }
    }

    pub fn seek_to_last(&mut self) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::Last, None);
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::LowerBound(target), None);
        }
    }

    pub fn next(&mut self) {
        if self.state != CursorState::Valid {
            return;
        }
        let Some(current) = self.current_key.take() else {
            self.set_failure(cursor_invariant_error(self.operation));
            return;
        };
        self.current_value = None;
        self.relocate(CursorTarget::GreaterThan(&current), None);
    }

    pub fn prev(&mut self) {
        if self.state != CursorState::Valid {
            return;
        }
        let Some(current) = self.current_key.take() else {
            self.set_failure(cursor_invariant_error(self.operation));
            return;
        };
        self.current_value = None;
        self.relocate(CursorTarget::LessThan(&current), None);
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.valid().then(|| self.current_key.as_deref()).flatten()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.valid()
            .then(|| self.current_value.as_deref())
            .flatten()
    }

    pub fn status(&self) -> std::result::Result<(), &StorageError> {
        match self.error.as_ref() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn relocate(&mut self, target: CursorTarget<'_>, exclusive_end: Option<&[u8]>) {
        #[cfg(not(test))]
        let _operation = match self.db.begin_operation(self.operation) {
            Ok(operation) => operation,
            Err(error) => {
                self.set_failure(error);
                return;
            }
        };

        let started = match self.db.begin_read(self.operation) {
            Ok(started) => started,
            Err(error) => {
                self.set_failure(error);
                return;
            }
        };
        if matches!(target, CursorTarget::LowerBound(bytes) if !bytes.is_empty() && bytes.len() > MAX_KEY_SIZE)
        {
            self.set_failure(StorageError::read_operation_error(
                StorageErrorKind::InvalidArgument,
                self.operation,
                started.instance_state,
                RetryAdvice::FixRequestAndRetrySameInstance,
            ));
            return;
        }

        let mut iterator = match self.take_iterator() {
            Ok(Some(iterator)) => iterator,
            Ok(None) => {
                self.exhaust();
                return;
            }
            Err(error) => {
                let error = self.db.map_index_read_failure(error, self.operation);
                self.set_failure(error);
                return;
            }
        };
        let selected = match select_entry(&mut iterator, target) {
            Ok(selected) => selected,
            Err(error) => {
                let error = self.db.map_index_read_failure(error, self.operation);
                self.set_failure(error);
                return;
            }
        };
        let Some(entry) = selected else {
            match self.db.complete_read(started, self.operation) {
                Ok(()) => self.exhaust(),
                Err(error) => self.set_failure(error),
            }
            return;
        };

        // A Range end is exclusive. Decide membership from the owned index
        // key before touching the ValuePointer or Value Log so an out-of-range
        // damaged record cannot fail or poison an otherwise complete range.
        if exclusive_end.is_some_and(|end| entry.key.as_slice() >= end) {
            match self.db.complete_read(started, self.operation) {
                Ok(()) => self.exhaust(),
                Err(error) => self.set_failure(error),
            }
            return;
        }

        match self
            .db
            .materialize_index_entry(started, self.operation, entry)
        {
            Ok((key, value)) => {
                self.current_key = Some(key);
                self.current_value = Some(value);
                self.error = None;
                self.state = CursorState::Valid;
            }
            Err(error) => self.set_failure(error),
        }
    }

    fn take_iterator(&mut self) -> crate::Result<Option<UserIndexIterator>> {
        if let Some(iterator) = self.initial_iterator.take() {
            return Ok(Some(iterator));
        }
        self.view.as_ref().map(|view| view.iter_user()).transpose()
    }

    pub(crate) fn seek_to_first_before(&mut self, exclusive_end: Option<&[u8]>) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::First, exclusive_end);
        }
    }

    pub(crate) fn seek_before(&mut self, target: &[u8], exclusive_end: Option<&[u8]>) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::LowerBound(target), exclusive_end);
        }
    }

    fn next_before(&mut self, exclusive_end: Option<&[u8]>) {
        if self.state != CursorState::Valid {
            return;
        }
        let Some(current) = self.current_key.take() else {
            self.set_failure(cursor_invariant_error(self.operation));
            return;
        };
        self.current_value = None;
        self.relocate(CursorTarget::GreaterThan(&current), exclusive_end);
    }

    fn set_failure(&mut self, error: StorageError) {
        if self.state == CursorState::Failed {
            return;
        }
        self.current_key = None;
        self.current_value = None;
        self.error = Some(error);
        self.state = CursorState::Failed;
    }

    pub(crate) fn exhaust(&mut self) {
        if self.state == CursorState::Failed {
            return;
        }
        self.current_key = None;
        self.current_value = None;
        self.error = None;
        self.state = CursorState::Exhausted;
    }
}

fn select_entry(
    iterator: &mut UserIndexIterator,
    target: CursorTarget<'_>,
) -> crate::Result<Option<IndexEntry>> {
    match target {
        CursorTarget::First => iterator.next().transpose(),
        CursorTarget::Last => iterator.next_back().transpose(),
        CursorTarget::LowerBound(target) => {
            for entry in iterator {
                let entry = entry?;
                if entry.key.as_slice() >= target {
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        }
        CursorTarget::GreaterThan(current) => {
            for entry in iterator {
                let entry = entry?;
                if entry.key.as_slice() > current {
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        }
        CursorTarget::LessThan(current) => {
            while let Some(entry) = iterator.next_back() {
                let entry = entry?;
                if entry.key.as_slice() < current {
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        }
    }
}

fn cursor_invariant_error(operation: Operation) -> StorageError {
    StorageError::read_operation_error(
        StorageErrorKind::StoragePoisoned,
        operation,
        InstanceState::Poisoned,
        RetryAdvice::ReopenAndVerify,
    )
}

pub struct KeyRange<'a> {
    pub start: Option<&'a [u8]>,
    pub end: Option<&'a [u8]>,
}

pub struct RangeCursor {
    inner: DbIterator,
    end: Option<Vec<u8>>,
    remaining: usize,
}

impl RangeCursor {
    #[cfg(not(test))]
    pub(crate) fn new(inner: DbIterator, end: Option<Vec<u8>>, remaining: usize) -> Self {
        let mut cursor = Self {
            inner,
            end,
            remaining,
        };
        cursor.enforce_boundary();
        cursor
    }

    #[cfg(test)]
    #[allow(dead_code)] // Used by source-assembled integration tests.
    pub(crate) fn new_for_test(inner: DbIterator, end: Option<Vec<u8>>, remaining: usize) -> Self {
        let mut cursor = Self {
            inner,
            end,
            remaining,
        };
        cursor.enforce_boundary();
        cursor
    }

    pub fn valid(&self) -> bool {
        self.remaining > 0 && self.inner.valid()
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.valid().then(|| self.inner.key()).flatten()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.valid().then(|| self.inner.value()).flatten()
    }

    pub fn next(&mut self) {
        if !self.valid() {
            return;
        }
        self.remaining -= 1;
        if self.remaining == 0 {
            self.inner.exhaust();
            return;
        }
        self.inner.next_before(self.end.as_deref());
        self.enforce_boundary();
    }

    pub fn status(&self) -> std::result::Result<(), &StorageError> {
        self.inner.status()
    }

    fn enforce_boundary(&mut self) {
        if self.remaining == 0 {
            self.inner.exhaust();
            return;
        }
        if matches!(
            (self.inner.key(), self.end.as_deref()),
            (Some(key), Some(end)) if key >= end
        ) {
            self.inner.exhaust();
        }
    }
}
