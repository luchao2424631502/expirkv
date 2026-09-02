//! Iteration, cursor, and range adaptation.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::{DbInner, UserIndexIterator, UserIndexSnapshot};
use crate::index::{IndexEntry, UserKeyRange};
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
    iterator: Option<UserIndexIterator>,
    view: Option<Arc<dyn UserIndexSnapshot>>,
    operation: Operation,
    state: CursorState,
    direction: Option<CursorDirection>,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorDirection {
    Forward,
    Reverse,
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
            iterator: Some(initial_iterator),
            view: Some(view),
            operation,
            state: CursorState::Unpositioned,
            direction: None,
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
            iterator: None,
            view: None,
            operation,
            state: CursorState::Exhausted,
            direction: None,
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
            iterator: Some(initial_iterator),
            view: Some(view),
            operation,
            state: CursorState::Unpositioned,
            direction: None,
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
            iterator: None,
            view: None,
            operation,
            state: CursorState::Exhausted,
            direction: None,
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
            self.relocate(CursorTarget::First);
        }
    }

    pub fn seek_to_last(&mut self) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::Last);
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::LowerBound(target));
        }
    }

    pub fn next(&mut self) {
        if self.state != CursorState::Valid {
            return;
        }
        self.advance(CursorDirection::Forward);
    }

    pub fn prev(&mut self) {
        if self.state != CursorState::Valid {
            return;
        }
        self.advance(CursorDirection::Reverse);
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

    fn relocate(&mut self, target: CursorTarget<'_>) {
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

        let (range, direction, may_use_initial) = match target {
            CursorTarget::First => (UserKeyRange::all(), CursorDirection::Forward, true),
            CursorTarget::Last => (UserKeyRange::all(), CursorDirection::Reverse, true),
            CursorTarget::LowerBound(target) => {
                let start = match clone_cursor_bound(target, self.operation, started.instance_state)
                {
                    Ok(start) => start,
                    Err(error) => {
                        self.set_failure(error);
                        return;
                    }
                };
                (
                    UserKeyRange::from_inclusive(start),
                    CursorDirection::Forward,
                    false,
                )
            }
        };

        let initial =
            may_use_initial && self.state == CursorState::Unpositioned && self.direction.is_none();
        let iterator = if initial { self.iterator.take() } else { None };
        let iterator = match iterator {
            Some(iterator) => Some(iterator),
            None => match self.create_iterator(range) {
                Ok(iterator) => iterator,
                Err(error) => {
                    self.set_failure(error);
                    return;
                }
            },
        };
        let Some(iterator) = iterator else {
            self.exhaust();
            return;
        };
        self.iterator = Some(iterator);
        self.direction = Some(direction);
        self.current_key = None;
        self.current_value = None;
        self.consume_adjacent(started, direction);
    }

    fn advance(&mut self, direction: CursorDirection) {
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
        let Some(current) = self.current_key.take() else {
            self.set_failure(cursor_invariant_error(self.operation));
            return;
        };
        self.current_value = None;

        if self.direction != Some(direction) {
            let range = match direction {
                CursorDirection::Forward => UserKeyRange::after(current),
                CursorDirection::Reverse => UserKeyRange::before(current),
            };
            let iterator = match self.create_iterator(range) {
                Ok(Some(iterator)) => iterator,
                Ok(None) => {
                    self.exhaust();
                    return;
                }
                Err(error) => {
                    self.set_failure(error);
                    return;
                }
            };
            self.iterator = Some(iterator);
            self.direction = Some(direction);
        }

        self.consume_adjacent(started, direction);
    }

    fn create_iterator(&self, range: UserKeyRange) -> crate::Result<Option<UserIndexIterator>> {
        self.view
            .as_ref()
            .map(|view| view.iter_user_range(range))
            .transpose()
            .map_err(|error| self.db.map_index_read_failure(error, self.operation))
    }

    fn consume_adjacent(
        &mut self,
        started: crate::db::ReadStateSnapshot,
        direction: CursorDirection,
    ) {
        let selected = match self.iterator.as_mut() {
            Some(iterator) => select_entry(iterator, direction),
            None => Err(cursor_invariant_error(self.operation)),
        };
        let selected = match selected {
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

    pub(crate) fn seek_to_first_in_initial_range(&mut self) {
        if self.state != CursorState::Failed {
            self.relocate(CursorTarget::First);
        }
    }

    fn next_in_range(&mut self) {
        if self.state != CursorState::Valid {
            return;
        }
        self.advance(CursorDirection::Forward);
    }

    fn set_failure(&mut self, error: StorageError) {
        if self.state == CursorState::Failed {
            return;
        }
        self.current_key = None;
        self.current_value = None;
        self.iterator = None;
        self.direction = None;
        self.error = Some(error);
        self.state = CursorState::Failed;
    }

    pub(crate) fn exhaust(&mut self) {
        if self.state == CursorState::Failed {
            return;
        }
        self.current_key = None;
        self.current_value = None;
        self.iterator = None;
        self.direction = None;
        self.error = None;
        self.state = CursorState::Exhausted;
    }
}

fn select_entry(
    iterator: &mut UserIndexIterator,
    direction: CursorDirection,
) -> crate::Result<Option<IndexEntry>> {
    match direction {
        CursorDirection::Forward => iterator.next().transpose(),
        CursorDirection::Reverse => iterator.next_back().transpose(),
    }
}

fn clone_cursor_bound(
    bound: &[u8],
    operation: Operation,
    state: InstanceState,
) -> crate::Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(bound.len()).map_err(|_| {
        StorageError::read_operation_error(
            StorageErrorKind::ResourceExhausted,
            operation,
            state,
            RetryAdvice::RetrySameInstance,
        )
    })?;
    owned.extend_from_slice(bound);
    Ok(owned)
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
        self.inner.next_in_range();
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
