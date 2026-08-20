//! Iteration, cursor, and range adaptation.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::DbInner;
use crate::{Operation, ProtocolStage, StorageError};

const MAX_KEY_SIZE: usize = 60_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorState {
    Unpositioned,
    Valid,
    Exhausted,
    Failed,
}

pub struct DbIterator {
    db: Arc<DbInner>,
    state: CursorState,
    current_key: Option<Vec<u8>>,
    current_value: Option<Vec<u8>>,
    error: Option<StorageError>,
    not_sync: PhantomData<Cell<()>>,
}

impl DbIterator {
    pub fn valid(&self) -> bool {
        self.state == CursorState::Valid
            && self.current_key.is_some()
            && self.current_value.is_some()
    }

    pub fn seek_to_first(&mut self) {
        if self.state != CursorState::Failed {
            self.fail_unsupported();
        }
    }

    pub fn seek_to_last(&mut self) {
        if self.state != CursorState::Failed {
            self.fail_unsupported();
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        if self.state == CursorState::Failed {
            return;
        }
        if !target.is_empty() && target.len() > MAX_KEY_SIZE {
            self.set_failure(StorageError::invalid_iterator_target(
                self.db.instance_state(),
            ));
            return;
        }
        self.fail_unsupported();
    }

    pub fn next(&mut self) {
        if self.state == CursorState::Valid {
            self.fail_unsupported();
        }
    }

    pub fn prev(&mut self) {
        if self.state == CursorState::Valid {
            self.fail_unsupported();
        }
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

    fn fail_unsupported(&mut self) {
        self.set_failure(
            self.db
                .unsupported_error(Operation::Iterator, ProtocolStage::Read),
        );
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

    fn exhaust(&mut self) {
        if self.state == CursorState::Failed {
            return;
        }
        self.current_key = None;
        self.current_value = None;
        self.state = CursorState::Exhausted;
    }
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
    pub fn valid(&self) -> bool {
        if self.remaining == 0 || !self.inner.valid() {
            return false;
        }
        match (self.inner.key(), self.end.as_deref()) {
            (Some(key), Some(end)) => key < end,
            (Some(_), None) => true,
            (None, _) => false,
        }
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
        } else {
            self.inner.next();
        }
    }

    pub fn status(&self) -> std::result::Result<(), &StorageError> {
        self.inner.status()
    }
}
