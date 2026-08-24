//! Runtime state, write admission, and operation lifecycle control.
#![allow(dead_code, unused_imports)] // Stage 6 boundary; Db wiring is added in later stages.

mod lifecycle;
mod state;
mod write_gate;

pub(crate) use lifecycle::{ExternalLease, LifecycleController, OperationGuard};
pub(crate) use state::{RuntimeControl, StateSnapshot, StateTransition};
pub(crate) use write_gate::{RequestId, WriteTicket};
