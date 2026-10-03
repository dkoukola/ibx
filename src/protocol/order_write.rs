//! Owned, single-use admission at an order batch's first socket write.

use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Transport evidence only; `Written` is not broker acceptance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderWriteOutcome {
    /// No socket write was attempted for this command.
    NotSent,
    /// The complete batch was accepted by the socket implementation.
    Written,
    /// A write was attempted, including TLS `WouldBlock`; do not resend.
    OutcomeUnknown,
}

type Authorize = Box<dyn FnOnce() -> Result<(), String> + Send>;

struct State {
    authorize: Option<Authorize>,
    prepared: bool,
    attempted: bool,
    outcome: Option<OrderWriteOutcome>,
}

struct Completion {
    state: Mutex<State>,
    changed: Condvar,
}

struct GuardOwner(Arc<Completion>);

impl Drop for GuardOwner {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.outcome.is_none() {
            state.outcome = Some(if state.attempted {
                OrderWriteOutcome::OutcomeUnknown
            } else {
                OrderWriteOutcome::NotSent
            });
            self.0.changed.notify_all();
        }
    }
}

/// Clones share one authorization and one write claim, never another send.
#[derive(Clone)]
pub struct OrderWriteGuard(Arc<GuardOwner>);

impl std::fmt::Debug for OrderWriteGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderWriteGuard").finish_non_exhaustive()
    }
}

/// Dropping or cancelling a pending receipt prevents a later first write.
/// Cancellation serializes with authorization and the first write call.
pub struct OrderWriteReceipt(Arc<Completion>);

impl OrderWriteReceipt {
    pub fn outcome(&self) -> Option<OrderWriteOutcome> {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outcome
            .clone()
    }

    pub fn wait_timeout(&self, timeout: Duration) -> Option<OrderWriteOutcome> {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = self
            .0
            .changed
            .wait_timeout_while(state, timeout, |state| state.outcome.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.outcome.clone()
    }

    pub fn cancel(&self) -> OrderWriteOutcome {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = state.outcome.clone().unwrap_or(if state.attempted {
            OrderWriteOutcome::OutcomeUnknown
        } else {
            OrderWriteOutcome::NotSent
        });
        state.outcome = Some(outcome.clone());
        state.authorize = None;
        self.0.changed.notify_all();
        outcome
    }
}

impl Drop for OrderWriteReceipt {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl OrderWriteGuard {
    pub fn new(
        authorize: impl FnOnce() -> Result<(), String> + Send + 'static,
    ) -> (Self, OrderWriteReceipt) {
        let completion = Arc::new(Completion {
            state: Mutex::new(State {
                authorize: Some(Box::new(authorize)),
                prepared: false,
                attempted: false,
                outcome: None,
            }),
            changed: Condvar::new(),
        });
        (
            Self(Arc::new(GuardOwner(completion.clone()))),
            OrderWriteReceipt(completion),
        )
    }

    pub(crate) fn claim(&self) -> bool {
        let mut state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.prepared || state.outcome.is_some() {
            return false;
        }
        state.prepared = true;
        true
    }

    pub(crate) fn pending_preparation(&self) -> bool {
        let state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.prepared && state.outcome.is_none()
    }

    pub(crate) fn refuse_unprepared(&self) {
        let mut state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Another clone may have claimed the command since this preflight
        // began. Only that owner can settle an admitted command.
        if !state.prepared && state.outcome.is_none() {
            state.outcome = Some(OrderWriteOutcome::NotSent);
            self.0.0.changed.notify_all();
        }
    }

    pub(crate) fn write(&self, write: impl FnOnce() -> io::Result<usize>) -> io::Result<usize> {
        let mut state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.outcome.is_some() {
            return Err(io::Error::other("guarded order no longer admitted"));
        }
        if !state.attempted {
            let authorize = state
                .authorize
                .take()
                .expect("single-use write authorization");
            if authorize().is_err() {
                state.outcome = Some(OrderWriteOutcome::NotSent);
                self.0.0.changed.notify_all();
                return Err(io::Error::other("guarded order authorization refused"));
            }
            // TLS may transmit even when the call returns WouldBlock.
            state.attempted = true;
        }
        write()
    }

    pub(crate) fn written(&self) {
        let mut state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.outcome.is_none() {
            state.outcome = Some(OrderWriteOutcome::Written);
            self.0.0.changed.notify_all();
        }
    }

    pub(crate) fn failed(&self) {
        let mut state = self
            .0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.outcome.is_none() {
            state.outcome = Some(if state.attempted {
                OrderWriteOutcome::OutcomeUnknown
            } else {
                OrderWriteOutcome::NotSent
            });
            self.0.0.changed.notify_all();
        }
    }

    pub(crate) fn outcome(&self) -> Option<OrderWriteOutcome> {
        self.0
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .outcome
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn guarded_write_clones_share_claim_and_authorization() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (guard, receipt) = OrderWriteGuard::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let duplicate = guard.clone();
        assert!(guard.claim());
        assert!(!duplicate.claim());
        assert!(
            guard
                .write(|| Err(io::ErrorKind::WouldBlock.into()))
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        duplicate.refuse_unprepared();
        assert_eq!(
            receipt.outcome(),
            None,
            "another clone's preflight cannot fail the owner"
        );
        assert_eq!(guard.write(|| Ok(2)).unwrap(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        guard.written();
        assert_eq!(receipt.outcome(), Some(OrderWriteOutcome::Written));
    }

    #[test]
    fn guarded_write_cancel_drop_and_tls_would_block_are_distinct() {
        let (guard, receipt) = OrderWriteGuard::new(|| panic!("cancelled callback"));
        assert_eq!(receipt.cancel(), OrderWriteOutcome::NotSent);
        assert!(guard.write(|| panic!("cancelled write")).is_err());

        let (guard, receipt) = OrderWriteGuard::new(|| panic!("dropped receipt callback"));
        drop(receipt);
        assert!(!guard.claim());

        let (guard, receipt) = OrderWriteGuard::new(|| Ok(()));
        assert!(guard.claim());
        assert!(
            guard
                .write(|| Err(io::ErrorKind::WouldBlock.into()))
                .is_err()
        );
        assert_eq!(receipt.cancel(), OrderWriteOutcome::OutcomeUnknown);
        assert!(
            guard
                .write(|| panic!("no cancelled remainder replay"))
                .is_err()
        );

        let (guard, receipt) = OrderWriteGuard::new(|| Ok(()));
        drop(guard);
        assert_eq!(receipt.outcome(), Some(OrderWriteOutcome::NotSent));
    }

    #[test]
    fn guarded_write_cancellation_joins_an_entered_authorization() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writes = Arc::new(AtomicUsize::new(0));
        let (guard, receipt) = OrderWriteGuard::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        });
        let observed = writes.clone();
        let writer = std::thread::spawn(move || {
            guard.claim();
            guard
                .write(|| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Err(io::ErrorKind::WouldBlock.into())
                })
                .unwrap_err();
            guard
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
        let cancel = std::thread::spawn(move || cancel_tx.send(receipt.cancel()).unwrap());
        assert!(cancel_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).unwrap();
        assert_eq!(
            cancel_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            OrderWriteOutcome::OutcomeUnknown
        );
        cancel.join().unwrap();
        let guard = writer.join().unwrap();
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert!(
            guard
                .write(|| panic!("no write after cancel returns"))
                .is_err()
        );
    }
}
