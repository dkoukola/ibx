//! Ownership of the synchronous API's engine registration reply.

use super::InstrumentId;
use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

const PENDING: u8 = 0;
const ACCEPTED: u8 = 1;
const ABANDONED: u8 = 2;
type Reply = Result<InstrumentId, String>;

/// A registration result is published before engine acceptance. A caller
/// racing its deadline can therefore either abandon the request or acquire
/// its already available ID, without leaving an unowned subscription behind.
///
/// Direct low-level users can convert an ordinary sender using `From`. That
/// path retains the sender's original blocking/rendezvous semantics and has
/// no API timeout handshake; the synchronous client uses `channel` instead.
#[derive(Debug, Clone)]
pub struct RegistrationReply {
    sender: Sender<Reply>,
    admission: Option<Arc<AtomicU8>>,
}

/// Caller half of a timeout-aware engine registration.
pub struct RegistrationReceiver {
    receiver: Receiver<Reply>,
    admission: Arc<AtomicU8>,
}

impl RegistrationReply {
    pub fn channel() -> (Self, RegistrationReceiver) {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let admission = Arc::new(AtomicU8::new(PENDING));
        (
            Self {
                sender,
                admission: Some(admission.clone()),
            },
            RegistrationReceiver {
                receiver,
                admission,
            },
        )
    }

    pub fn send(&self, value: Reply) -> Result<(), SendError<Reply>> {
        let Some(admission) = &self.admission else {
            return self.sender.send(value);
        };
        // One result per registration, in a private bounded(1) channel. The
        // hot loop never waits on its caller; acceptance proves a queued ID.
        self.sender
            .try_send(value.clone())
            .map_err(|error| SendError(error.into_inner()))?;
        if admission
            .compare_exchange(PENDING, ACCEPTED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(SendError(value));
        }
        Ok(())
    }
}

impl From<Sender<Reply>> for RegistrationReply {
    fn from(sender: Sender<Reply>) -> Self {
        Self {
            sender,
            admission: None,
        }
    }
}

impl RegistrationReceiver {
    pub fn recv_timeout(self, timeout: Duration) -> Result<Reply, RecvTimeoutError> {
        match self.receiver.recv_timeout(timeout) {
            Err(RecvTimeoutError::Timeout) => self.abandon_or_receive(),
            result => result,
        }
    }

    fn abandon_or_receive(&self) -> Result<Reply, RecvTimeoutError> {
        if self
            .admission
            .compare_exchange(PENDING, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Err(RecvTimeoutError::Timeout)
        } else {
            // Accepted means send has already queued the result. This is
            // deliberately try_recv, not another wait past the deadline.
            self.receiver
                .try_recv()
                .map_err(|_| RecvTimeoutError::Disconnected)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_timeout_owns_late_engine_reply() {
        let (reply, receiver) = RegistrationReply::channel();
        // Keep the receiver alive to cover the narrow ACK/timeout race, not
        // merely a disconnected sender after the caller has returned.
        assert_eq!(
            receiver.abandon_or_receive(),
            Err(RecvTimeoutError::Timeout)
        );
        assert!(reply.send(Ok(7)).is_err());
        assert_eq!(receiver.admission.load(Ordering::Acquire), ABANDONED);
    }

    #[test]
    fn registration_accepted_result_is_available_without_waiting() {
        let (reply, receiver) = RegistrationReply::channel();
        reply.send(Ok(7)).unwrap();
        assert_eq!(receiver.abandon_or_receive(), Ok(Ok(7)));
    }

    #[test]
    fn registration_timeout_can_win_after_publication_before_acceptance() {
        let (reply, receiver) = RegistrationReply::channel();
        reply.sender.try_send(Ok(7)).unwrap();
        assert_eq!(
            receiver.abandon_or_receive(),
            Err(RecvTimeoutError::Timeout)
        );
        assert_eq!(
            reply.admission.unwrap().compare_exchange(
                PENDING,
                ACCEPTED,
                Ordering::AcqRel,
                Ordering::Acquire
            ),
            Err(ABANDONED)
        );
    }

    #[test]
    fn registration_normal_receive_may_precede_engine_acceptance() {
        let (reply, receiver) = RegistrationReply::channel();
        reply.sender.try_send(Ok(7)).unwrap();
        assert_eq!(receiver.recv_timeout(Duration::ZERO), Ok(Ok(7)));
        assert_eq!(
            reply.admission.unwrap().compare_exchange(
                PENDING,
                ACCEPTED,
                Ordering::AcqRel,
                Ordering::Acquire
            ),
            Ok(PENDING)
        );
    }

    #[test]
    fn registration_timeout_races_have_one_consistent_owner() {
        for _ in 0..256 {
            let (reply, receiver) = RegistrationReply::channel();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let sender = std::thread::spawn({
                let barrier = barrier.clone();
                move || {
                    barrier.wait();
                    reply.send(Ok(7)).is_ok()
                }
            });
            barrier.wait();
            let received = receiver.recv_timeout(Duration::ZERO);
            assert_eq!(sender.join().unwrap(), received.is_ok(), "{received:?}");
            assert!(matches!(
                received,
                Ok(Ok(7)) | Err(RecvTimeoutError::Timeout)
            ));
        }
    }

    #[test]
    fn registration_direct_sender_keeps_rendezvous_behavior() {
        let (sender, receiver) = crossbeam_channel::bounded(0);
        let reply = RegistrationReply::from(sender);
        let thread = std::thread::spawn(move || reply.send(Ok(7)).unwrap());
        assert_eq!(receiver.recv().unwrap(), Ok(7));
        thread.join().unwrap();
    }
}
