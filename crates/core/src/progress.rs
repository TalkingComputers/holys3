//! Progress events for long-running pipeline work. Producers emit typed
//! events through an unbounded channel; a vanished consumer must never block
//! or fail the pipeline.

use std::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressEvent {
    Listed { objects: u64 },
    ListingComplete { objects: u64 },
    DiffComputed { to_add: u64, to_remove: u64 },
    SourceIngested { decoded_bytes: u64 },
    UploadStarted { bytes: u64 },
    UploadedChunk { bytes: u64 },
}

#[derive(Debug, Clone)]
pub struct ProgressSender(Sender<ProgressEvent>);

impl ProgressSender {
    pub fn channel() -> (Self, Receiver<ProgressEvent>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        (Self(sender), receiver)
    }

    pub fn emit(&self, event: ProgressEvent) {
        let _ = self.0.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emitted_events_arrive_in_order() {
        let (sender, receiver) = ProgressSender::channel();
        sender.emit(ProgressEvent::Listed { objects: 3 });
        sender.emit(ProgressEvent::DiffComputed {
            to_add: 2,
            to_remove: 1,
        });
        drop(sender);
        let events: Vec<_> = receiver.iter().collect();
        assert_eq!(
            events,
            [
                ProgressEvent::Listed { objects: 3 },
                ProgressEvent::DiffComputed {
                    to_add: 2,
                    to_remove: 1
                },
            ]
        );
    }

    #[test]
    fn emit_after_receiver_drop_is_a_noop() {
        let (sender, receiver) = ProgressSender::channel();
        drop(receiver);
        sender.emit(ProgressEvent::Listed { objects: 1 });
    }
}
