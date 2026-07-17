//! Renders `ProgressEvent`s from the index pipeline as a single stderr line.
//! One render thread owns the bar and consumes the channel; producers never
//! touch the terminal.

use indicatif::{ProgressBar, ProgressStyle};
use seagrep_core::ProgressEvent;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

const SPINNER_FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const BAR_CELLS: u64 = 14;
const TICK: Duration = Duration::from_millis(100);

#[derive(Default)]
pub(crate) struct ProgressState {
    listed: u64,
    listing_complete: bool,
    diff: Option<(u64, u64)>,
    ingested: u64,
    decoded_bytes: u64,
    upload_total: u64,
    upload_done: u64,
    tick: u64,
}

impl ProgressState {
    pub(crate) fn apply(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::Listed { objects } => self.listed = objects,
            ProgressEvent::ListingComplete { objects } => {
                self.listed = objects;
                self.listing_complete = true;
            }
            ProgressEvent::DiffComputed { to_add, to_remove } => {
                self.diff = Some((to_add, to_remove));
            }
            ProgressEvent::SourceIngested { decoded_bytes } => {
                self.ingested += 1;
                self.decoded_bytes += decoded_bytes;
            }
            ProgressEvent::UploadStarted { bytes } => self.upload_total += bytes,
            ProgressEvent::UploadedChunk { bytes } => self.upload_done += bytes,
        }
    }

    pub(crate) fn render_line(&self, target: &str) -> String {
        let spinner = SPINNER_FRAMES[(self.tick as usize) % SPINNER_FRAMES.len()];
        match self.diff {
            None => format!(
                "{spinner} {} {target} · {} objects",
                if self.listing_complete {
                    "diffing"
                } else {
                    "listing"
                },
                thousands(self.listed)
            ),
            Some((to_add, _)) if self.ingested < to_add => format!(
                "indexing {} {}/{} · {} MiB",
                bar(self.ingested, to_add),
                thousands(self.ingested),
                thousands(to_add),
                mib(self.decoded_bytes)
            ),
            Some(_) if self.upload_done < self.upload_total => format!(
                "uploading {} {}/{} MiB",
                bar(self.upload_done, self.upload_total),
                mib(self.upload_done),
                mib(self.upload_total)
            ),
            Some(_) => format!("{spinner} finalizing {target}"),
        }
    }
}

pub(crate) struct IndexProgressBar {
    thread: std::thread::JoinHandle<()>,
}

impl IndexProgressBar {
    pub(crate) fn spawn(receiver: Receiver<ProgressEvent>, target: String) -> IndexProgressBar {
        let thread = std::thread::spawn(move || {
            let bar = ProgressBar::new_spinner();
            bar.set_style(ProgressStyle::with_template("{msg}").expect("static template"));
            let mut state = ProgressState::default();
            loop {
                match receiver.recv_timeout(TICK) {
                    Ok(event) => {
                        state.apply(event);
                        while let Ok(event) = receiver.try_recv() {
                            state.apply(event);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
                state.tick += 1;
                bar.set_message(state.render_line(&target));
                bar.tick();
            }
            bar.finish_and_clear();
        });
        IndexProgressBar { thread }
    }

    pub(crate) fn finish(self) {
        let _ = self.thread.join();
    }
}

fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn mib(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / (1024.0 * 1024.0))
}

fn bar(done: u64, total: u64) -> String {
    let filled = (done.min(total) * BAR_CELLS)
        .checked_div(total)
        .unwrap_or(0) as usize;
    let mut out = String::new();
    for cell in 0..BAR_CELLS as usize {
        out.push(if cell < filled { '▰' } else { '▱' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folded(events: &[ProgressEvent]) -> ProgressState {
        let mut state = ProgressState::default();
        for event in events {
            state.apply(*event);
        }
        state
    }

    #[test]
    fn renders_listing_phase_with_spinner_and_count() {
        let state = folded(&[ProgressEvent::Listed { objects: 1204 }]);
        assert_eq!(
            state.render_line("s3://b/logs"),
            "⠋ listing s3://b/logs · 1,204 objects"
        );
    }

    #[test]
    fn renders_diffing_phase_with_filtered_count_after_listing_completes() {
        let state = folded(&[
            ProgressEvent::Listed { objects: 1221 },
            ProgressEvent::ListingComplete { objects: 1204 },
        ]);
        assert_eq!(
            state.render_line("s3://b/logs"),
            "⠋ diffing s3://b/logs · 1,204 objects"
        );
    }

    #[test]
    fn renders_indexing_phase_with_bar_counts_and_decoded_mib() {
        let state = folded(&[
            ProgressEvent::Listed { objects: 4 },
            ProgressEvent::DiffComputed {
                to_add: 4,
                to_remove: 0,
            },
            ProgressEvent::SourceIngested {
                decoded_bytes: 3 * 1024 * 1024,
            },
            ProgressEvent::SourceIngested {
                decoded_bytes: 2 * 1024 * 1024,
            },
        ]);
        assert_eq!(
            state.render_line("s3://b/logs"),
            "indexing ▰▰▰▰▰▰▰▱▱▱▱▱▱▱ 2/4 · 5.0 MiB"
        );
    }

    #[test]
    fn renders_uploading_phase_once_ingest_completes() {
        let state = folded(&[
            ProgressEvent::DiffComputed {
                to_add: 1,
                to_remove: 0,
            },
            ProgressEvent::SourceIngested { decoded_bytes: 10 },
            ProgressEvent::UploadStarted {
                bytes: 4 * 1024 * 1024,
            },
            ProgressEvent::UploadedChunk {
                bytes: 3 * 1024 * 1024,
            },
        ]);
        assert_eq!(
            state.render_line("s3://b/logs"),
            "uploading ▰▰▰▰▰▰▰▰▰▰▱▱▱▱ 3.0/4.0 MiB"
        );
    }

    #[test]
    fn renders_finalizing_after_uploads_complete() {
        let state = folded(&[
            ProgressEvent::DiffComputed {
                to_add: 1,
                to_remove: 0,
            },
            ProgressEvent::SourceIngested { decoded_bytes: 10 },
            ProgressEvent::UploadStarted { bytes: 8 },
            ProgressEvent::UploadedChunk { bytes: 8 },
        ]);
        assert_eq!(state.render_line("s3://b/logs"), "⠋ finalizing s3://b/logs");
    }

    #[test]
    fn spinner_advances_with_ticks() {
        let mut state = folded(&[ProgressEvent::Listed { objects: 1 }]);
        state.tick = 1;
        assert!(state.render_line("t").starts_with('⠙'));
    }

    #[test]
    fn thousands_groups_digits() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(24908), "24,908");
        assert_eq!(thousands(1_000_000), "1,000,000");
    }
}
