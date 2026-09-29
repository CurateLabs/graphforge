//! SHA-256 with byte and elapsed-time observation during explicit captures.
use sha2::digest::{FixedOutput, HashMarker, Output, OutputSizeUser, Reset, Update};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
static CAPTURES: AtomicUsize = AtomicUsize::new(0);

/// Activates process-wide SHA-256 observation without resetting other captures.
#[derive(Debug)]
pub struct HashObservation(());
impl HashObservation {
    /// Begin optional observation on all process threads.
    #[must_use]
    pub fn start() -> Self {
        CAPTURES.fetch_add(1, Ordering::Relaxed);
        Self(())
    }
}
impl Drop for HashObservation {
    fn drop(&mut self) {
        CAPTURES.fetch_sub(1, Ordering::Relaxed);
    }
}
fn active() -> bool {
    CAPTURES.load(Ordering::Relaxed) != 0
}
use std::time::Instant;

static BYTES: AtomicU64 = AtomicU64::new(0);
static NANOS: AtomicU64 = AtomicU64::new(0);

/// SHA-256, preserving digest bytes while observing work on every process thread.
#[derive(Clone, Default)]
pub struct ObservedSha256(sha2::Sha256);
impl OutputSizeUser for ObservedSha256 {
    type OutputSize = <sha2::Sha256 as OutputSizeUser>::OutputSize;
}
impl HashMarker for ObservedSha256 {}
impl Update for ObservedSha256 {
    fn update(&mut self, data: &[u8]) {
        if !active() {
            Update::update(&mut self.0, data);
            return;
        }
        let started = Instant::now();
        Update::update(&mut self.0, data);
        NANOS.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        BYTES.fetch_add(data.len() as u64, Ordering::Relaxed);
    }
}
impl FixedOutput for ObservedSha256 {
    fn finalize_into(self, out: &mut Output<Self>) {
        if !active() {
            FixedOutput::finalize_into(self.0, out);
            return;
        }
        let started = Instant::now();
        FixedOutput::finalize_into(self.0, out);
        NANOS.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}
impl Reset for ObservedSha256 {
    fn reset(&mut self) {
        Reset::reset(&mut self.0);
    }
}
/// Cumulative input bytes and update/finalize elapsed nanoseconds.
#[must_use]
pub fn totals() -> (u64, u64) {
    (BYTES.load(Ordering::Relaxed), NANOS.load(Ordering::Relaxed))
}
