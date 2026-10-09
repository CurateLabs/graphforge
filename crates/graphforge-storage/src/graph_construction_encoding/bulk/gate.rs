//! A pool of bytes that concurrent workers reserve before they hold them (#1938).
//!
//! The scratch passes run as many workers as the budget admits, but the bytes
//! one worker holds depend on the task it is given. Two pools bound them: one
//! for the source batches a task decodes, one for the batches the property
//! sort retains. A worker asks for what it is about to hold, waits if the pool
//! is short, and gives it back when done. A request larger than the pool is
//! trimmed to the pool, so a single task can always run alone.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use super::tables::check_cancelled;
use super::{GfError, storage};

pub(super) struct ByteGate {
    capacity: u64,
    available: Mutex<u64>,
    changed: Condvar,
    /// Most bytes ever held at once.
    peak: AtomicU64,
}

impl ByteGate {
    pub(super) fn new(capacity: u64) -> Self {
        Self {
            capacity,
            available: Mutex::new(capacity),
            changed: Condvar::new(),
            peak: AtomicU64::new(0),
        }
    }

    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// The most bytes held at once so far.
    pub(super) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Bytes held at the moment.
    #[cfg(test)]
    pub(super) fn free(&self) -> u64 {
        self.available.lock().map_or(0, |free| *free)
    }

    fn take(&self, available: &mut u64, bytes: u64) {
        *available -= bytes;
        self.peak
            .fetch_max(self.capacity - *available, Ordering::Relaxed);
    }

    /// Reserve `bytes` if they are free now.
    pub(super) fn try_acquire(&self, bytes: u64) -> Result<bool, GfError> {
        let bytes = bytes.min(self.capacity);
        let mut available = self
            .available
            .lock()
            .map_err(|_| storage("byte gate poisoned"))?;
        if *available >= bytes {
            self.take(&mut available, bytes);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Reserve `bytes`, waiting for others to give theirs back.
    pub(super) fn acquire(&self, bytes: u64, cancel: &AtomicBool) -> Result<(), GfError> {
        let bytes = bytes.min(self.capacity);
        let mut available = self
            .available
            .lock()
            .map_err(|_| storage("byte gate poisoned"))?;
        while *available < bytes {
            check_cancelled(cancel)?;
            available = self
                .changed
                .wait_timeout(available, Duration::from_millis(20))
                .map_err(|_| storage("byte gate poisoned"))?
                .0;
        }
        self.take(&mut available, bytes);
        Ok(())
    }

    /// Give back `bytes` reserved, trimmed as they were taken.
    pub(super) fn release(&self, bytes: u64) {
        if let Ok(mut available) = self.available.lock() {
            *available += bytes.min(self.capacity);
        }
        self.changed.notify_all();
    }

    /// Reserve `bytes` until the guard drops.
    pub(super) fn hold(&self, bytes: u64, cancel: &AtomicBool) -> Result<Held<'_>, GfError> {
        self.acquire(bytes, cancel)?;
        Ok(Held {
            gate: self,
            bytes,
            trim: false,
        })
    }

    /// Reserve what a decoding task holds. When the task is done and the bytes
    /// are returned, the allocator gives the pages it freed back to the
    /// system: a pool of threads that each decode and free tens of megabytes
    /// otherwise leaves them in per-thread arenas, and resident memory ends
    /// up a multiple of what the tasks hold (#1938: 3.2 GB against 1.7 GB for
    /// SNB BI SF1 with the arenas capped).
    pub(super) fn hold_task(&self, bytes: u64, cancel: &AtomicBool) -> Result<Held<'_>, GfError> {
        self.acquire(bytes, cancel)?;
        Ok(Held {
            gate: self,
            bytes,
            trim: true,
        })
    }
}

/// Bytes reserved from a [`ByteGate`], returned when dropped.
pub(super) struct Held<'g> {
    gate: &'g ByteGate,
    bytes: u64,
    trim: bool,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        if self.trim {
            return_freed_memory();
        }
        self.gate.release(self.bytes);
    }
}

/// Give the pages the allocator has freed back to the system.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(super) fn return_freed_memory() {
    // SAFETY: `malloc_trim` takes no pointers and may be called from any thread.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(super) fn return_freed_memory() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_request_larger_than_the_pool_runs_alone_and_waiters_wake_in_time() {
        let gate = Arc::new(ByteGate::new(100));
        let cancel = AtomicBool::new(false);
        let big = gate.hold(10_000, &cancel).unwrap();
        assert_eq!(gate.free(), 0);
        assert!(!gate.try_acquire(1).unwrap());
        let waiter = {
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                let cancel = AtomicBool::new(false);
                let held = gate.hold(60, &cancel).unwrap();
                drop(held);
            })
        };
        std::thread::sleep(Duration::from_millis(60));
        assert!(!waiter.is_finished());
        drop(big);
        waiter.join().unwrap();
        assert_eq!(gate.free(), 100);
        assert_eq!(gate.peak(), 100);
    }

    #[test]
    fn a_waiter_gives_up_when_the_build_is_cancelled() {
        let gate = ByteGate::new(10);
        let cancel = AtomicBool::new(false);
        let _held = gate.hold(10, &cancel).unwrap();
        cancel.store(true, Ordering::Release);
        let error = gate.acquire(5, &cancel).unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
    }
}
