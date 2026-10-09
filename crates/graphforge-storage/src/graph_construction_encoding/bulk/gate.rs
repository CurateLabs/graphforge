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
        Ok(Held { gate: self, bytes })
    }

    /// Reserve a complete job without clipping its request to the pool.
    pub(super) fn hold_strict(&self, bytes: u64, cancel: &AtomicBool) -> Result<Held<'_>, GfError> {
        check_cancelled(cancel)?;
        if bytes > self.capacity {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                message: format!(
                    "graph construction merge needs {bytes} bytes; its shared pool holds {}",
                    self.capacity
                ),
            });
        }
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
        Ok(Held { gate: self, bytes })
    }
}

/// Bytes reserved from a [`ByteGate`], returned when dropped.
pub(super) struct Held<'g> {
    gate: &'g ByteGate,
    bytes: u64,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.gate.release(self.bytes);
    }
}

#[cfg(test)]
#[path = "gate_tests.rs"]
pub(crate) mod tests;
