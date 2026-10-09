//! The workspace registered-source readers share (#1918).
//!
//! A source reader holds a task's decoded pages, dictionaries and the decoded
//! batch it hands the builder. Those bytes are neither `BulkSource::decoded_bytes`
//! nor normalized scratch records, so the builder gives the readers one pool
//! and every task reserves what it will hold *before* it reads. A task that
//! asks for more than the pool can ever grant is refused with a typed resource
//! limit before the offending allocation; a task that asks for more than is
//! free waits for a running task to finish.
//!
//! The pool is a plan-time quantity derived from the memory budget. Nothing
//! retries or falls back: a reservation either fits or is refused.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use graphforge_core::{GfError, ProjectErrorCode};

/// How often a waiting reservation re-checks cancellation.
const POLL: Duration = Duration::from_millis(20);

/// The pool of workspace bytes shared by every source reader of one build.
pub struct SourceWorkspace {
    capacity: u64,
    reserved: Mutex<u64>,
    freed: Condvar,
    peak: AtomicU64,
    reservations: AtomicU64,
}

/// Bytes held from a [`SourceWorkspace`] until dropped.
pub struct SourceReservation {
    workspace: Arc<SourceWorkspace>,
    bytes: u64,
}

impl SourceWorkspace {
    /// A pool granting at most `capacity` bytes at once.
    #[must_use]
    pub fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            reserved: Mutex::new(0),
            freed: Condvar::new(),
            peak: AtomicU64::new(0),
            reservations: AtomicU64::new(0),
        })
    }

    /// Bytes the pool can grant at once.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// The most bytes held at once so far.
    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Reservations granted so far.
    #[must_use]
    pub fn reservations(&self) -> u64 {
        self.reservations.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` for `what`, waiting for running tasks to release enough.
    ///
    /// # Errors
    /// A typed resource limit when `bytes` exceeds the pool's capacity, which
    /// no amount of waiting can satisfy, or the error `cancelled` returns.
    pub fn reserve(
        self: &Arc<Self>,
        bytes: u64,
        what: &str,
        cancelled: &dyn Fn() -> Result<(), GfError>,
    ) -> Result<SourceReservation, GfError> {
        if bytes > self.capacity {
            return Err(GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                message: format!(
                    "graph construction encoding: {what} needs {bytes} bytes of source workspace \
                     before it reads, and the memory budget grants {}",
                    self.capacity
                ),
            });
        }
        let mut reserved = self
            .reserved
            .lock()
            .expect("source workspace lock poisoned");
        while *reserved + bytes > self.capacity {
            cancelled()?;
            reserved = self
                .freed
                .wait_timeout(reserved, POLL)
                .expect("source workspace lock poisoned")
                .0;
        }
        *reserved += bytes;
        self.peak.fetch_max(*reserved, Ordering::Relaxed);
        self.reservations.fetch_add(1, Ordering::Relaxed);
        drop(reserved);
        Ok(SourceReservation {
            workspace: Arc::clone(self),
            bytes,
        })
    }
}

impl SourceReservation {
    /// Bytes held.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for SourceReservation {
    fn drop(&mut self) {
        let mut reserved = self
            .workspace
            .reserved
            .lock()
            .expect("source workspace lock poisoned");
        *reserved -= self.bytes;
        drop(reserved);
        self.workspace.freed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    fn never() -> Result<(), GfError> {
        Ok(())
    }

    #[test]
    fn a_reservation_larger_than_the_pool_is_refused_before_it_waits() {
        let pool = SourceWorkspace::new(100);
        let held = pool.reserve(60, "first", &never).unwrap();
        // A reservation that waited instead of being refused would wait forever;
        // the deadline turns that into a failure the assertion below names.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let error = pool
            .reserve(101, "second", &|| {
                if std::time::Instant::now() > deadline {
                    Err(GfError::Storage(
                        "waited for bytes the pool never had".into(),
                    ))
                } else {
                    Ok(())
                }
            })
            .err()
            .unwrap();
        assert!(matches!(
            error,
            GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                ..
            }
        ));
        assert!(error.to_string().contains("101 bytes"), "{error}");
        drop(held);
        // Exactly the capacity is granted.
        pool.reserve(100, "third", &never).unwrap();
        assert_eq!((pool.peak_bytes(), pool.reservations()), (100, 2));
    }

    #[test]
    fn a_reservation_waits_for_a_running_task_and_never_overcommits() {
        let pool = SourceWorkspace::new(100);
        let first = pool.reserve(70, "first", &never).unwrap();
        let granted = Arc::new(AtomicBool::new(false));
        let waiter = {
            let (pool, granted) = (Arc::clone(&pool), Arc::clone(&granted));
            std::thread::spawn(move || {
                let second = pool.reserve(60, "second", &never).unwrap();
                granted.store(true, Ordering::SeqCst);
                drop(second);
            })
        };
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            !granted.load(Ordering::SeqCst),
            "130 bytes were granted from a pool of 100"
        );
        drop(first);
        waiter.join().unwrap();
        assert!(granted.load(Ordering::SeqCst));
        assert_eq!(pool.peak_bytes(), 70);
    }

    #[test]
    fn a_waiting_reservation_gives_up_when_the_build_is_cancelled() {
        let pool = SourceWorkspace::new(10);
        let _held = pool.reserve(10, "held", &never).unwrap();
        let cancelled = AtomicBool::new(false);
        let error = std::thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                pool.reserve(5, "waiting", &|| {
                    if cancelled.load(Ordering::SeqCst) {
                        Err(GfError::Storage("cancelled".into()))
                    } else {
                        Ok(())
                    }
                })
                .err()
            });
            std::thread::sleep(Duration::from_millis(100));
            cancelled.store(true, Ordering::SeqCst);
            waiter.join().unwrap()
        });
        assert_eq!(error.unwrap().to_string(), "storage error: cancelled");
        // The abandoned request held nothing.
        assert_eq!(pool.reservations(), 1);
    }
}
