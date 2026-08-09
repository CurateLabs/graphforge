use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use graphforge_core::{ApiErrorCode, GfError};

use crate::resource_policy::{ExecutionResourcePolicy, NormalizedResourcePolicy};
use crate::CancellationToken;

/// Embedded project-write coordination policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProjectWriteMode {
    /// Preserve the existing non-blocking cross-process single-writer protocol.
    #[default]
    SingleWriter,
    /// Admit same-instance writes through a bounded first-in, first-out queue.
    QueuedWriter,
    /// Stage compatible composite transactions concurrently and rebase them
    /// before commit; other mutation APIs remain serialized.
    OptimisticMultiWriter,
}

/// Validated construction options for an embedded [`crate::GraphForge`] facade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphForgeOptions {
    /// Project write policy. Defaults to [`ProjectWriteMode::SingleWriter`].
    pub write_mode: ProjectWriteMode,
    /// Maximum number of not-yet-started operations retained by queued mode.
    pub write_queue_capacity: usize,
    /// Maximum optimistic rebase attempts after the initial staging attempt.
    pub max_rebase_attempts: u32,
    /// Bounded per-instance execution resource policy (#337).
    pub resource: ExecutionResourcePolicy,
}

impl Default for GraphForgeOptions {
    fn default() -> Self {
        Self {
            write_mode: ProjectWriteMode::SingleWriter,
            write_queue_capacity: 64,
            max_rebase_attempts: 3,
            resource: ExecutionResourcePolicy::default(),
        }
    }
}

impl GraphForgeOptions {
    pub(crate) fn validate(self) -> Result<(Self, NormalizedResourcePolicy), GfError> {
        if !(1..=65_536).contains(&self.write_queue_capacity) {
            return Err(validation(
                "write_queue_capacity must be between 1 and 65536",
            ));
        }
        if self.max_rebase_attempts > 32 {
            return Err(validation("max_rebase_attempts must not exceed 32"));
        }
        let resource = self.resource.clone().normalize()?;
        Ok((self, resource))
    }
}

pub(crate) struct WriteCoordinator {
    mode: ProjectWriteMode,
    visibility: RwLock<()>,
    queued: Mutex<QueuedState>,
    changed: Condvar,
    capacity: usize,
}

#[derive(Default)]
struct QueuedState {
    next_ticket: u64,
    waiting: VecDeque<u64>,
    active: bool,
}

pub(crate) enum WritePermit<'a> {
    Direct {
        _guard: RwLockWriteGuard<'a, ()>,
    },
    Queued {
        coordinator: &'a WriteCoordinator,
        _guard: RwLockWriteGuard<'a, ()>,
    },
}

impl WriteCoordinator {
    pub(crate) fn new(options: GraphForgeOptions) -> Self {
        Self {
            mode: options.write_mode,
            visibility: RwLock::new(()),
            queued: Mutex::new(QueuedState::default()),
            changed: Condvar::new(),
            capacity: options.write_queue_capacity,
        }
    }

    pub(crate) fn acquire(
        &self,
        cancellation: Option<&CancellationToken>,
    ) -> Result<WritePermit<'_>, GfError> {
        if self.mode != ProjectWriteMode::QueuedWriter {
            return self
                .visibility
                .write()
                .map(|guard| WritePermit::Direct { _guard: guard })
                .map_err(|_| validation("write coordinator lock poisoned"));
        }
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        let mut state = self
            .queued
            .lock()
            .map_err(|_| validation("write queue lock poisoned"))?;
        if state.waiting.len() >= self.capacity {
            return Err(resource_limit("write queue capacity exceeded"));
        }
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| resource_limit("write queue ticket space exhausted"))?;
        state.waiting.push_back(ticket);
        loop {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                state.waiting.retain(|candidate| *candidate != ticket);
                self.changed.notify_all();
                return Err(cancelled());
            }
            if !state.active && state.waiting.front() == Some(&ticket) {
                state.waiting.pop_front();
                state.active = true;
                drop(state);
                let Ok(guard) = self.visibility.write() else {
                    let mut state = self
                        .queued
                        .lock()
                        .map_err(|_| validation("write queue lock poisoned"))?;
                    state.active = false;
                    self.changed.notify_all();
                    return Err(validation("write coordinator lock poisoned"));
                };
                if cancellation.is_some_and(CancellationToken::is_cancelled) {
                    drop(guard);
                    let mut state = self
                        .queued
                        .lock()
                        .map_err(|_| validation("write queue lock poisoned"))?;
                    state.active = false;
                    self.changed.notify_all();
                    return Err(cancelled());
                }
                return Ok(WritePermit::Queued {
                    coordinator: self,
                    _guard: guard,
                });
            }
            state = self
                .changed
                .wait_timeout(state, std::time::Duration::from_millis(50))
                .map(|(guard, _)| guard)
                .map_err(|_| validation("write queue lock poisoned"))?;
        }
    }

    pub(crate) fn lock(&self) -> Result<WritePermit<'_>, GfError> {
        self.acquire(None)
    }

    pub(crate) fn read(&self) -> Result<RwLockReadGuard<'_, ()>, GfError> {
        if self.mode == ProjectWriteMode::QueuedWriter {
            let mut state = self
                .queued
                .lock()
                .map_err(|_| validation("write queue lock poisoned"))?;
            while state.active || !state.waiting.is_empty() {
                state = self
                    .changed
                    .wait(state)
                    .map_err(|_| validation("write queue lock poisoned"))?;
            }
        }
        self.visibility
            .read()
            .map_err(|_| validation("write coordinator lock poisoned"))
    }

    #[cfg(test)]
    pub(crate) fn try_lock(&self) -> Result<WritePermit<'_>, ()> {
        if self.mode != ProjectWriteMode::QueuedWriter {
            return match self.visibility.try_write() {
                Ok(guard) => Ok(WritePermit::Direct { _guard: guard }),
                Err(std::sync::TryLockError::Poisoned(_) | std::sync::TryLockError::WouldBlock) => {
                    Err(())
                }
            };
        }
        let mut state = self.queued.try_lock().map_err(|_| ())?;
        if state.active || !state.waiting.is_empty() {
            return Err(());
        }
        let guard = self.visibility.try_write().map_err(|_| ())?;
        state.active = true;
        drop(state);
        Ok(WritePermit::Queued {
            coordinator: self,
            _guard: guard,
        })
    }
}

impl Drop for WritePermit<'_> {
    fn drop(&mut self) {
        if let Self::Queued { coordinator, .. } = self
            && let Ok(mut state) = coordinator.queued.lock()
        {
            state.active = false;
            coordinator.changed.notify_all();
        }
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn cancelled() -> GfError {
    GfError::Api {
        code: ApiErrorCode::Cancelled,
        message: "queued write was cancelled before execution".into(),
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;

    use super::*;

    fn queued(capacity: usize) -> Arc<WriteCoordinator> {
        Arc::new(WriteCoordinator::new(GraphForgeOptions {
            write_mode: ProjectWriteMode::QueuedWriter,
            write_queue_capacity: capacity,
            ..GraphForgeOptions::default()
        }))
    }

    #[test]
    fn queued_writes_start_in_admission_order() {
        let coordinator = queued(2);
        let active = coordinator.acquire(None).unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (order_tx, order_rx) = mpsc::channel();
        let mut workers = Vec::new();
        for id in [1, 2] {
            let worker_coordinator = Arc::clone(&coordinator);
            let ready_tx = ready_tx.clone();
            let order_tx = order_tx.clone();
            workers.push(thread::spawn(move || {
                ready_tx.send(id).unwrap();
                let _permit = worker_coordinator.acquire(None).unwrap();
                order_tx.send(id).unwrap();
            }));
            assert_eq!(ready_rx.recv().unwrap(), id);
            while coordinator.queued.lock().unwrap().waiting.len() != id {
                thread::yield_now();
            }
        }
        drop(active);
        assert_eq!(order_rx.recv().unwrap(), 1);
        assert_eq!(order_rx.recv().unwrap(), 2);
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn queue_is_bounded_and_cancellation_never_starts_work() {
        let coordinator = queued(1);
        let active = coordinator.acquire(None).unwrap();
        let cancellation = CancellationToken::new();
        let worker_coordinator = Arc::clone(&coordinator);
        let worker_token = cancellation.clone();
        let (cancelled_tx, cancelled_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            cancelled_tx
                .send(worker_coordinator.acquire(Some(&worker_token)).err())
                .unwrap();
        });
        while coordinator.queued.lock().unwrap().waiting.is_empty() {
            thread::yield_now();
        }
        let overflow = coordinator.acquire(None).err().unwrap();
        assert_eq!(overflow.code(), "GF_RESOURCE_LIMIT");
        cancellation.cancel();
        let cancelled = cancelled_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("queued cancellation must not wait for the active writer")
            .unwrap();
        drop(active);
        worker.join().unwrap();
        assert_eq!(cancelled.code(), "GF_CANCELLED");
        assert!(coordinator.try_lock().is_ok());
    }

    #[test]
    fn snapshot_reads_are_concurrent_and_do_not_consume_queue_capacity() {
        let coordinator = queued(1);
        let first = coordinator.read().unwrap();
        let second = coordinator.read().unwrap();
        assert!(coordinator.queued.lock().unwrap().waiting.is_empty());
        drop(second);
        drop(first);
        assert!(coordinator.try_lock().is_ok());
    }

    #[test]
    fn cancellation_after_admission_never_returns_a_write_permit() {
        let coordinator = queued(1);
        let reader = coordinator.read().unwrap();
        let cancellation = CancellationToken::new();
        let worker_coordinator = Arc::clone(&coordinator);
        let worker_token = cancellation.clone();
        let (result_tx, result_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            result_tx
                .send(worker_coordinator.acquire(Some(&worker_token)).err())
                .unwrap();
        });
        while !coordinator.queued.lock().unwrap().active {
            thread::yield_now();
        }

        cancellation.cancel();
        drop(reader);
        let error = result_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("admitted cancellation must finish after the reader releases")
            .expect("cancellation must not return a write permit");
        worker.join().unwrap();

        assert_eq!(error.code(), "GF_CANCELLED");
        assert!(!coordinator.queued.lock().unwrap().active);
        assert!(coordinator.try_lock().is_ok());
    }

    #[test]
    fn admitted_writer_precedes_new_snapshot_reader() {
        let coordinator = queued(1);
        let existing_reader = coordinator.read().unwrap();
        let (writer_started_tx, writer_started_rx) = mpsc::channel();
        let (release_writer_tx, release_writer_rx) = mpsc::channel();
        let writer_coordinator = Arc::clone(&coordinator);
        let writer = thread::spawn(move || {
            let _permit = writer_coordinator.acquire(None).unwrap();
            writer_started_tx.send(()).unwrap();
            release_writer_rx.recv().unwrap();
        });
        while !coordinator.queued.lock().unwrap().active {
            thread::yield_now();
        }

        let (reader_started_tx, reader_started_rx) = mpsc::channel();
        let reader_coordinator = Arc::clone(&coordinator);
        let reader = thread::spawn(move || {
            let _permit = reader_coordinator.read().unwrap();
            reader_started_tx.send(()).unwrap();
        });
        drop(existing_reader);
        writer_started_rx.recv().unwrap();
        assert!(reader_started_rx.try_recv().is_err());
        release_writer_tx.send(()).unwrap();
        writer.join().unwrap();
        reader_started_rx.recv().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn defaults_and_bounds_are_stable() {
        let defaults = GraphForgeOptions::default();
        assert_eq!(defaults.write_mode, ProjectWriteMode::SingleWriter);
        assert_eq!(defaults.resource.tokio_worker_threads, Some(2));
        assert!(defaults.clone().validate().is_ok());
        assert!(
            GraphForgeOptions {
                write_queue_capacity: 0,
                ..defaults.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            GraphForgeOptions {
                max_rebase_attempts: 33,
                ..defaults
            }
            .validate()
            .is_err()
        );
    }
}
