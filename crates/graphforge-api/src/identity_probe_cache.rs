//! The identity probe a facade caches for one topology generation.
//!
//! A probe opens and answers by mapping its fragments with rayon, and an
//! import reaches it from the jobs of the compute pool. A worker that joins
//! nested parallel work runs whatever it can find while it waits, and that
//! includes a sibling job of the same import. If the worker holds a lock that
//! the sibling takes, it waits for itself: the import hung that way (#1881).
//! So the cache lock is only ever held to read or swap the cached handle,
//! never while a probe opens or answers.

use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};

use graphforge_core::GfError;
use graphforge_storage::TopologyIdentityProbe;

use crate::workspace_hydration::GraphWorkspace;

/// A probe and the workspace whose files it reads, so a handle that outlives
/// its cache entry (a lookup in flight when the entry is replaced) still reads
/// files that exist.
pub(crate) struct IdentityProbe {
    probe: TopologyIdentityProbe,
    _workspace: GraphWorkspace,
}

impl IdentityProbe {
    pub(crate) fn new(probe: TopologyIdentityProbe, workspace: GraphWorkspace) -> Self {
        Self {
            probe,
            _workspace: workspace,
        }
    }
}

impl Deref for IdentityProbe {
    type Target = TopologyIdentityProbe;

    fn deref(&self) -> &TopologyIdentityProbe {
        &self.probe
    }
}

#[derive(Default)]
pub(crate) struct CachedProbe {
    pub(crate) current: Option<Arc<IdentityProbe>>,
    /// Counts invalidations, so an open that raced one does not undo it.
    epoch: u64,
}

/// The probe cached on a facade.
#[derive(Default)]
pub(crate) struct IdentityProbeCache {
    pub(crate) state: Mutex<CachedProbe>,
}

impl IdentityProbeCache {
    fn lock(&self) -> Result<MutexGuard<'_, CachedProbe>, GfError> {
        self.state
            .lock()
            .map_err(|_| GfError::Storage("identity probe lock poisoned".into()))
    }

    /// The cached probe for the generation `select` finds, else the probe
    /// `open` returns for it.
    ///
    /// `select` picks the workspace and reads its topology generation,
    /// returning the generation and whatever `open` needs. A probe is only
    /// cached if no invalidation happened since before `select` ran. That is
    /// exact because the invalidation count is read under the cache lock
    /// before the workspace is chosen, and every site that replaces or wipes a
    /// workspace invalidates after the new state is in place. So whatever
    /// `select` saw is either newer than every invalidation counted before it,
    /// or the count has moved and nothing is cached. Sampling the count after
    /// `select` would miss an invalidation in between: a probe of the replaced
    /// workspace could then be cached under a generation number that the new
    /// workspace later reuses (a cleared project counts again from one), and
    /// serve the wrong nodes. The generation number alone cannot tell the
    /// two workspaces apart; the count can.
    ///
    /// `select` and `open` run without the cache lock: two callers may both
    /// open one, and the last to finish is cached. The lock is not held by the
    /// returned handle either, so callers answer from it without any lock.
    pub(crate) fn get_or_open<T>(
        &self,
        select: impl FnOnce() -> Result<(u64, T), GfError>,
        open: impl FnOnce(u64, T) -> Result<Arc<IdentityProbe>, GfError>,
    ) -> Result<Arc<IdentityProbe>, GfError> {
        let epoch = self.lock()?.epoch;
        let (generation, selected) = select()?;
        {
            let state = self.lock()?;
            if let Some(probe) = &state.current
                && probe.topology_generation() == generation
            {
                return Ok(Arc::clone(probe));
            }
        }
        let opened = open(generation, selected)?;
        let mut state = self.lock()?;
        if state.epoch == epoch {
            state.current = Some(Arc::clone(&opened));
        }
        Ok(opened)
    }

    /// Drop the cached probe, and with it any open still in flight.
    pub(crate) fn invalidate(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.current = None;
        state.epoch += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};

    use arrow::array::FixedSizeBinaryArray;
    use graphforge_core::uuid::Uuid;
    use graphforge_exec::ComputePool;
    use graphforge_storage::UuidIndexKind;

    use super::*;
    use crate::GraphForge;

    const DEADLINE: Duration = Duration::from_secs(30);

    /// Spin until `flag` is set, failing the test at the deadline.
    #[track_caller]
    fn wait_for(flag: &AtomicBool, what: &str) {
        let deadline = Instant::now() + DEADLINE;
        while !flag.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::yield_now();
        }
    }

    fn graph_with_a_node() -> (GraphForge, Uuid) {
        let graph = GraphForge::new(None).unwrap();
        graph.execute("CREATE (:Person {name: 'Alice'})").unwrap();
        let batches = graphforge_storage::read_nodes(&graph.dir()).unwrap();
        let uuid = Uuid::from_slice(
            batches[0]
                .column_by_name("node_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
        )
        .unwrap();
        (graph, uuid)
    }

    /// An import normalizes its batches on the compute pool and opens the probe
    /// from each. A worker that opens it joins nested parallel work, and while
    /// it waits it runs a sibling batch it can steal, which opens the probe
    /// too. If the first holds the cache lock across its open, the sibling
    /// waits for a lock its own worker holds (#1881).
    ///
    /// The schedule is forced with flags, not time. Two workers: one is held
    /// until the open's nested work is queued, so it steals that work, not the
    /// queued sibling; that work does not finish until the sibling has started,
    /// which only the opener's own worker can arrange.
    #[test]
    fn an_open_in_a_pool_job_never_holds_the_lock_for_a_sibling_job() {
        let (graph, _) = graph_with_a_node();
        let handle = graph.cached_identity_probe().unwrap();
        let generation = handle.topology_generation();
        let cache = Arc::new(IdentityProbeCache::default());
        let pool = Arc::new(ComputePool::new(2).unwrap());
        let flag = || Arc::new(AtomicBool::new(false));
        let (second_worker_busy, release_second_worker) = (flag(), flag());
        let (open_in_flight, nested_work_stolen, sibling_started) = (flag(), flag(), flag());
        let (opened, opened_rx) = channel();
        let (sibling_opened, sibling_opened_rx) = channel();

        // Hold one worker until the opener says to release it.
        let held = std::thread::spawn({
            let (pool, busy, release) = (
                pool.clone(),
                second_worker_busy.clone(),
                release_second_worker.clone(),
            );
            move || {
                pool.install(|| {
                    busy.store(true, Ordering::Release);
                    wait_for(&release, "the held worker was never released");
                });
            }
        });
        wait_for(&second_worker_busy, "the pool never ran its first job");

        // The opener runs on the other worker.
        let open_job = std::thread::spawn({
            let (cache, pool, handle) = (cache.clone(), pool.clone(), handle.clone());
            let (open_in_flight, release) = (open_in_flight.clone(), release_second_worker.clone());
            let (stolen, sibling) = (nested_work_stolen.clone(), sibling_started.clone());
            move || {
                pool.clone().install(|| {
                    open_in_flight.store(true, Ordering::Release);
                    let result = cache.get_or_open(
                        || Ok((generation, ())),
                        |_, ()| {
                            // The open joins two pieces of nested work. This
                            // worker runs the first, which frees the held worker
                            // and waits for it to steal the second. The second
                            // waits for the sibling to start. A sibling job queued
                            // on the pool can then only run on this worker, inside
                            // its wait for the second.
                            pool.map_ordered(&[0, 1], |piece| {
                                if *piece == 0 {
                                    release.store(true, Ordering::Release);
                                    wait_for(&stolen, "the nested work was never stolen");
                                } else {
                                    stolen.store(true, Ordering::Release);
                                    wait_for(&sibling, "the sibling never started");
                                }
                            });
                            Ok(handle)
                        },
                    );
                    let _ = opened.send(result.map(|_| ()));
                });
            }
        });
        wait_for(&open_in_flight, "the opener never started");

        // The sibling is queued behind both workers.
        let sibling = std::thread::spawn({
            let (cache, pool, handle) = (cache.clone(), pool.clone(), handle.clone());
            let started = sibling_started.clone();
            move || {
                pool.install(|| {
                    started.store(true, Ordering::Release);
                    let result = cache.get_or_open(|| Ok((generation, ())), |_, ()| Ok(handle));
                    let _ = sibling_opened.send(result.map(|_| ()));
                });
            }
        });

        // With the lock held across the open, the opener's worker has run the
        // sibling inside its wait and blocked on its own lock: neither answers.
        sibling_opened_rx
            .recv_timeout(DEADLINE)
            .expect("the sibling job never finished: the opener held the lock across its open")
            .unwrap();
        opened_rx
            .recv_timeout(DEADLINE)
            .expect("the opener never finished")
            .unwrap();
        for thread in [held, open_job, sibling] {
            thread.join().unwrap();
        }
        assert!(cache.state.lock().unwrap().current.is_some());
    }

    /// An open that an invalidation overtakes must not put its handle back.
    #[test]
    fn an_open_overtaken_by_an_invalidation_is_not_cached() {
        let (graph, _) = graph_with_a_node();
        let handle = graph.cached_identity_probe().unwrap();
        let generation = handle.topology_generation();
        let cache = Arc::new(IdentityProbeCache::default());
        let (inside_open, inside_open_rx) = channel();
        let (resume, resume_rx) = channel::<()>();
        let open_job = std::thread::spawn({
            let cache = cache.clone();
            let handle = handle.clone();
            move || {
                cache
                    .get_or_open(
                        || Ok((generation, ())),
                        |_, ()| {
                            inside_open.send(()).unwrap();
                            resume_rx.recv_timeout(DEADLINE).unwrap();
                            Ok(handle)
                        },
                    )
                    .unwrap()
            }
        });
        inside_open_rx.recv_timeout(DEADLINE).unwrap();
        // The lock is free during the open, so this does not wait for it.
        cache.invalidate();
        resume.send(()).unwrap();
        let used = open_job.join().unwrap();
        assert_eq!(used.topology_generation(), generation);
        assert!(cache.state.lock().unwrap().current.is_none());
        // A later caller opens, and caches, its own.
        cache
            .get_or_open(|| Ok((generation, ())), |_, ()| Ok(handle))
            .unwrap();
        assert!(cache.state.lock().unwrap().current.is_some());
    }

    /// An invalidation between a caller picking its workspace and the cache
    /// noting the invalidation count must keep that caller's probe out of the
    /// cache. The generation number cannot tell the workspaces apart: a cleared
    /// project counts again from the same value (#1881).
    #[test]
    fn a_workspace_chosen_before_an_invalidation_is_not_cached() {
        let (graph, _) = graph_with_a_node();
        let replaced = graph.cached_identity_probe().unwrap();
        let (other, _) = graph_with_a_node();
        let current = other.cached_identity_probe().unwrap();
        // Two workspaces, one generation number.
        let generation = replaced.topology_generation();
        assert_eq!(generation, current.topology_generation());
        let cache = Arc::new(IdentityProbeCache::default());
        let (chosen, chosen_rx) = channel();
        let (resume, resume_rx) = channel::<()>();
        let caller = std::thread::spawn({
            let (cache, replaced) = (cache.clone(), replaced.clone());
            move || {
                cache
                    .get_or_open(
                        || {
                            // The caller has its workspace and its generation.
                            chosen.send(()).unwrap();
                            resume_rx.recv_timeout(DEADLINE).unwrap();
                            Ok((generation, ()))
                        },
                        |_, ()| Ok(replaced),
                    )
                    .unwrap()
            }
        });
        chosen_rx.recv_timeout(DEADLINE).unwrap();
        // The workspace is replaced and the cache invalidated.
        cache.invalidate();
        resume.send(()).unwrap();
        let used = caller.join().unwrap();
        assert!(Arc::ptr_eq(&used, &replaced));
        assert!(
            cache.state.lock().unwrap().current.is_none(),
            "a probe of the replaced workspace was cached"
        );
        // The next caller reads the new workspace, not the replaced one.
        let next = cache
            .get_or_open(|| Ok((generation, ())), |_, ()| Ok(current.clone()))
            .unwrap();
        assert!(Arc::ptr_eq(&next, &current));
    }

    /// A caller answers from the handle while another caller asks for it. The
    /// lookup parks inside a fragment read, the way a slow read or a nested
    /// join would, and nobody holds the cache lock meanwhile.
    #[cfg(unix)]
    #[test]
    fn a_lookup_in_flight_never_holds_the_lock_for_another_caller() {
        let (graph, uuid) = graph_with_a_node();
        let graph = Arc::new(graph);
        let handle = graph.cached_identity_probe().unwrap();
        // The fragment becomes a FIFO, so reading it blocks until a writer opens.
        let fragment = graph
            .dir()
            .topology_files()
            .unwrap()
            .node_fragments()
            .first()
            .unwrap()
            .0
            .clone();
        std::fs::rename(&fragment, fragment.with_extension("moved")).unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fragment)
                .status()
                .unwrap()
                .success()
        );
        drop(handle);

        let (holding, holding_rx) = channel();
        let (lookup_done, lookup_done_rx) = channel();
        let lookup = std::thread::spawn({
            let graph = graph.clone();
            move || {
                let probe = graph.cached_identity_probe().unwrap();
                holding.send(()).unwrap();
                let _ = lookup_done.send(probe.probe(UuidIndexKind::Node, &[uuid]).map(|_| ()));
            }
        });
        holding_rx.recv_timeout(DEADLINE).unwrap();

        // Another caller asks for the probe while the lookup is parked.
        let (asked, asked_rx) = channel();
        let other = std::thread::spawn({
            let graph = graph.clone();
            move || {
                let _ = asked.send(graph.cached_identity_probe().map(|_| ()));
            }
        });
        asked_rx
            .recv_timeout(DEADLINE)
            .expect("another caller waited on a lock held by a lookup in flight")
            .unwrap();

        // Release the lookup: a writer on the FIFO lets its read go on, and the
        // probe refuses the replaced fragment.
        let (writer_tx, writer_rx) = channel();
        let writer = std::thread::spawn(move || {
            let _ = writer_tx.send(std::fs::OpenOptions::new().write(true).open(&fragment));
        });
        let _writer = writer_rx
            .recv_timeout(DEADLINE)
            .expect("no reader on the FIFO")
            .unwrap();
        lookup_done_rx
            .recv_timeout(DEADLINE)
            .expect("the lookup never finished")
            .unwrap_err();
        for thread in [lookup, other, writer] {
            thread.join().unwrap();
        }
    }
}
