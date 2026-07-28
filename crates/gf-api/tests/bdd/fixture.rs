use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use gf_api::{GfError, GraphForge};

/// Stable concurrency for the timed TCK profile.
///
/// `GraphForge` owns a two-thread Tokio runtime. Cucumber's default of 64
/// scenarios therefore creates 128 executor threads and severe temp-storage
/// contention on a four-core CI runner. Even bounded parallelism makes the
/// other active scenarios inherit a genuine outlier's CPU and I/O cost. A
/// single reusable fixture is both faster for this corpus and makes elapsed
/// time attributable to the scenario being reported.
pub const TCK_CONCURRENCY: usize = 1;

/// Reusable, scenario-isolated in-memory GraphForge fixtures.
pub struct FixturePool {
    available: Mutex<Vec<GraphForge>>,
    created: AtomicUsize,
}

impl Default for FixturePool {
    fn default() -> Self {
        Self {
            available: Mutex::new(Vec::new()),
            created: AtomicUsize::new(0),
        }
    }
}

impl FixturePool {
    /// Lease a clean fixture, preserving its parser/executor/runtime
    /// infrastructure while resetting all scenario-owned state.
    pub fn acquire(&self) -> Result<GraphForge, GfError> {
        let fixture = self.available.lock().expect("TCK fixture pool lock").pop();
        match fixture {
            Some(fixture) => {
                fixture.clear()?;
                Ok(fixture)
            }
            None => {
                self.created.fetch_add(1, Ordering::Relaxed);
                GraphForge::new(None)
            }
        }
    }

    /// Return a fixture after a scenario. It is reset on the next lease so a
    /// failed scenario cannot bypass cleanup and contaminate another scenario.
    pub fn release(&self, fixture: GraphForge) {
        self.available
            .lock()
            .expect("TCK fixture pool lock")
            .push(fixture);
    }

    pub fn created_count(&self) -> usize {
        self.created.load(Ordering::Relaxed)
    }

    fn drain(&self) {
        self.available
            .lock()
            .expect("TCK fixture pool lock")
            .clear();
    }

    fn reset_metrics(&self) {
        self.created.store(0, Ordering::Relaxed);
    }
}

static TCK_POOL: OnceLock<FixturePool> = OnceLock::new();
static TCK_POOL_ACTIVE: AtomicBool = AtomicBool::new(false);

fn pool() -> &'static FixturePool {
    TCK_POOL.get_or_init(FixturePool::default)
}

/// Enable pooling for the TCK run only. The API BDD suite runs first and keeps
/// its existing fresh-instance behavior.
pub fn activate() -> FixtureRunGuard {
    TCK_POOL_ACTIVE
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .expect("only one TCK fixture run may be active");
    pool().drain();
    pool().reset_metrics();
    FixtureRunGuard
}

pub fn created_count() -> usize {
    pool().created_count()
}

pub struct FixtureRunGuard;

impl Drop for FixtureRunGuard {
    fn drop(&mut self) {
        TCK_POOL_ACTIVE.store(false, Ordering::SeqCst);
        pool().drain();
    }
}

/// Replace a world's current fixture with an isolated empty instance.
pub fn replace_with_fresh(slot: &mut Option<GraphForge>) {
    if TCK_POOL_ACTIVE.load(Ordering::SeqCst) {
        if let Some(previous) = slot.take() {
            pool().release(previous);
        }
        *slot = Some(pool().acquire().expect("acquire clean TCK fixture"));
    } else {
        *slot = Some(GraphForge::new(None).expect("in-memory forge must succeed"));
    }
}

/// Return the scenario fixture from cucumber's after hook.
pub fn release(slot: &mut Option<GraphForge>) {
    if TCK_POOL_ACTIVE.load(Ordering::SeqCst)
        && let Some(fixture) = slot.take()
    {
        pool().release(fixture);
    }
}
