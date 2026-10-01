//! Opt-in synchronous observation of this thread's primitive fence attempts.
use std::cell::Cell;

thread_local! {
    static ATTEMPTS: Cell<Option<u64>> = const { Cell::new(None) };
}

struct Observation {
    parent: Option<u64>,
}
impl Observation {
    fn begin() -> Self {
        Self {
            parent: ATTEMPTS.with(|attempts| attempts.replace(Some(0))),
        }
    }
    fn count() -> u64 {
        ATTEMPTS.with(|attempts| attempts.get().expect("barrier observation is active"))
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        ATTEMPTS.with(|attempts| {
            let count = attempts.get().expect("barrier observation is active");
            attempts.set(self.parent.map(|parent| {
                parent
                    .checked_add(count)
                    .expect("barrier observation count overflows")
            }));
        });
    }
}

/// Count file-seal and retained-directory fence dispatch attempts in one
/// synchronous operation. Nested scopes contribute to their parent, including
/// when unwinding. Cache writers retain their existing exact writer evidence.
/// No counter storage is allocated and other threads are excluded.
pub fn observe_barriers<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let observation = Observation::begin();
    let result = operation();
    let count = Observation::count();
    drop(observation);
    (result, count)
}

pub(super) fn record_attempt() {
    ATTEMPTS.with(|attempts| {
        if let Some(count) = attempts.get() {
            attempts.set(Some(
                count
                    .checked_add(1)
                    .expect("barrier attempt count overflows"),
            ));
        }
    });
}
