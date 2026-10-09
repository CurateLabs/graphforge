use super::*;
use std::sync::Arc;

/// Bytes held at the moment, available to other focused component tests.
pub(crate) fn free(gate: &ByteGate) -> u64 {
    gate.available.lock().map_or(0, |free| *free)
}

#[test]
fn a_request_larger_than_the_pool_runs_alone_and_waiters_wake_in_time() {
    let gate = Arc::new(ByteGate::new(100));
    let cancel = AtomicBool::new(false);
    let big = gate.hold(10_000, &cancel).unwrap();
    assert_eq!(free(&gate), 0);
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
    assert_eq!(free(&gate), 100);
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

#[test]
fn strict_reservation_refuses_instead_of_clipping() {
    let gate = ByteGate::new(10);
    let error = gate.hold_strict(11, &AtomicBool::new(false)).err().unwrap();
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    assert_eq!(free(&gate), 10);
    assert_eq!(gate.peak(), 0);
}
