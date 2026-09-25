use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

fn lanes(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn never() -> impl FnMut() -> bool {
    || false
}

#[test]
fn grants_up_to_the_request_and_never_past_the_limit() {
    let admission = Arc::new(ConstructionCpuAdmission::new(lanes(3)));
    let first = admission.acquire(lanes(2), &mut never()).unwrap();
    assert_eq!(first.lanes().get(), 2);
    // Only one lane is free: the second holder gets a partial grant.
    let second = admission.acquire(lanes(4), &mut never()).unwrap();
    assert_eq!(second.lanes().get(), 1);
    assert_eq!(admission.in_use(), 3);
    assert_eq!(admission.peak(), 3);
    drop(first);
    assert_eq!(admission.in_use(), 1);
    drop(second);
    assert_eq!(admission.in_use(), 0);
    assert_eq!(admission.peak(), 3, "peak is a high-water mark");
}

#[test]
fn a_waiting_holder_is_admitted_when_a_lease_drops() {
    let admission = Arc::new(ConstructionCpuAdmission::new(lanes(1)));
    let held = admission.acquire(lanes(1), &mut never()).unwrap();
    let waiter = {
        let admission = Arc::clone(&admission);
        std::thread::spawn(move || {
            let lease = admission.acquire(lanes(1), &mut never()).unwrap();
            lease.lanes().get()
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !waiter.is_finished(),
        "the limit must hold the second holder"
    );
    drop(held);
    assert_eq!(waiter.join().unwrap(), 1);
    assert_eq!(admission.in_use(), 0);
}

#[test]
fn cancellation_while_waiting_returns_promptly_and_leaks_nothing() {
    let admission = Arc::new(ConstructionCpuAdmission::new(lanes(1)));
    let _held = admission.acquire(lanes(1), &mut never()).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let waiter = {
        let admission = Arc::clone(&admission);
        let cancel = Arc::clone(&cancel);
        std::thread::spawn(move || {
            let mut cancelled = || cancel.load(Ordering::Acquire);
            let started = Instant::now();
            let outcome = admission.acquire(lanes(1), &mut cancelled);
            (outcome.map(|lease| lease.lanes().get()), started)
        })
    };
    std::thread::sleep(Duration::from_millis(30));
    let cancelled_at = Instant::now();
    cancel.store(true, Ordering::Release);
    let (outcome, _) = waiter.join().unwrap();
    let latency = cancelled_at.elapsed();
    let error = outcome.unwrap_err().to_string();
    assert!(error.contains("construction cancelled"), "{error}");
    assert!(
        latency < Duration::from_millis(500),
        "cancel took {latency:?} to return"
    );
    assert_eq!(admission.in_use(), 1, "only the original lease remains");
}

#[test]
fn concurrent_holders_never_exceed_the_limit() {
    let limit = 3;
    let admission = Arc::new(ConstructionCpuAdmission::new(lanes(limit)));
    let running = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..8)
        .map(|index| {
            let admission = Arc::clone(&admission);
            let running = Arc::clone(&running);
            let observed = Arc::clone(&observed);
            std::thread::spawn(move || {
                for _ in 0..50 {
                    let lease = admission
                        .acquire(lanes(1 + index % 3), &mut never())
                        .unwrap();
                    let now = running.fetch_add(lease.lanes().get(), Ordering::AcqRel)
                        + lease.lanes().get();
                    observed.fetch_max(now, Ordering::AcqRel);
                    std::thread::yield_now();
                    running.fetch_sub(lease.lanes().get(), Ordering::AcqRel);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert!(observed.load(Ordering::Acquire) <= limit);
    assert!(admission.peak() <= limit);
    assert_eq!(admission.in_use(), 0);
}
