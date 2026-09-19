//! Quiet-host calibration: enable schedstats and pin this process to one CPU.
//! `taskset -c CPU region_controls` runs CPU, sleep, and runnable-delay controls.
use std::sync::Barrier;
use std::time::{Duration, Instant};

use graphforge_storage::concurrency_attribution::{RegionCapture, RegionScope};

fn burn(duration: Duration) {
    let until = Instant::now() + duration;
    let mut value = 7_u64;
    while Instant::now() < until {
        for i in 0..4096 {
            value = value.wrapping_mul(31).wrapping_add(i);
        }
        std::hint::black_box(value);
    }
}

fn main() {
    let capture = RegionCapture::start("cpu");
    burn(Duration::from_millis(400));
    let cpu = capture.finish();
    let measured = &cpu.regions["cpu"].inclusive;
    #[allow(clippy::cast_precision_loss, reason = "calibration reporting ratio")]
    let cores = measured.process_cpu_ns.unwrap() as f64 / measured.wall_ns as f64;
    assert!((0.7..1.3).contains(&cores), "single-worker cores={cores}");
    let capture = RegionCapture::start("known_wait");
    {
        let _wait = RegionScope::named("sleep_control");
        std::thread::sleep(Duration::from_millis(200));
    }
    let wait = capture.finish();
    let measured = &wait.regions["known_wait/sleep_control"].inclusive;
    assert!(measured.wall_ns >= 200_000_000);
    assert!(measured.process_cpu_ns.unwrap() < measured.wall_ns / 2);
    assert!(
        measured
            .thread_sleeping_ns
            .expect("enable kernel.sched_schedstats for calibration")
            > 150_000_000
    );
    let barrier = Barrier::new(2);
    let delay = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            barrier.wait();
            burn(Duration::from_millis(600));
        });
        barrier.wait();
        let capture = RegionCapture::start("scheduler_delay");
        burn(Duration::from_millis(400));
        let delay = capture.finish();
        worker.join().unwrap();
        delay
    });
    assert!(
        delay.regions["scheduler_delay"]
            .inclusive
            .thread_runnable_ns
            .expect("enable kernel.sched_schedstats")
            > 100_000_000,
        "pin the process to one CPU"
    );
    println!(
        "{}",
        serde_json::json!({"cpu":cpu, "known_wait":wait, "scheduler_delay":delay})
    );
}
