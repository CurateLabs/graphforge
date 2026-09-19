//! Calling-thread scheduler counters; absence never means zero.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SchedulerSample {
    pub running: Option<u64>,
    pub runnable: Option<u64>,
    pub sleeping: Option<u64>,
    pub uninterruptible: Option<u64>,
    pub iowait: Option<u64>,
}

#[cfg(target_os = "linux")]
pub(super) fn sample() -> SchedulerSample {
    let enabled =
        std::fs::read_to_string("/proc/sys/kernel/sched_schedstats").is_ok_and(|s| s.trim() == "1");
    let stats = std::fs::read_to_string("/proc/thread-self/schedstat").ok();
    let (running, runnable) = stats
        .as_deref()
        .and_then(|s| parse_schedstat(s, enabled))
        .map_or((None, None), |(r, q)| (Some(r), q));
    let mut result = SchedulerSample {
        running,
        runnable,
        ..SchedulerSample::default()
    };
    if enabled && let Ok(sched) = std::fs::read_to_string("/proc/thread-self/sched") {
        result.sleeping = field(&sched, "sum_sleep_runtime");
        result.uninterruptible = field(&sched, "sum_block_runtime");
        result.iowait = field(&sched, "iowait_sum");
    }
    result
}

#[cfg(not(target_os = "linux"))]
pub(super) fn sample() -> SchedulerSample {
    SchedulerSample::default()
}

#[cfg(target_os = "linux")]
fn parse_schedstat(text: &str, enabled: bool) -> Option<(u64, Option<u64>)> {
    let mut fields = text.split_whitespace();
    let running = fields.next()?.parse().ok()?;
    let runnable = fields.next()?.parse().ok()?;
    Some((running, enabled.then_some(runnable)))
}

// Linux prints PN scheduler counters as milliseconds with six fractional
// decimal digits. Older kernels prefix the name with `se.statistics.`.
#[cfg(target_os = "linux")]
fn field(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim().rsplit('.').next()? == name).then_some(value.trim())
        })
        .and_then(decimal_millis_to_nanos)
}

#[cfg(target_os = "linux")]
fn decimal_millis_to_nanos(text: &str) -> Option<u64> {
    let (millis, fraction) = text.split_once('.')?;
    if fraction.is_empty() || fraction.len() > 6 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let scale = 10_u64.checked_pow(u32::try_from(6 - fraction.len()).ok()?)?;
    millis
        .parse::<u64>()
        .ok()?
        .checked_mul(1_000_000)?
        .checked_add(fraction.parse::<u64>().ok()?.checked_mul(scale)?)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn disabled_scheduler_accounting_is_unknown_not_zero_delay() {
        assert_eq!(parse_schedstat("12 7 3", true), Some((12, Some(7))));
        assert_eq!(parse_schedstat("12 0 3", false), Some((12, None)));
        assert_eq!(parse_schedstat("broken", true), None);
    }

    #[test]
    fn scheduler_sleep_and_block_counters_preserve_units_and_missing_fields() {
        let sched = "se.statistics.sum_sleep_runtime : 200.123456\nsum_block_runtime : 12.000001\niowait_sum : 3.25";
        assert_eq!(field(sched, "sum_sleep_runtime"), Some(200_123_456));
        assert_eq!(field(sched, "sum_block_runtime"), Some(12_000_001));
        assert_eq!(field(sched, "iowait_sum"), Some(3_250_000));
        assert_eq!(field(sched, "missing"), None);
        for malformed in [
            "-1.000000",
            "1.0000001",
            "1",
            "1.x",
            "18446744073709551615.000000",
        ] {
            assert_eq!(decimal_millis_to_nanos(malformed), None);
        }
    }
}
