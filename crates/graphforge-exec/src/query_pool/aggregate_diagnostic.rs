//! Diagnostic scheduling of genuine aggregate IPC; never changes admission.
use super::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;

#[derive(Debug, Default)]
struct Trace {
    sequence: usize,
    events: VecDeque<String>,
    first_refusal: Option<String>,
}

#[derive(Debug)]
pub(crate) struct AggregateDiagnosticPool {
    inner: QueryMemoryPool,
    trace: Mutex<Trace>,
    decoder_held: AtomicBool,
    pub(crate) decoder_started: tokio::sync::Notify,
    pub(crate) loan_started: tokio::sync::Notify,
    release: (Mutex<bool>, std::sync::Condvar),
}

impl AggregateDiagnosticPool {
    pub(crate) fn new(bytes: usize) -> Self {
        Self {
            inner: QueryMemoryPool::new(bytes),
            trace: Mutex::new(Trace::default()),
            decoder_held: AtomicBool::new(false),
            decoder_started: tokio::sync::Notify::new(),
            loan_started: tokio::sync::Notify::new(),
            release: (Mutex::new(false), std::sync::Condvar::new()),
        }
    }

    // Called under trace, after the delegated method has released the query
    // mutex. Serializing delegate+snapshot gives an ordered bounded trace.
    fn record(
        &self,
        trace: &mut Trace,
        kind: &str,
        consumer: &MemoryConsumer,
        reservation: usize,
        bytes: usize,
        refused: bool,
    ) {
        let state = self.inner.locked();
        let spiller = state.spillers.get(&consumer.id()).copied();
        let event = format!(
            "{} {kind} id={} name={} spill={} request={bytes} reservation={reservation} total={} nonspill={} floor={} other_floors={} loans={} owner={spiller:?}",
            trace.sequence,
            consumer.id(),
            consumer.name(),
            consumer.can_spill(),
            state.reserved,
            state.non_spilling_reserved,
            self.inner.floor(state.spillers.len()),
            self.inner.unfilled_floors_except(&state, consumer.id()),
            QueryMemoryPool::lent(&state),
        );
        trace.sequence += 1;
        drop(state);
        // Freeze the last 64 transitions at the first aggregate refusal after
        // entering the decoder barrier; later teardown cannot erase its cause.
        if trace.first_refusal.is_none() {
            if trace.events.len() == 64 {
                trace.events.pop_front();
            }
            trace.events.push_back(event.clone());
            if refused
                && self.decoder_held.load(Ordering::Acquire)
                && consumer.name().starts_with(AGGREGATE_CONSUMER_PREFIX)
            {
                let stack: String = std::backtrace::Backtrace::force_capture()
                    .to_string()
                    .chars()
                    .take(16_384)
                    .collect();
                trace.first_refusal = Some(format!("{event}\n{stack}"));
            }
        }
    }

    pub(crate) fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }

    pub(crate) fn report(&self) -> String {
        let trace = self.trace.lock().unwrap();
        format!(
            "events={}\n{}\nfirst_refusal={}",
            trace.sequence,
            trace.events.iter().cloned().collect::<Vec<_>>().join("\n"),
            trace.first_refusal.as_deref().unwrap_or("none")
        )
    }
}

impl Display for AggregateDiagnosticPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl MemoryPool for AggregateDiagnosticPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        let mut trace = self.trace.lock().unwrap();
        self.inner.register(consumer);
        self.record(&mut trace, "register", consumer, 0, 0, false);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let mut trace = self.trace.lock().unwrap();
        self.inner.unregister(consumer);
        self.record(&mut trace, "unregister", consumer, 0, 0, false);
    }

    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        let mut trace = self.trace.lock().unwrap();
        self.inner.grow(reservation, bytes);
        self.record(
            &mut trace,
            "grow",
            reservation.consumer(),
            reservation.size(),
            bytes,
            false,
        );
    }

    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        let mut trace = self.trace.lock().unwrap();
        self.inner.shrink(reservation, bytes);
        self.record(
            &mut trace,
            "shrink",
            reservation.consumer(),
            reservation.size(),
            bytes,
            false,
        );
    }

    fn try_grow(&self, reservation: &MemoryReservation, bytes: usize) -> Result<()> {
        let result = {
            let mut trace = self.trace.lock().unwrap();
            let before = self
                .inner
                .locked()
                .spillers
                .get(&reservation.consumer().id())
                .map(|s| s.grant);
            let result = self.inner.try_grow(reservation, bytes);
            self.record(
                &mut trace,
                "try_grow",
                reservation.consumer(),
                reservation.size(),
                bytes,
                result.is_err(),
            );
            let after = self
                .inner
                .locked()
                .spillers
                .get(&reservation.consumer().id())
                .map(|s| s.grant);
            if self.decoder_held.load(Ordering::Acquire)
                && before == Some(Grant::Armed)
                && after.is_some_and(|g| matches!(g, Grant::Lent(_)))
            {
                self.loan_started.notify_one();
            }
            result
        };
        if result.is_ok()
            && bytes > 0
            && reservation.consumer().name() == "SpillReaderDecode"
            && !self.decoder_held.swap(true, Ordering::AcqRel)
        {
            self.decoder_started.notify_one();
            // No trace/query mutex is held while the real blocking IPC worker
            // waits. The driver releases it after a real new aggregate loan, or on
            // unwind/timeout. No admission failure is injected.
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
        }
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Keep the session's ordinary top-consumer reporter around the observed pool.
#[derive(Debug)]
pub(crate) struct SharedDiagnosticPool(pub(crate) Arc<AggregateDiagnosticPool>);
impl Display for SharedDiagnosticPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self.0.as_ref(), f)
    }
}
impl MemoryPool for SharedDiagnosticPool {
    fn name(&self) -> &str {
        self.0.name()
    }
    fn register(&self, c: &MemoryConsumer) {
        self.0.register(c);
    }
    fn unregister(&self, c: &MemoryConsumer) {
        self.0.unregister(c);
    }
    fn grow(&self, r: &MemoryReservation, n: usize) {
        self.0.grow(r, n);
    }
    fn shrink(&self, r: &MemoryReservation, n: usize) {
        self.0.shrink(r, n);
    }
    fn try_grow(&self, r: &MemoryReservation, n: usize) -> Result<()> {
        self.0.try_grow(r, n)
    }
    fn reserved(&self) -> usize {
        self.0.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.0.memory_limit()
    }
}

pub(crate) struct ReleaseDecoder(pub(crate) Arc<AggregateDiagnosticPool>);
impl Drop for ReleaseDecoder {
    fn drop(&mut self) {
        self.0.release();
    }
}
