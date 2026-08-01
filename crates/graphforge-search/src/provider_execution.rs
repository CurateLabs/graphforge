//! Bounded provider-neutral attempt execution.

use std::cmp::max;
use std::thread;
use std::time::{Duration, Instant};

use crate::{ProviderError, ProviderFailureClass, ProviderModelContract, ProviderResult};

const WAIT_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(10);

/// Cooperative cancellation/deadline checkpoint used during provider work.
pub type ProviderCheckpoint<'a> = dyn FnMut() -> ProviderResult<()> + 'a;

/// Global limits for one provider execution controller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderExecutionLimits {
    /// Maximum provider calls, including retries and later batches.
    pub provider_calls: usize,
    /// Maximum retries for one logical call.
    pub retries: usize,
    /// Maximum input-token exposure across all attempts.
    pub input_token_exposure: u64,
    /// Maximum caller-estimated cost across all attempts.
    pub estimated_cost_microunits: u64,
    /// Cooperative wall-clock deadline for the controller.
    pub timeout: Duration,
    /// Minimum spacing between provider call starts.
    pub minimum_call_interval: Duration,
    /// Initial deterministic retry delay.
    pub retry_backoff: Duration,
    /// Maximum deterministic retry delay.
    pub maximum_retry_backoff: Duration,
}

impl Default for ProviderExecutionLimits {
    fn default() -> Self {
        Self {
            provider_calls: 128,
            retries: 2,
            input_token_exposure: 1_000_000,
            estimated_cost_microunits: u64::MAX,
            timeout: Duration::from_secs(30),
            minimum_call_interval: Duration::ZERO,
            retry_backoff: Duration::from_millis(100),
            maximum_retry_backoff: Duration::from_secs(2),
        }
    }
}

/// Payload-free estimate reserved before every provider attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderWorkEstimate {
    /// Number of outbound items.
    items: usize,
    /// Outbound UTF-8 bytes, without retaining their contents.
    input_bytes: usize,
    /// Counted input tokens exposed by one attempt.
    input_tokens: u64,
    /// Caller-estimated charge for one attempt.
    estimated_cost_microunits: u64,
}

impl ProviderWorkEstimate {
    /// Construct one non-empty payload-free estimate.
    ///
    /// # Errors
    /// Rejects estimates that cannot describe provider work.
    pub fn new(
        contract: &ProviderModelContract,
        items: usize,
        input_bytes: usize,
        input_tokens: u64,
        estimated_cost_microunits: u64,
    ) -> ProviderResult<Self> {
        if items == 0 || input_bytes == 0 || input_tokens == 0 {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        Ok(Self {
            items,
            input_bytes,
            input_tokens,
            estimated_cost_microunits,
        })
    }

    /// Number of outbound items.
    #[must_use]
    pub const fn items(self) -> usize {
        self.items
    }

    /// Outbound UTF-8 bytes, without their contents.
    #[must_use]
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }

    /// Counted input tokens exposed by one attempt.
    #[must_use]
    pub const fn input_tokens(self) -> u64 {
        self.input_tokens
    }

    /// Caller-estimated charge for one attempt.
    #[must_use]
    pub const fn estimated_cost_microunits(self) -> u64 {
        self.estimated_cost_microunits
    }
}

/// Content-free counters suitable for neutral diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderExecutionSnapshot {
    /// Provider calls already started.
    pub provider_calls: usize,
    /// Input-token exposure already reserved.
    pub input_token_exposure: u64,
    /// Caller-estimated cost already reserved.
    pub estimated_cost_microunits: u64,
}

/// Clock and bounded-wait boundary used by the execution controller.
pub trait ProviderExecutionRuntime {
    /// Monotonic elapsed time in one runtime domain.
    fn elapsed(&self) -> Duration;

    /// Perform one bounded wait while polling caller cancellation.
    fn wait(
        &mut self,
        duration: Duration,
        checkpoint: &mut ProviderCheckpoint<'_>,
    ) -> ProviderResult<()>;
}

/// Standard monotonic runtime for synchronous adapters.
pub struct StandardProviderExecutionRuntime {
    started: Instant,
}

impl StandardProviderExecutionRuntime {
    /// Start a new monotonic runtime domain.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Default for StandardProviderExecutionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderExecutionRuntime for StandardProviderExecutionRuntime {
    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn wait(
        &mut self,
        duration: Duration,
        checkpoint: &mut ProviderCheckpoint<'_>,
    ) -> ProviderResult<()> {
        let started = Instant::now();
        while started.elapsed() < duration {
            checkpoint()?;
            let remaining = duration.saturating_sub(started.elapsed());
            thread::sleep(remaining.min(WAIT_CHECKPOINT_INTERVAL));
        }
        checkpoint()
    }
}

/// Stateful controller whose counters apply across calls and retries.
pub struct ProviderExecutionController {
    contract: ProviderModelContract,
    limits: ProviderExecutionLimits,
    started_at: Duration,
    last_call_at: Option<Duration>,
    snapshot: ProviderExecutionSnapshot,
}

impl ProviderExecutionController {
    /// Validate limits and start one bounded execution domain.
    ///
    /// # Errors
    /// Rejects zero call/token/cost/deadline limits or an inverted backoff cap.
    pub fn new(
        contract: &ProviderModelContract,
        limits: ProviderExecutionLimits,
        runtime: &dyn ProviderExecutionRuntime,
    ) -> ProviderResult<Self> {
        if limits.provider_calls == 0
            || limits.input_token_exposure == 0
            || limits.estimated_cost_microunits == 0
            || limits.timeout.is_zero()
            || limits.maximum_retry_backoff < limits.retry_backoff
        {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        Ok(Self {
            contract: contract.clone(),
            limits,
            started_at: runtime.elapsed(),
            last_call_at: None,
            snapshot: ProviderExecutionSnapshot::default(),
        })
    }

    /// Content-free execution counters.
    #[must_use]
    pub const fn snapshot(&self) -> ProviderExecutionSnapshot {
        self.snapshot
    }

    /// Exact provider/model contract whose failures this controller reports.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Execute one logical provider call, retrying only stable transient classes.
    ///
    /// The attempt receives a guarded checkpoint that combines caller
    /// cancellation with the controller deadline. A value is returned only
    /// when the complete attempt and its post-call checks succeed.
    ///
    /// # Errors
    /// Returns a redacted provider failure for cancellation, timeout, resource
    /// exhaustion, a terminal attempt, or an invalid runtime clock.
    pub fn execute<T, F>(
        &mut self,
        work: ProviderWorkEstimate,
        runtime: &mut dyn ProviderExecutionRuntime,
        checkpoint: &mut ProviderCheckpoint<'_>,
        attempt: &mut F,
    ) -> ProviderResult<T>
    where
        F: FnMut(&mut ProviderCheckpoint<'_>) -> ProviderResult<T>,
    {
        let mut retries = 0_usize;
        loop {
            self.checkpoint(runtime, checkpoint)?;
            let next = self.reserve(work)?;
            let delay = max(self.rate_delay(runtime)?, retry_delay(self.limits, retries));
            if !delay.is_zero() {
                if delay >= self.deadline_remaining(runtime)? {
                    return Err(failure(&self.contract, ProviderFailureClass::Timeout));
                }
                runtime.wait(delay, checkpoint)?;
                self.checkpoint(runtime, checkpoint)?;
            }

            self.snapshot = next;
            self.last_call_at = Some(runtime.elapsed());
            let contract = self.contract.clone();
            let started_at = self.started_at;
            let timeout = self.limits.timeout;
            let result = {
                let mut guarded_checkpoint = || {
                    checkpoint()?;
                    check_deadline(&contract, started_at, timeout, runtime.elapsed())
                };
                attempt(&mut guarded_checkpoint)
            };
            self.checkpoint(runtime, checkpoint)?;

            match result {
                Ok(value) => return Ok(value),
                Err(error) if retryable(error.class()) && retries < self.limits.retries => {
                    retries += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn checkpoint(
        &self,
        runtime: &dyn ProviderExecutionRuntime,
        checkpoint: &mut ProviderCheckpoint<'_>,
    ) -> ProviderResult<()> {
        checkpoint()?;
        check_deadline(
            &self.contract,
            self.started_at,
            self.limits.timeout,
            runtime.elapsed(),
        )
    }

    fn reserve(&self, work: ProviderWorkEstimate) -> ProviderResult<ProviderExecutionSnapshot> {
        let provider_calls = self
            .snapshot
            .provider_calls
            .checked_add(1)
            .ok_or_else(|| exhausted(&self.contract))?;
        let input_token_exposure = self
            .snapshot
            .input_token_exposure
            .checked_add(work.input_tokens)
            .ok_or_else(|| exhausted(&self.contract))?;
        let estimated_cost_microunits = self
            .snapshot
            .estimated_cost_microunits
            .checked_add(work.estimated_cost_microunits)
            .ok_or_else(|| exhausted(&self.contract))?;
        if provider_calls > self.limits.provider_calls
            || input_token_exposure > self.limits.input_token_exposure
            || estimated_cost_microunits > self.limits.estimated_cost_microunits
        {
            return Err(exhausted(&self.contract));
        }
        Ok(ProviderExecutionSnapshot {
            provider_calls,
            input_token_exposure,
            estimated_cost_microunits,
        })
    }

    fn rate_delay(&self, runtime: &dyn ProviderExecutionRuntime) -> ProviderResult<Duration> {
        let now = runtime.elapsed();
        if now < self.started_at || self.last_call_at.is_some_and(|last| now < last) {
            return Err(failure(
                &self.contract,
                ProviderFailureClass::InvalidRequest,
            ));
        }
        let Some(last) = self.last_call_at else {
            return Ok(Duration::ZERO);
        };
        Ok(self
            .limits
            .minimum_call_interval
            .saturating_sub(now.saturating_sub(last)))
    }

    fn deadline_remaining(
        &self,
        runtime: &dyn ProviderExecutionRuntime,
    ) -> ProviderResult<Duration> {
        let now = runtime.elapsed();
        if now < self.started_at {
            return Err(failure(
                &self.contract,
                ProviderFailureClass::InvalidRequest,
            ));
        }
        Ok(self
            .limits
            .timeout
            .saturating_sub(now.saturating_sub(self.started_at)))
    }
}

fn check_deadline(
    contract: &ProviderModelContract,
    started_at: Duration,
    timeout: Duration,
    now: Duration,
) -> ProviderResult<()> {
    if now < started_at {
        return Err(failure(contract, ProviderFailureClass::InvalidRequest));
    }
    if now.saturating_sub(started_at) >= timeout {
        Err(failure(contract, ProviderFailureClass::Timeout))
    } else {
        Ok(())
    }
}

fn retry_delay(limits: ProviderExecutionLimits, retries: usize) -> Duration {
    if retries == 0 || limits.retry_backoff.is_zero() {
        return Duration::ZERO;
    }
    let mut delay = limits.retry_backoff;
    for _ in 1..retries {
        if delay >= limits.maximum_retry_backoff {
            return limits.maximum_retry_backoff;
        }
        delay = delay
            .checked_mul(2)
            .unwrap_or(limits.maximum_retry_backoff)
            .min(limits.maximum_retry_backoff);
    }
    delay
}

const fn retryable(class: ProviderFailureClass) -> bool {
    matches!(
        class,
        ProviderFailureClass::Timeout
            | ProviderFailureClass::Transport
            | ProviderFailureClass::ProviderRejected
    )
}

fn exhausted(contract: &ProviderModelContract) -> ProviderError {
    failure(contract, ProviderFailureClass::ResourceExhausted)
}

fn failure(contract: &ProviderModelContract, class: ProviderFailureClass) -> ProviderError {
    ProviderError::new(contract, class)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use crate::{ProviderCapabilities, ProviderCapability};

    use super::*;

    struct FakeRuntime {
        now: Rc<Cell<Duration>>,
        waits: Vec<Duration>,
    }

    impl FakeRuntime {
        fn new() -> Self {
            Self {
                now: Rc::new(Cell::new(Duration::ZERO)),
                waits: Vec::new(),
            }
        }
    }

    impl ProviderExecutionRuntime for FakeRuntime {
        fn elapsed(&self) -> Duration {
            self.now.get()
        }

        fn wait(
            &mut self,
            duration: Duration,
            checkpoint: &mut ProviderCheckpoint<'_>,
        ) -> ProviderResult<()> {
            checkpoint()?;
            self.waits.push(duration);
            self.now.set(self.now.get().saturating_add(duration));
            checkpoint()
        }
    }

    fn contract() -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            "vendor/model",
            "revision",
            "v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
            TokenizerIdentity {
                identifier: "provider-tokenizer".into(),
                version: "1".into(),
                count_class: TokenCountClass::ProviderReported,
                max_input_tokens: 16,
                normalization: "nfc".into(),
            },
            None,
        )
        .unwrap()
    }

    fn work(tokens: u64, cost: u64) -> ProviderWorkEstimate {
        ProviderWorkEstimate::new(&contract(), 2, 8, tokens, cost).unwrap()
    }

    fn cancelled() -> ProviderError {
        failure(&contract(), ProviderFailureClass::Cancelled)
    }

    #[test]
    fn success_records_only_content_free_counters() {
        let contract = contract();
        let mut runtime = FakeRuntime::new();
        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits::default(),
            &runtime,
        )
        .unwrap();
        let estimate = work(3, 7);
        assert_eq!(estimate.items(), 2);
        assert_eq!(estimate.input_bytes(), 8);
        assert_eq!(estimate.input_tokens(), 3);
        assert_eq!(estimate.estimated_cost_microunits(), 7);
        let result = controller
            .execute(estimate, &mut runtime, &mut || Ok(()), &mut |_| {
                Ok("complete")
            })
            .unwrap();
        assert_eq!(result, "complete");
        assert_eq!(
            controller.snapshot(),
            ProviderExecutionSnapshot {
                provider_calls: 1,
                input_token_exposure: 3,
                estimated_cost_microunits: 7,
            }
        );
    }

    #[test]
    fn transient_retry_obeys_rate_and_backoff_without_jitter() {
        let contract = contract();
        let mut runtime = FakeRuntime::new();
        let mut limits = ProviderExecutionLimits::default();
        limits.minimum_call_interval = Duration::from_secs(5);
        limits.retry_backoff = Duration::from_secs(2);
        limits.maximum_retry_backoff = Duration::from_secs(4);
        let mut controller = ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
        let calls = Cell::new(0);
        let result = controller
            .execute(work(3, 7), &mut runtime, &mut || Ok(()), &mut |_| {
                calls.set(calls.get() + 1);
                if calls.get() == 1 {
                    Err(failure(&contract, ProviderFailureClass::Transport))
                } else {
                    Ok(42)
                }
            })
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(runtime.waits, [Duration::from_secs(5)]);
        assert_eq!(controller.snapshot().provider_calls, 2);
        assert_eq!(controller.snapshot().input_token_exposure, 6);
        assert_eq!(controller.snapshot().estimated_cost_microunits, 14);
    }

    #[test]
    fn terminal_and_exhausted_attempts_never_return_a_value() {
        let contract = contract();
        for class in [
            ProviderFailureClass::Authentication,
            ProviderFailureClass::InvalidRequest,
            ProviderFailureClass::UnsupportedCapability,
            ProviderFailureClass::MalformedResponse,
            ProviderFailureClass::ResourceExhausted,
            ProviderFailureClass::Cancelled,
        ] {
            let mut runtime = FakeRuntime::new();
            let mut controller = ProviderExecutionController::new(
                &contract,
                ProviderExecutionLimits::default(),
                &runtime,
            )
            .unwrap();
            let calls = Cell::new(0);
            let error = controller
                .execute(work(1, 1), &mut runtime, &mut || Ok(()), &mut |_| {
                    calls.set(calls.get() + 1);
                    Err::<(), _>(failure(&contract, class))
                })
                .unwrap_err();
            assert_eq!(error.class(), class);
            assert_eq!(calls.get(), 1);
        }

        let mut runtime = FakeRuntime::new();
        let mut limits = ProviderExecutionLimits::default();
        limits.provider_calls = 1;
        let mut controller = ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
        let error = controller
            .execute(work(1, 1), &mut runtime, &mut || Ok(()), &mut |_| {
                Err::<(), _>(failure(&contract, ProviderFailureClass::Timeout))
            })
            .unwrap_err();
        assert_eq!(error.class(), ProviderFailureClass::ResourceExhausted);
        assert_eq!(controller.snapshot().provider_calls, 1);

        let mut runtime = FakeRuntime::new();
        let mut limits = ProviderExecutionLimits::default();
        limits.retries = 1;
        let mut controller = ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
        let attempts = Cell::new(0);
        let error = controller
            .execute(work(1, 1), &mut runtime, &mut || Ok(()), &mut |_| {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(failure(&contract, ProviderFailureClass::Transport))
            })
            .unwrap_err();
        assert_eq!(error.class(), ProviderFailureClass::Transport);
        assert_eq!(attempts.get(), 2);
    }

    #[test]
    fn token_cost_and_overflow_are_reserved_before_attempts() {
        let contract = contract();
        for (token_limit, cost_limit, estimate) in [
            (2, 10, work(3, 1)),
            (10, 2, work(1, 3)),
            (u64::MAX, u64::MAX, work(u64::MAX, u64::MAX)),
        ] {
            let mut runtime = FakeRuntime::new();
            let mut limits = ProviderExecutionLimits::default();
            limits.input_token_exposure = token_limit;
            limits.estimated_cost_microunits = cost_limit;
            let mut controller =
                ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
            let attempts = Cell::new(0);
            let first = controller.execute(estimate, &mut runtime, &mut || Ok(()), &mut |_| {
                attempts.set(attempts.get() + 1);
                Err::<(), _>(failure(&contract, ProviderFailureClass::Transport))
            });
            assert_eq!(
                first.unwrap_err().class(),
                ProviderFailureClass::ResourceExhausted
            );
            assert!(attempts.get() <= 1);
        }
    }

    #[test]
    fn deadline_is_checked_before_during_after_and_after_wait() {
        let contract = contract();
        for phase in 0..3 {
            let mut runtime = FakeRuntime::new();
            let clock = Rc::clone(&runtime.now);
            let mut limits = ProviderExecutionLimits::default();
            limits.timeout = Duration::from_secs(10);
            let mut controller =
                ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
            if phase == 0 {
                clock.set(Duration::from_secs(10));
            }
            let result = controller.execute(
                work(1, 1),
                &mut runtime,
                &mut || Ok(()),
                &mut |checkpoint| {
                    if phase == 1 {
                        clock.set(Duration::from_secs(10));
                        checkpoint()?;
                    } else if phase == 2 {
                        clock.set(Duration::from_secs(10));
                    }
                    Ok(())
                },
            );
            assert_eq!(result.unwrap_err().class(), ProviderFailureClass::Timeout);
        }

        let mut runtime = FakeRuntime::new();
        let mut limits = ProviderExecutionLimits::default();
        limits.timeout = Duration::from_secs(3);
        limits.minimum_call_interval = Duration::from_secs(5);
        let mut controller = ProviderExecutionController::new(&contract, limits, &runtime).unwrap();
        let calls = Cell::new(0);
        let result = controller.execute(work(1, 1), &mut runtime, &mut || Ok(()), &mut |_| {
            calls.set(calls.get() + 1);
            Err::<(), _>(failure(&contract, ProviderFailureClass::Transport))
        });
        assert_eq!(result.unwrap_err().class(), ProviderFailureClass::Timeout);
        assert_eq!(calls.get(), 1);
        assert!(runtime.waits.is_empty());
    }

    #[test]
    fn cancellation_clock_regression_and_invalid_limits_are_structured() {
        let contract = contract();
        let mut runtime = FakeRuntime::new();
        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits::default(),
            &runtime,
        )
        .unwrap();
        let error = controller
            .execute(
                work(1, 1),
                &mut runtime,
                &mut || Err(cancelled()),
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert_eq!(error.class(), ProviderFailureClass::Cancelled);
        assert_eq!(controller.snapshot(), ProviderExecutionSnapshot::default());

        runtime.now.set(Duration::from_secs(5));
        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits::default(),
            &runtime,
        )
        .unwrap();
        runtime.now.set(Duration::ZERO);
        assert_eq!(
            controller
                .execute(work(1, 1), &mut runtime, &mut || Ok(()), &mut |_| Ok(()))
                .unwrap_err()
                .class(),
            ProviderFailureClass::InvalidRequest
        );

        let invalid = ProviderExecutionLimits {
            provider_calls: 0,
            ..ProviderExecutionLimits::default()
        };
        assert_eq!(
            ProviderExecutionController::new(&contract, invalid, &runtime)
                .err()
                .unwrap()
                .class(),
            ProviderFailureClass::InvalidRequest
        );
    }

    #[test]
    fn errors_are_redacted() {
        let error = failure(&contract(), ProviderFailureClass::ProviderRejected);
        let rendered = error.to_string();
        assert!(rendered.contains("provider=openrouter"));
        assert!(rendered.contains("model=vendor/model"));
        assert!(!rendered.contains("credential"));
        assert!(!rendered.contains("source text"));
    }
}
