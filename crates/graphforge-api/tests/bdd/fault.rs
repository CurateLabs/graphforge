//! Test-only TCK performance fault injection: the known positive for #1654.
//!
//! Setting both variables delays scenarios inside the timed region, both for
//! the Cucumber whole-TCK run that BenchExec measures and for each iteration
//! of the Divan per-scenario benchmark (`benches/tck_scenarios/`):
//!
//! * `GF_TCK_PERF_FAULT_DELAY_MS=<positive integer>`: the added delay;
//! * `GF_TCK_PERF_FAULT_SCENARIO=<feature>:<line>:<name>`, or `*` for every
//!   scenario.
//!
//! Setting only one of the two, or an invalid value, fails closed. An active
//! injection is announced once on stderr; the `make tck-perf` driver requires
//! that announcement and records the injection in the run provenance, so an
//! injected run can never be captured as a baseline. Nothing here is compiled
//! into a product crate.

use std::sync::OnceLock;
use std::time::Duration;

pub const DELAY_ENV: &str = "GF_TCK_PERF_FAULT_DELAY_MS";
pub const SCENARIO_ENV: &str = "GF_TCK_PERF_FAULT_SCENARIO";

/// Which scenarios an injection delays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Scope {
    All,
    Scenario(String),
}

/// One configured delay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fault {
    pub scope: Scope,
    pub delay: Duration,
}

impl Fault {
    /// Parse the two environment values. `Ok(None)` only when both are unset.
    pub fn parse(delay_ms: Option<&str>, scenario: Option<&str>) -> Result<Option<Self>, String> {
        match (delay_ms, scenario) {
            (None, None) => Ok(None),
            (Some(_), None) | (None, Some(_)) => Err(format!(
                "TCK fault injection needs both {DELAY_ENV} and {SCENARIO_ENV}"
            )),
            (Some(delay_ms), Some(scenario)) => {
                let millis: u64 = delay_ms
                    .trim()
                    .parse()
                    .map_err(|_| format!("{DELAY_ENV} must be a positive integer: {delay_ms:?}"))?;
                if millis == 0 {
                    return Err(format!(
                        "{DELAY_ENV} must be a positive integer: {delay_ms:?}"
                    ));
                }
                let scope = match scenario.trim() {
                    "" => return Err(format!("{SCENARIO_ENV} must name a scenario or `*`")),
                    "*" => Scope::All,
                    key => Scope::Scenario(key.to_owned()),
                };
                Ok(Some(Self {
                    scope,
                    delay: Duration::from_millis(millis),
                }))
            }
        }
    }

    /// The delay to inject into the scenario keyed `key`, if any.
    pub fn delay_for(&self, key: &str) -> Option<Duration> {
        match &self.scope {
            Scope::All => Some(self.delay),
            Scope::Scenario(named) if named == key => Some(self.delay),
            Scope::Scenario(_) => None,
        }
    }

    /// The stderr line the `make tck-perf` driver requires for an injected run.
    pub fn announcement(&self) -> String {
        let scope = match &self.scope {
            Scope::All => "*".to_owned(),
            Scope::Scenario(key) => key.clone(),
        };
        format!(
            "TCK PERF FAULT INJECTION: delay_ms={} scenario={scope}",
            self.delay.as_millis()
        )
    }
}

/// The process-wide injection from the environment, announced once.
/// Invalid configuration panics rather than running uninjected.
pub fn active() -> Option<&'static Fault> {
    static ACTIVE: OnceLock<Option<Fault>> = OnceLock::new();
    ACTIVE
        .get_or_init(|| {
            let delay = std::env::var(DELAY_ENV).ok();
            let scenario = std::env::var(SCENARIO_ENV).ok();
            let fault = Fault::parse(delay.as_deref(), scenario.as_deref())
                .unwrap_or_else(|error| panic!("{error}"));
            if let Some(fault) = &fault {
                eprintln!("{}", fault.announcement());
            }
            fault
        })
        .as_ref()
}

/// Delay the scenario keyed `key` if an injection targets it. Call inside the
/// timed region.
pub fn inject(key: &str) {
    if let Some(delay) = active().and_then(|fault| fault.delay_for(key)) {
        std::thread::sleep(delay);
    }
}
