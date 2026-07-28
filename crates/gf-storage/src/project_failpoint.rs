//! Test-only process failpoints for the project-generation protocol.
//!
//! Production builds compile these calls to no-ops. Unit-test subprocesses opt
//! in with an exact environment cookie and either terminate at a named phase or
//! receive the phase's stable injected error.

use gf_core::GfError;
#[cfg(any(test, feature = "test-failpoints"))]
use gf_core::ProjectErrorCode;
use uuid::Uuid;

#[cfg(any(test, feature = "test-failpoints"))]
const ENABLE_ENV: &str = "GRAPHFORGE_PROJECT_FAILPOINTS";
#[cfg(any(test, feature = "test-failpoints"))]
const ACTIVE_ENV: &str = "GRAPHFORGE_PROJECT_FAILPOINT";
#[cfg(any(test, feature = "test-failpoints"))]
const ENABLE_COOKIE: &str = "graphforge-internal-subprocess-v1";
#[cfg(any(test, feature = "test-failpoints"))]
const EXIT_CODE: i32 = 86;

#[cfg(any(test, feature = "test-failpoints"))]
pub(crate) fn hit(
    name: &str,
    transaction_uuid: Option<Uuid>,
    generation_uuid: Option<Uuid>,
    phase: &str,
    committed: bool,
) -> Result<(), GfError> {
    if std::env::var(ENABLE_ENV).as_deref() != Ok(ENABLE_COOKIE) {
        return Ok(());
    }
    let Ok(active) = std::env::var(ACTIVE_ENV) else {
        return Ok(());
    };
    if active == name {
        std::process::exit(EXIT_CODE);
    }
    if active == format!("{name}.error") {
        let transaction = transaction_uuid
            .map(|uuid| uuid.hyphenated().to_string())
            .unwrap_or_else(|| "none".into());
        let generation = generation_uuid
            .map(|uuid| uuid.hyphenated().to_string())
            .unwrap_or_else(|| "none".into());
        return Err(GfError::Project {
            code: ProjectErrorCode::PublicationFailed,
            message: format!(
                "transaction_uuid={transaction} generation_uuid={generation} \
                 phase={phase} committed={committed} cause=injected_failpoint"
            ),
        });
    }
    Ok(())
}

#[cfg(not(any(test, feature = "test-failpoints")))]
#[inline]
#[allow(clippy::unnecessary_wraps)]
pub(crate) const fn hit(
    _name: &str,
    _transaction_uuid: Option<Uuid>,
    _generation_uuid: Option<Uuid>,
    _phase: &str,
    _committed: bool,
) -> Result<(), GfError> {
    Ok(())
}

#[cfg(test)]
pub(crate) const fn exit_code() -> i32 {
    EXIT_CODE
}
