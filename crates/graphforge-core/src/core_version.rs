//! Detection of data written by another GraphForge Core version.
//!
//! Before v1.0.0, data is only guaranteed to run on the Core version that
//! wrote it (ADR 0054 section 9). Opening a durable project, importing a
//! portable package and cloning a published Version each pass the producing
//! version they found through [`check_core_version`], the one place that
//! decides what a mismatch means. Today a mismatch warns and nothing is
//! refused; a refusal policy or a data fix-up plugs in here.

use serde::{Deserialize, Serialize};

use crate::manifest::GRAPHFORGE_VERSION;

/// Stable code of the warning for data from another Core version.
pub const CORE_VERSION_MISMATCH_CODE: &str = "GF_CORE_VERSION_MISMATCH";

/// Structured warning that data was produced by a different Core version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoreVersionWarning {
    /// Always [`CORE_VERSION_MISMATCH_CODE`].
    pub code: String,
    /// Exact Core version that produced the data.
    pub producing_core_version: String,
    /// Exact Core version that is running.
    pub running_core_version: String,
    /// Human-readable text naming both versions and how to get a matching runtime.
    pub message: String,
}

/// Compare the Core version that produced data with the running Core version.
///
/// "Same version" is an exact match of the canonical version string. A `None`
/// producing version (data written before the declaration existed) is not a
/// mismatch: only a recorded, different version warns.
#[must_use]
pub fn check_core_version(producing_core_version: Option<&str>) -> Option<CoreVersionWarning> {
    let producing = producing_core_version?;
    if producing == GRAPHFORGE_VERSION {
        return None;
    }
    Some(CoreVersionWarning {
        code: CORE_VERSION_MISMATCH_CODE.to_owned(),
        producing_core_version: producing.to_owned(),
        running_core_version: GRAPHFORGE_VERSION.to_owned(),
        message: format!(
            "data was produced by GraphForge Core {producing}, but this is Core \
             {GRAPHFORGE_VERSION}; it may not read correctly. Use a matching runtime: \
             `python -m pip install graphforge=={producing}`, \
             `npm install @curatelabs/graphforge@{producing}`, or \
             `npx @curatelabs/graphforge-cli@{producing}`"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_and_absent_versions_do_not_warn() {
        assert_eq!(check_core_version(Some(GRAPHFORGE_VERSION)), None);
        assert_eq!(check_core_version(None), None);
    }

    #[test]
    fn a_different_version_warns_naming_both() {
        let warning = check_core_version(Some("0.0.1")).unwrap();
        assert_eq!(warning.code, "GF_CORE_VERSION_MISMATCH");
        assert!(warning.message.contains("Core 0.0.1"));
        assert!(warning.message.contains(GRAPHFORGE_VERSION));
        assert!(warning.message.contains("graphforge==0.0.1"));
    }
}
