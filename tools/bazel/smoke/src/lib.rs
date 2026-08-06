//! Minimal rules_rust smoke target for Bazel bootstrap (#11).
//!
//! Intentionally depends only on `core`/`alloc` via libstd so ordinary
//! compilation does not shell out to Cargo and does not require crate_universe
//! yet. First-party GraphForge crates are modeled in #10+.

/// Returns a stable token proving the Bazel-built library linked.
pub fn bazel_smoke_token() -> &'static str {
    "graphforge-bazel-smoke"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_token_is_stable() {
        assert_eq!(bazel_smoke_token(), "graphforge-bazel-smoke");
    }
}
