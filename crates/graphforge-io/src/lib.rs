//! GraphForge CSV/JSON/Parquet/IPC sinks.
#![forbid(unsafe_code)]

/// Returns the crate name.
#[must_use]
pub const fn name() -> &'static str {
    "graphforge-io"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(name(), "graphforge-io");
    }
}
