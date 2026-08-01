//! Fuzz target: `graphforge_cypher::parse` (#602).
//!
//! Parsing arbitrary input must never panic — it may only return `Ok` or a
//! structured `ParseError`. libfuzzer flags any panic, OOM, or timeout.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = graphforge_cypher::parse(s);
    }
});
