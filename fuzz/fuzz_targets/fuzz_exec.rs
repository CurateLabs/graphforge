//! Fuzz target: end-to-end execution via `graphforge_api::GraphForge::execute` (#603).
//!
//! Drives the full pipeline — parse → bind → lower → execute — against a fresh
//! in-memory (temp-dir-backed) instance per input. Executing arbitrary
//! parseable queries must never panic; it may only return `Ok` or a structured
//! `GfError`. A fresh instance per input keeps state from accumulating across
//! iterations (and is dropped — temp dir cleaned — at the end of each).
#![no_main]

use graphforge_api::GraphForge;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data)
        && let Ok(forge) = GraphForge::new(None)
    {
        let _ = forge.execute(s);
    }
});
