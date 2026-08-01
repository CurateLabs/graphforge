//! Fuzz target: `graphforge_cypher::parse` → `graphforge_ir::Binder::bind` (#602).
//!
//! Any input that parses is then bound in exploratory mode (no ontology, all
//! labels/types auto-interned). Binding arbitrary-but-parseable queries must
//! never panic — it may only return `Ok` or a `Vec<BindError>`.
#![no_main]

use std::sync::{Arc, Mutex};

use graphforge_cypher::{Binder, OntologyMode, RuntimeCatalog, parse};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data)
        && let Ok(ast) = parse(s)
    {
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
        let _ = binder.bind(&ast);
    }
});
