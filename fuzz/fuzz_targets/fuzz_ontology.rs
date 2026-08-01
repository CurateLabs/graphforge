//! Fuzz target: `graphforge_ontology::OntologyLoader` (#602).
//!
//! Raw bytes are fed to both the YAML and JSON loaders. Deserializing
//! arbitrary input must never panic — it may only return `Ok(OntologyDoc)` or
//! a structured `OntologyError`. (`&[u8]` implements `std::io::Read`.)
#![no_main]

use graphforge_ontology::OntologyLoader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = OntologyLoader::load_yaml(data);
    let _ = OntologyLoader::load_json(data);
});
