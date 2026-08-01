//! [`OntologyRegistry`] — thread-safe store for named ontologies.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::handle::OntologyHandle;

/// Thread-safe store for named ontologies.
///
/// Multiple ontologies can be registered simultaneously; callers retrieve them
/// by `ontology_id`. When exactly one ontology is loaded,
/// [`OntologyRegistry::default_ontology`] returns it without needing to know
/// its ID.
#[derive(Default)]
pub struct OntologyRegistry {
    map: RwLock<HashMap<String, OntologyHandle>>,
}

impl OntologyRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handle`, keyed by its `id()`.
    ///
    /// If an ontology with the same ID is already registered, it is replaced.
    pub fn register(&self, handle: OntologyHandle) {
        let key = handle.id().to_owned();
        self.map
            .write()
            .expect("OntologyRegistry lock poisoned")
            .insert(key, handle);
    }

    /// Look up an ontology by its stable ID.
    #[must_use]
    pub fn get(&self, ontology_id: &str) -> Option<OntologyHandle> {
        self.map
            .read()
            .expect("OntologyRegistry lock poisoned")
            .get(ontology_id)
            .cloned()
    }

    /// Return the sole registered ontology, or `None` if zero or more than one
    /// are loaded.
    #[must_use]
    pub fn default_ontology(&self) -> Option<OntologyHandle> {
        let map = self.map.read().expect("OntologyRegistry lock poisoned");
        if map.len() == 1 {
            map.values().next().cloned()
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::OntologyCompiler;
    use crate::handle::OntologyHandle;
    use crate::ontology::OntologyDoc;

    fn make_handle(id: &str) -> OntologyHandle {
        let doc = OntologyDoc {
            ontology_id: id.to_string(),
            version: "1.0".to_string(),
            entity_types: vec![],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap())
    }

    #[test]
    fn registry_register_and_get() {
        let reg = OntologyRegistry::new();
        let h = make_handle("core");
        reg.register(h);
        let retrieved = reg.get("core").unwrap();
        assert_eq!(retrieved.id(), "core");
        assert!(reg.get("other").is_none());
    }

    #[test]
    fn registry_default_single() {
        let reg = OntologyRegistry::new();
        reg.register(make_handle("core"));
        let def = reg.default_ontology().unwrap();
        assert_eq!(def.id(), "core");
    }

    #[test]
    fn registry_default_multi() {
        let reg = OntologyRegistry::new();
        reg.register(make_handle("core"));
        reg.register(make_handle("extra"));
        assert!(reg.default_ontology().is_none());
    }

    #[test]
    fn registry_default_empty() {
        let reg = OntologyRegistry::new();
        assert!(reg.default_ontology().is_none());
    }

    #[test]
    fn registry_replace_on_same_id() {
        let reg = OntologyRegistry::new();
        reg.register(make_handle("core"));
        // Replace with a handle that has the same id but different content.
        let mut doc = OntologyDoc {
            ontology_id: "core".to_string(),
            version: "2.0".to_string(),
            entity_types: vec![],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        doc.version = "2.0".to_string();
        reg.register(OntologyHandle::new(
            OntologyCompiler::compile(&doc).unwrap(),
        ));
        let h = reg.get("core").unwrap();
        assert_eq!(h.version(), "2.0");
    }
}
