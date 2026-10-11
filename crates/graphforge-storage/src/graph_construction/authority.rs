//! What a construction may route a row by: a persisted ontology composition
//! with its physical bindings, or the one ontology document a legacy project
//! adopted.

use std::collections::BTreeMap;

use graphforge_core::{GfError, OntologyMode, TypeId};
use graphforge_ir::CompositionBindingContext;
use graphforge_ontology::SymbolKind;
use graphforge_value::TaggedTypeId;
use sha2::{Digest, Sha256};

use super::{ConstructionSemanticAuthority, hex, storage};
use crate::SemanticStorageBindings;

/// Exact semantic authority of a project that adopted a single ontology
/// document and has not migrated to a composition (ADR 0023).
///
/// Such a project carries no composition context and no storage bindings. Its
/// entity and relation types are the document's declarations, stored under
/// their ontology identities, and its routes are named by label. This is the
/// authority `add_node` and the Cypher writer already honour for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SingleDocumentAuthority {
    mode: OntologyMode,
    ontology_sha256: String,
    entity_types: BTreeMap<String, TaggedTypeId>,
    relation_types: BTreeMap<String, TaggedTypeId>,
}

impl SingleDocumentAuthority {
    /// Pin the declarations of the adopted document.
    ///
    /// # Errors
    /// Refuses an exploratory mode, which has no ontology to pin, and an
    /// identity outside the ontology identity space.
    pub fn new(
        mode: OntologyMode,
        ontology_sha256: String,
        entity_types: impl IntoIterator<Item = (String, TypeId)>,
        relation_types: impl IntoIterator<Item = (String, TypeId)>,
    ) -> Result<Self, GfError> {
        if mode == OntologyMode::Exploratory {
            return Err(storage(
                "single-document construction authority requires an advisory or strict mode",
            ));
        }
        let pin = |declared: Vec<(String, TypeId)>| {
            declared
                .into_iter()
                .map(|(name, id)| Ok((name, TaggedTypeId::ontology(id).map_err(storage)?)))
                .collect::<Result<BTreeMap<_, _>, GfError>>()
        };
        Ok(Self {
            mode,
            ontology_sha256,
            entity_types: pin(entity_types.into_iter().collect())?,
            relation_types: pin(relation_types.into_iter().collect())?,
        })
    }

    pub(crate) const fn mode(&self) -> OntologyMode {
        self.mode
    }

    fn digest(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"graphforge-construction-single-document-authority/v1\0");
        digest.update([match self.mode {
            OntologyMode::Exploratory => 0,
            OntologyMode::Advisory => 1,
            OntologyMode::Strict => 2,
        }]);
        digest.update(self.ontology_sha256.as_bytes());
        for (tag, declared) in [(b'E', &self.entity_types), (b'R', &self.relation_types)] {
            for (name, id) in declared {
                digest.update([tag]);
                digest.update((name.len() as u64).to_be_bytes());
                digest.update(name.as_bytes());
                digest.update(id.encode().to_be_bytes());
            }
        }
        hex(&digest.finalize())
    }

    /// The identity a declared type is stored under. `None` for a type the
    /// document does not declare: refused under a strict mode, a runtime type
    /// under an advisory one.
    ///
    /// # Errors
    /// A strict mode refuses a type the document does not declare.
    pub(crate) fn declared(
        &self,
        kind: SymbolKind,
        name: &str,
    ) -> Result<Option<TaggedTypeId>, GfError> {
        let (declared, noun) = match kind {
            SymbolKind::Entity => (&self.entity_types, "entity"),
            SymbolKind::Relation => (&self.relation_types, "relationship"),
            _ => {
                return Err(storage(
                    "single-document owner must be an entity or relation",
                ));
            }
        };
        match declared.get(name) {
            Some(id) => Ok(Some(*id)),
            None if self.mode == OntologyMode::Strict => Err(GfError::Validation(format!(
                "unknown strict ontology {noun} type {name:?}"
            ))),
            None => Ok(None),
        }
    }
}

/// The semantic authority a construction is pinned to.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstructionAuthority {
    /// A persisted composition and its physical bindings.
    Composition(ConstructionSemanticAuthority),
    /// The one ontology document of a project that has not migrated to a
    /// composition.
    SingleDocument(SingleDocumentAuthority),
}

impl From<ConstructionSemanticAuthority> for ConstructionAuthority {
    fn from(authority: ConstructionSemanticAuthority) -> Self {
        Self::Composition(authority)
    }
}

impl From<SingleDocumentAuthority> for ConstructionAuthority {
    fn from(authority: SingleDocumentAuthority) -> Self {
        Self::SingleDocument(authority)
    }
}

impl ConstructionAuthority {
    /// Validate the authority against itself.
    ///
    /// # Errors
    /// A composition must agree with its bindings.
    pub fn validate(&self) -> Result<(), GfError> {
        match self {
            Self::Composition(authority) => authority.validate(),
            Self::SingleDocument(_) => Ok(()),
        }
    }

    /// The ontology mode the authority was pinned under.
    #[must_use]
    pub fn mode(&self) -> OntologyMode {
        match self {
            Self::Composition(authority) => authority.mode(),
            Self::SingleDocument(authority) => authority.mode(),
        }
    }

    pub(crate) fn digest(&self) -> Result<String, GfError> {
        match self {
            Self::Composition(authority) => authority.digest(),
            Self::SingleDocument(authority) => Ok(authority.digest()),
        }
    }

    pub(crate) fn context(&self) -> Result<Option<CompositionBindingContext>, GfError> {
        match self {
            Self::Composition(authority) => authority.context().map(Some),
            Self::SingleDocument(_) => Ok(None),
        }
    }

    pub(crate) fn bindings(&self) -> Option<&SemanticStorageBindings> {
        match self {
            Self::Composition(authority) => Some(&authority.bindings),
            Self::SingleDocument(_) => None,
        }
    }

    pub(crate) fn single_document(&self) -> Option<&SingleDocumentAuthority> {
        match self {
            Self::Composition(_) => None,
            Self::SingleDocument(authority) => Some(authority),
        }
    }
}
