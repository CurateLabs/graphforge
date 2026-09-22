use super::super::{
    BTreeMap, BTreeSet, GfError, MAX_CONTEXTS, MAX_RECEIPTS, MAX_REGISTRY_BYTES, RESEARCH_VERSION,
    ResearchAcceptedMapping, ResearchBranchRecord, ResearchRegistry, ResearchVersionRecord, Uuid,
    identity_digest, invalid, json,
};
use serde::{Deserialize, Serialize};

/// Whether exported research is exactly the source Version or a distinct selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchInterchangeSelection {
    /// Every immutable source participant and required object is included.
    Complete {
        /// Exact immutable source research identity.
        source_version_uuid: Uuid,
    },
    /// Selected native content has its own identity; the source remains provenance.
    Projection {
        /// Original immutable research identity, never assigned to the subset.
        source_version_uuid: Uuid,
        /// Commitment to explicit membership, field selection, and redactions.
        selection_sha256: [u8; 32],
    },
}

/// Explicit creation of independent local research authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchForkRecord {
    /// Independent new Project identity.
    pub project_uuid: Uuid,
    /// Stable public Fork operation identity.
    pub operation_uuid: Uuid,
    /// Canonical bounded public Fork request commitment for exact durable replay.
    pub intent_sha256: [u8; 32],
    /// Recorded creator, not authentication.
    pub actor_uuid: Uuid,
    /// Explicit bounded local governance policy.
    pub governance: String,
    /// Explicit decision to adopt the selected native ontology as local authority.
    pub adopt_selected_ontology: bool,
}

/// Closed native manifest carried by the registered portable research component.
/// It records selected provenance without installing foreign Branch heads,
/// proposals, promotion authority, or whole-ancestor retention roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchInterchangeManifest {
    /// Research interchange component schema, independently versioned from transport.
    pub contract_version: u32,
    /// Required native research reader capability.
    pub research_capability_version: u32,
    /// Exact exporting producer contract.
    pub producer: String,
    /// Original research Project identity.
    pub source_project_uuid: Uuid,
    /// Optional independent Project identity created only by explicit Fork.
    pub fork_project_uuid: Option<Uuid>,
    /// Explicit independent governance creation; bytes-only import has none.
    pub fork: Option<ResearchForkRecord>,
    /// Exact selected immutable research content.
    pub selected_version_uuid: Uuid,
    /// Complete-copy or distinct-projection identity semantics.
    pub selection: ResearchInterchangeSelection,
    /// Only selected content and its explicit required Version closure.
    pub versions: BTreeMap<Uuid, ResearchVersionRecord>,
    /// Permanent original identity commitments, including unavailable ancestors.
    pub identities: BTreeMap<Uuid, [u8; 32]>,
    /// Historical Branch records; these are never destination live heads.
    pub genealogy: BTreeMap<Uuid, ResearchBranchRecord>,
    /// Exact selected accepted-contribution mappings, not local acceptance decisions.
    pub accepted: BTreeMap<Uuid, ResearchAcceptedMapping>,
    /// Original proof identity to exported complete or distinctly projected proof.
    /// This does not rewrite the original accepted mapping or frozen dependencies.
    pub proof_exports: BTreeMap<Uuid, Uuid>,
}

impl ResearchInterchangeManifest {
    /// Validate identity, bounds, selected closure, and provenance before admission.
    pub fn validate(&self) -> Result<(), GfError> {
        if self.contract_version != 1
            || self.research_capability_version != RESEARCH_VERSION
            || self.producer
                != concat!(
                    "graphforge-storage/",
                    env!("CARGO_PKG_VERSION"),
                    ";research-interchange/1"
                )
            || self.source_project_uuid.is_nil()
            || self
                .fork_project_uuid
                .is_some_and(|id| id.is_nil() || id == self.source_project_uuid)
            || self.genealogy.len() > MAX_CONTEXTS
            || self.identities.len() > MAX_RECEIPTS
            || self.accepted.len() > 16_384
            || json(self)?.len() > MAX_REGISTRY_BYTES
        {
            return Err(invalid(
                "unsupported or oversized research interchange manifest",
            ));
        }
        match (&self.fork, self.fork_project_uuid) {
            (None, None) => {}
            (Some(fork), Some(id))
                if fork.project_uuid == id
                    && !fork.operation_uuid.is_nil()
                    && !fork.actor_uuid.is_nil()
                    && !fork.governance.trim().is_empty()
                    && fork.governance.len() <= 4096
                    && fork.adopt_selected_ontology => {}
            _ => {
                return Err(invalid(
                    "Fork requires explicit independent governance and ontology decision",
                ));
            }
        }
        let selected = self
            .versions
            .get(&self.selected_version_uuid)
            .ok_or_else(|| invalid("interchange selected Version is unavailable"))?;
        match &self.selection {
            ResearchInterchangeSelection::Complete {
                source_version_uuid,
            } if *source_version_uuid == self.selected_version_uuid => {}
            ResearchInterchangeSelection::Projection {
                source_version_uuid,
                ..
            } if *source_version_uuid != self.selected_version_uuid
                && selected.content.source_version == Some(*source_version_uuid)
                && self.identities.contains_key(source_version_uuid) => {}
            _ => {
                return Err(invalid(
                    "interchange projection cannot impersonate a complete source Version",
                ));
            }
        }
        let registry = ResearchRegistry {
            versions: self.versions.clone(),
            identities: self.identities.clone(),
            materialized: self.versions.keys().copied().collect(),
            ..ResearchRegistry::default()
        };
        registry.validate()?;
        self.validate_content_closure()?;
        self.validate_genealogy()?;
        self.validate_accepted()
    }

    fn validate_content_closure(&self) -> Result<(), GfError> {
        let mut closure = BTreeSet::new();
        let expected_proofs: BTreeSet<_> = self
            .accepted
            .values()
            .map(|mapping| mapping.proof_version_uuid)
            .collect();
        if expected_proofs != self.proof_exports.keys().copied().collect() {
            return Err(invalid(
                "interchange proof roots differ from accepted mappings",
            ));
        }
        let mut pending = vec![self.selected_version_uuid];
        for (original, exported) in &self.proof_exports {
            let proof = self
                .versions
                .get(exported)
                .ok_or_else(|| invalid("exported acceptance proof is unavailable"))?;
            if !self.identities.contains_key(original)
                || (original != exported && proof.content.source_version != Some(*original))
            {
                return Err(invalid(
                    "projected acceptance proof lost its source identity",
                ));
            }
            pending.push(*exported);
        }
        while let Some(id) = pending.pop() {
            if closure.insert(id) {
                pending.extend(self.versions[&id].content.required_versions.iter().copied());
            }
        }
        if closure != self.versions.keys().copied().collect() {
            return Err(invalid(
                "research package includes Versions outside selected dependency closure",
            ));
        }
        for version in self.versions.values() {
            if self.identities.get(&version.version_uuid) != Some(&identity_digest(version)?) {
                return Err(invalid(
                    "interchange immutable identity conflicts with content",
                ));
            }
            let supported_producer = version
                .content
                .producer
                .rsplit_once(";research/")
                .filter(|(name, _)| name.starts_with("graphforge-storage/"))
                .and_then(|(_, version)| version.parse::<u32>().ok())
                .is_some_and(|version| (1..=RESEARCH_VERSION).contains(&version));
            if !supported_producer {
                return Err(invalid("unsupported research Version producer"));
            }
        }
        Ok(())
    }

    fn validate_accepted(&self) -> Result<(), GfError> {
        for (id, mapping) in &self.accepted {
            if *id != mapping.mapping_uuid
                || mapping.identity()? != *id
                || mapping.operation_uuid.is_nil()
                || mapping.item_uuid.is_nil()
                || mapping.contribution_uuid.is_nil()
                || mapping.unit.object_uuid.is_nil()
                || mapping.unit.object_kind.is_empty()
                || mapping.unit.object_kind.len() > 64
                || mapping.unit.field.is_empty()
                || mapping.unit.field.len() > 4096
                || !self.genealogy.contains_key(&mapping.source_branch_uuid)
                || !self.identities.contains_key(&mapping.source_version_uuid)
                || !self
                    .identities
                    .contains_key(&mapping.destination_version_uuid)
                || !self.identities.contains_key(&mapping.proof_version_uuid)
            {
                return Err(invalid("invalid imported accepted-contribution provenance"));
            }
            let branch = &self.genealogy[&mapping.source_branch_uuid];
            let destination = match branch.parent_branch_uuid {
                Some(branch_uuid) => {
                    super::super::ResearchProposalDestination::Branch { branch_uuid }
                }
                None => super::super::ResearchProposalDestination::Project {
                    project_uuid: branch.project_uuid,
                },
            };
            if destination != mapping.destination {
                return Err(invalid(
                    "imported acceptance destination differs from genealogy",
                ));
            }
        }
        Ok(())
    }

    fn validate_genealogy(&self) -> Result<(), GfError> {
        for (id, branch) in &self.genealogy {
            if *id != branch.branch_uuid
                || id.is_nil()
                || branch.creator_uuid.is_nil()
                || branch.project_uuid != self.source_project_uuid
                || branch.label.is_empty()
                || branch.label.len() > 4096
                || !self.identities.contains_key(&branch.base_version_uuid)
                || !self.identities.contains_key(&branch.origin_version_uuid)
            {
                return Err(invalid("invalid imported Branch genealogy"));
            }
            let mut seen = BTreeSet::from([*id]);
            let mut next = branch.parent_branch_uuid;
            while let Some(parent) = next {
                if !seen.insert(parent) {
                    return Err(invalid("cyclic imported Branch genealogy"));
                }
                next = self
                    .genealogy
                    .get(&parent)
                    .ok_or_else(|| invalid("imported Branch ancestor metadata is unavailable"))?
                    .parent_branch_uuid;
            }
        }
        Ok(())
    }
}

impl ResearchVersionRecord {
    /// Canonical immutable commitment; physical placement is not content identity.
    pub fn identity_sha256(&self) -> Result<[u8; 32], GfError> {
        identity_digest(self)
    }
}
