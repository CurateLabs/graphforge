//! Read-only recognition of the discovery family admitted by Core 9cbb4107.
//! No retired value can be passed to current discovery or item operations.

use super::*;

/// A recognized retired manifest, never current admission or package proof.
///
/// The producing Core version is absent and unknown. This type has no conversion
/// to [`DiscoveryManifest`] and supplies no publisher, refs or lineage proof.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredManifest {
    format: String,
    version: ProtocolVersion,
    repository: RepositoryIdentity,
    default_ref: String,
    resolved_ref: String,
    immutable_version: Sha256Digest,
    package: RetiredPackageReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<ProjectSummaryReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ontology: Option<RetiredOntologyInventory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage: Option<ResearchLineageReference>,
    requirements: Vec<ProtocolRequirement>,
    capabilities: Vec<ProtocolCapability>,
    objects: Vec<ObjectDescriptor>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetiredPackageReference {
    format: String,
    package_digest: Sha256Digest,
    object_digest: Sha256Digest,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetiredModuleDescriptor {
    id: String,
    version: String,
    content_digest: Sha256Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    package: Option<RetiredPackageReference>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetiredOntologyInventory {
    composition_digest: Sha256Digest,
    modules: Vec<RetiredModuleDescriptor>,
    bridge_sets: Vec<BridgeSetDescriptor>,
}

impl RetiredManifest {
    /// Recognize bounded, duplicate-free historical controls. Any inventory
    /// entries are retained only so the claims owner can refuse them; their
    /// identities and packages are deliberately not admitted or resolved.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        check_response_bound(bytes, limits)?;
        let manifest: Self = parse_unique_json(bytes)?;
        validate_header(
            &manifest.format,
            manifest.version,
            &manifest.repository,
            limits,
        )?;
        validate_ref_name(&manifest.default_ref, limits)?;
        validate_ref_name(&manifest.resolved_ref, limits)?;
        manifest.immutable_version.validate()?;
        if manifest.package.format != PORTABLE_FORMAT {
            return Err(malformed());
        }
        manifest.package.package_digest.validate()?;
        manifest.package.object_digest.validate()?;
        validate_semantics(
            &manifest.requirements,
            &manifest.capabilities,
            "portable-v2",
            limits,
        )?;
        validate_extensions(&manifest.extensions, limits)?;
        validate_objects(&manifest.objects, limits)?;
        manifest.selected_object(
            &manifest.package.object_digest,
            PORTABLE_MEDIA_TYPE,
            u64::MAX,
        )?;
        if let Some(summary) = &manifest.summary {
            if summary.format != PROJECT_SUMMARY_FORMAT {
                return Err(malformed());
            }
            summary.summary_digest.validate()?;
            manifest.selected_object(
                &summary.object_digest,
                PROJECT_SUMMARY_MEDIA_TYPE,
                u64::try_from(limits.max_summary_bytes).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(lineage) = &manifest.lineage {
            if lineage.format != "graphforge-research-lineage/1" {
                return Err(malformed());
            }
            lineage.lineage_digest.validate()?;
            manifest.selected_object(
                &lineage.object_digest,
                RESEARCH_LINEAGE_MEDIA_TYPE,
                u64::try_from(limits.max_lineage_bytes).unwrap_or(u64::MAX),
            )?;
        }
        if let Some(inventory) = &manifest.ontology {
            inventory.composition_digest.validate()?;
            if inventory.modules.len() > limits.max_ontology_entries
                || inventory.bridge_sets.len() > limits.max_ontology_entries
            {
                return Err(limit("ontology"));
            }
        }
        Ok(manifest)
    }

    fn selected_object(
        &self,
        digest: &Sha256Digest,
        media_type: &str,
        max_bytes: u64,
    ) -> Result<(), DiscoveryError> {
        digest.validate()?;
        let object = self
            .objects
            .binary_search_by(|object| object.digest.cmp(digest))
            .map(|index| &self.objects[index])
            .map_err(|_| malformed())?;
        if object.media_type != media_type {
            return Err(malformed());
        }
        if object.length > max_bytes {
            return Err(limit("objects.length"));
        }
        Ok(())
    }

    /// Whether any module or bridge-set entry is advertised, regardless of ID
    /// shape, kind, package presence or eligibility under current discovery.
    #[must_use]
    pub fn advertises_inventory(&self) -> bool {
        self.ontology.as_ref().is_some_and(|inventory| {
            !inventory.modules.is_empty() || !inventory.bridge_sets.is_empty()
        })
    }

    /// Original repository identity, without publisher authority.
    #[must_use]
    pub fn repository(&self) -> &RepositoryIdentity {
        &self.repository
    }

    /// Original immutable repository version, never an inferred producer.
    #[must_use]
    pub fn immutable_version(&self) -> &Sha256Digest {
        &self.immutable_version
    }

    /// Canonical historical bytes without injecting a producing Core version.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        canonical_json(self)
    }
}
