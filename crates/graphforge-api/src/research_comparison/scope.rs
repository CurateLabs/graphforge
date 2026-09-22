//! Follow only explicit evidence dependencies of already selected assertions.
use crate::{CancellationToken, GfError, GraphForge, branches::fields::Objects};
use graphforge_knowledge::EvidenceSourceKind;
pub(super) fn dependencies(
    graph: &GraphForge,
    selected: &Objects,
    cancel: &CancellationToken,
) -> Result<Objects, GfError> {
    let generation = graph.generation_for_read()?;
    crate::branches::domain_bounds::preflight(&generation)?;
    let mut objects = selected.clone();
    if generation.capability("knowledge")?.is_none() {
        return Ok(objects);
    }
    for link in crate::knowledge::read_evidence_ledger(&generation)?.links {
        cancel.checkpoint()?;
        if !selected.contains(&("assertion".into(), link.assertion_uuid)) {
            continue;
        }
        let kind = match link.source_kind {
            EvidenceSourceKind::Source => "source",
            EvidenceSourceKind::Artifact => "artifact",
            EvidenceSourceKind::GraphNode => "node",
            EvidenceSourceKind::GraphEdge => "edge",
            EvidenceSourceKind::Document | EvidenceSourceKind::Observation => continue,
        };
        objects.insert((kind.into(), link.source_uuid));
        if objects.len() > 40_000 {
            return Err(super::limit());
        }
    }
    for artifact in crate::knowledge::read_artifact_ledger(&generation)?.artifacts {
        cancel.checkpoint()?;
        if objects.contains(&("artifact".into(), artifact.artifact_uuid)) {
            objects.insert(("source".into(), artifact.source_uuid));
            if objects.len() > 40_000 {
                return Err(super::limit());
            }
        }
    }
    Ok(objects)
}
