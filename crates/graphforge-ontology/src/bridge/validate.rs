//! Bridge-set document validation (endpoints, kinds, conflicts, provenance).

use std::collections::{HashMap, HashSet};

use unicode_normalization::UnicodeNormalization;

use crate::composition::{
    CompositionDiagnostic, CompositionError, DiagnosticCode, DiagnosticLimit, OntologyModuleId,
    QualifiedSymbol, SymbolKind, bridge_document_digest,
};

use super::store::ModuleSymbolTable;
use super::types::{BridgeAssertion, BridgeDocument, BridgePredicate, MappingMethod};

/// Validate a bridge document against known module symbol tables.
pub fn validate_bridge_document(
    doc: &BridgeDocument,
    modules: &[ModuleSymbolTable],
    limit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    require_nfc(&doc.bridge_id, "bridge_id", limit)?;
    require_nfc(&doc.authored_version, "authored_version", limit)?;
    if doc.assertions.is_empty() {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InventoryMalformed,
            "bridge document must contain at least one assertion",
            vec![doc.bridge_id.clone()],
            limit,
        )));
    }

    let by_id: HashMap<String, &ModuleSymbolTable> =
        modules.iter().map(|m| (m.id.display_ref(), m)).collect();

    for module in doc.source_modules.iter().chain(doc.target_modules.iter()) {
        if !by_id.contains_key(&module.display_ref()) {
            return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
                DiagnosticCode::DependencyMissing,
                "bridge source/target module constraint missing from known inventory",
                vec![module.display_ref()],
                limit,
            )));
        }
    }

    let constraint_ok = |module: &OntologyModuleId| {
        doc.source_modules.iter().any(|m| m == module)
            || doc.target_modules.iter().any(|m| m == module)
    };

    for assertion in &doc.assertions {
        validate_assertion(assertion, &by_id, &constraint_ok, limit)?;
    }

    detect_contradictions(&doc.assertions, limit)?;

    // Digest must be computable (canonicalization / serde).
    let _ = bridge_document_digest(doc).map_err(|e| {
        CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InterchangeIntegrity,
            format!("failed to digest bridge document: {e}"),
            vec![doc.bridge_id.clone()],
            limit,
        ))
    })?;

    Ok(())
}

fn validate_assertion(
    assertion: &BridgeAssertion,
    by_id: &HashMap<String, &ModuleSymbolTable>,
    constraint_ok: &dyn Fn(&OntologyModuleId) -> bool,
    limit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    require_nfc(&assertion.source.local_id, "source.local_id", limit)?;
    require_nfc(&assertion.target.local_id, "target.local_id", limit)?;

    if !constraint_ok(&assertion.source.module) || !constraint_ok(&assertion.target.module) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::BridgeEndpointMissing,
            "assertion endpoint module is outside bridge source/target constraints",
            vec![assertion.source.display(), assertion.target.display()],
            limit,
        )));
    }

    resolve_endpoint(&assertion.source, by_id, limit)?;
    resolve_endpoint(&assertion.target, by_id, limit)?;

    if !kinds_compatible(
        assertion.predicate,
        assertion.source.kind,
        assertion.target.kind,
    ) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::ResolutionKindMismatch,
            format!(
                "predicate {} rejects kind pair {} -> {}",
                assertion.predicate.as_str(),
                assertion.source.kind.as_str(),
                assertion.target.kind.as_str()
            ),
            vec![assertion.source.display(), assertion.target.display()],
            limit,
        )));
    }

    // Authoritative mappings require justification + provenance method authored.
    if assertion.provenance.method == MappingMethod::Authored
        && assertion.provenance.justification.trim().is_empty()
    {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::BridgeProvenanceMissing,
            "authored mapping requires a non-empty justification",
            vec![assertion.source.display(), assertion.target.display()],
            limit,
        )));
    }

    if let Some(confidence) = assertion.provenance.confidence
        && !(0.0..=1.0).contains(&confidence.value)
    {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::InventoryMalformed,
            "mapping confidence must be in [0.0, 1.0]",
            vec![assertion.source.display(), assertion.target.display()],
            limit,
        )));
    }

    Ok(())
}

fn resolve_endpoint(
    symbol: &QualifiedSymbol,
    by_id: &HashMap<String, &ModuleSymbolTable>,
    limit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    let Some(table) = by_id.get(&symbol.module.display_ref()) else {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::BridgeEndpointMissing,
            "bridge endpoint module is absent from the known inventory",
            vec![symbol.display()],
            limit,
        )));
    };
    if !table.contains(symbol.kind, &symbol.local_id) {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::BridgeEndpointMissing,
            "bridge endpoint symbol is missing from its module",
            vec![symbol.display()],
            limit,
        )));
    }
    Ok(())
}

fn kinds_compatible(predicate: BridgePredicate, source: SymbolKind, target: SymbolKind) -> bool {
    match predicate {
        BridgePredicate::Equivalent
        | BridgePredicate::Related
        | BridgePredicate::Broader
        | BridgePredicate::Narrower
        | BridgePredicate::Disjoint => {
            source == target
                && matches!(
                    source,
                    SymbolKind::Entity | SymbolKind::Relation | SymbolKind::Property
                )
        }
        BridgePredicate::MapsTo => {
            source == target && matches!(source, SymbolKind::Property | SymbolKind::Relation)
        }
        BridgePredicate::EvidenceFor => {
            // Typically entity/claim → evidence entity; allow entity→entity.
            source == SymbolKind::Entity && target == SymbolKind::Entity
        }
    }
}

/// Detect contradictory assertions; return a minimal attributable conflict set.
fn detect_contradictions(
    assertions: &[BridgeAssertion],
    limit: DiagnosticLimit,
) -> Result<(), CompositionError> {
    // Index by unordered endpoint pair + kind for symmetric conflict checks.
    let mut by_pair: HashMap<(String, String), Vec<&BridgeAssertion>> = HashMap::new();
    for assertion in assertions {
        let a = assertion.source.display();
        let b = assertion.target.display();
        let key = if a <= b { (a, b) } else { (b, a) };
        by_pair.entry(key).or_default().push(assertion);
    }

    for ((_a, _b), group) in &by_pair {
        let predicates: HashSet<BridgePredicate> = group.iter().map(|a| a.predicate).collect();
        if predicates.contains(&BridgePredicate::Equivalent)
            && predicates.contains(&BridgePredicate::Disjoint)
        {
            let subjects: Vec<String> = group
                .iter()
                .flat_map(|a| [a.source.display(), a.target.display()])
                .collect();
            return Err(CompositionError::one(CompositionDiagnostic::new(
                DiagnosticCode::BridgeContradiction,
                "equivalent and disjoint assertions conflict for the same endpoints",
                subjects,
                group
                    .iter()
                    .map(|a| a.predicate.as_str().to_owned())
                    .collect(),
                limit,
            )));
        }
        // Broader both ways without a coherent inverse is incoherent.
        let mut directed: HashSet<(String, BridgePredicate, String)> = HashSet::new();
        for a in group {
            directed.insert((a.source.display(), a.predicate, a.target.display()));
        }
        for a in group {
            if a.predicate == BridgePredicate::Broader {
                let reverse = (
                    a.target.display(),
                    BridgePredicate::Broader,
                    a.source.display(),
                );
                let conflicting_narrower = (
                    a.source.display(),
                    BridgePredicate::Narrower,
                    a.target.display(),
                );
                if directed.contains(&reverse) || directed.contains(&conflicting_narrower) {
                    return Err(CompositionError::one(CompositionDiagnostic::new(
                        DiagnosticCode::BridgeContradiction,
                        "broader/narrower assertions form an incoherent pair",
                        vec![a.source.display(), a.target.display()],
                        vec![
                            BridgePredicate::Broader.as_str().to_owned(),
                            BridgePredicate::Narrower.as_str().to_owned(),
                        ],
                        limit,
                    )));
                }
            }
        }
    }
    Ok(())
}

fn require_nfc(value: &str, field: &str, limit: DiagnosticLimit) -> Result<(), CompositionError> {
    let nfc: String = value.nfc().collect();
    if nfc != value {
        return Err(CompositionError::one(CompositionDiagnostic::with_subjects(
            DiagnosticCode::CollisionMetadata,
            format!("{field} must be NFC-normalized"),
            vec![value.to_owned()],
            limit,
        )));
    }
    Ok(())
}
