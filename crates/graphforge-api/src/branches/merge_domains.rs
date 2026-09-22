//! Combine selected immutable owner ledgers with exact replay/conflict validation.
use crate::{
    GfError, GraphForge,
    knowledge::{knowledge_error, ledger as k},
};
use graphforge_storage::ProjectParticipant;

pub(super) fn merge(
    destination: &GraphForge,
    source: &GraphForge,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let dst = destination.generation_for_read()?;
    let src = source.generation_for_read()?;
    super::domain_bounds::preflight(&dst)?;
    super::domain_bounds::preflight(&src)?;
    let mut out = Vec::new();
    macro_rules! ledgers {
        ($cap:literal, $owner:path, $error:ident; $( $read:ident => $encode:ident ),+ $(,)?) => {{
            use $owner as owner;
            if src.capability($cap)?.is_some() {
                $(
                    let incoming = owner::$read(&src)?;
                    let existing = if dst.capability($cap)?.is_some() { owner::$read(&dst)? } else { Default::default() };
                    out.extend(owner::$encode(&existing.merge(&incoming).map_err($error)?)?);
                )+
            }
        }};
    }
    ledgers!("knowledge", k, knowledge_error;
        read_ledger => encode_ledger,
        read_source_ledger => encode_source_ledger,
        read_artifact_ledger => encode_artifact_ledger,
        read_evidence_ledger => encode_evidence_ledger,
        read_derivation_ledger => encode_derivation_ledger,
        read_confidence_ledger => encode_confidence_ledger,
        read_preference_ledger => encode_preference_ledger,
        read_retention_ledger => encode_retention_ledger,
    );
    ledgers!("knowledge", crate::algorithm_runs, knowledge_error; read_ledger => encode_ledger);
    ledgers!("epistemic", k, knowledge_error;
        read_reasoning_ledger => encode_reasoning_ledger,
        read_status_ledger => encode_status_ledger,
        read_supersession_ledger => encode_supersession_ledger,
    );
    ledgers!("valid_time", crate::valid_time, knowledge_error; read_ledger => encode_ledger);
    ledgers!("epistemic", crate::research_claims::ledger, knowledge_error;
        read_claims => encode_claims,
        read_suppressions => encode_suppressions,
    );
    if src.capability("provenance")?.is_some() {
        let incoming = crate::provenance::read_ledger(&src)?;
        let existing = if dst.capability("provenance")?.is_some() {
            crate::provenance::read_ledger(&dst)?
        } else {
            graphforge_provenance::ProvenanceLedger::default()
        };
        out.extend(crate::provenance::encode_ledger(
            &crate::provenance::union_selected(&existing, &incoming)?,
        )?);
    }

    Ok(out)
}
