//! Effective preference is a semantic value; immutable events retain its history.
use super::fields::{Fields, Objects, insert};
use crate::{CancellationToken, GfError, GraphForge};
use sha2::{Digest, Sha256};

pub(super) fn read(
    graph: &GraphForge,
    fields: &mut Fields,
    bytes: &mut usize,
    selected: Option<&Objects>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let generation = graph.generation_for_read()?;
    super::domain_bounds::preflight(&generation)?;
    if generation.capability("knowledge")?.is_none() {
        return Ok(());
    }
    let ledger = crate::knowledge::ledger::read_preference_ledger(&generation)?;
    for source in crate::knowledge::read_source_ledger(&generation)?.sources {
        cancellation.checkpoint()?;
        if selected.is_some_and(|set| !set.contains(&("source".into(), source.source_uuid))) {
            continue;
        }
        let preferred = ledger.current_preferred_artifact(source.source_uuid);
        let mut hash = Sha256::new();
        hash.update(b"graphforge-source-preferred-artifact/1");
        hash.update([u8::from(preferred.is_some())]);
        if let Some(id) = preferred {
            hash.update(id.as_bytes());
        }
        insert(
            fields,
            bytes,
            (
                "source".into(),
                source.source_uuid,
                "$preferred_artifact".into(),
            ),
            hash.finalize().into(),
        )?;
    }
    Ok(())
}
