//! Shared mutation accounting. Storage staging and publication consume the same
//! state for statement and analyst mutations.

use std::collections::{BTreeMap, HashSet};

/// The openCypher write counters a statement reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteCounters {
    pub nodes_created: u64,
    pub edges_created: u64,
    pub nodes_deleted: u64,
    pub edges_deleted: u64,
    pub properties_set: u64,
    pub properties_removed: u64,
    pub labels_added: u64,
    pub labels_removed: u64,
}

/// Deduplicated effects and property counters for one mutation boundary.
#[derive(Default)]
pub(crate) struct MutationState {
    pub counters: WriteCounters,
    pub property_sets: HashSet<(bool, [u8; 16], String)>,
    pub effects: BTreeMap<
        crate::MutationKind,
        (
            HashSet<crate::MutationSubject>,
            HashSet<crate::MutationSubject>,
        ),
    >,
}

impl MutationState {
    pub(crate) fn record_mutation_input(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.effects
            .entry(kind)
            .or_default()
            .0
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    pub(crate) fn record_mutation_output(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.effects
            .entry(kind)
            .or_default()
            .1
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    pub(crate) fn mutation_receipt(&self) -> crate::MutationReceipt {
        crate::MutationReceipt::from_accumulators(self.effects.clone())
    }

    pub(crate) fn record_property_set(
        &mut self,
        is_edge: bool,
        uuid: [u8; 16],
        name: &str,
    ) -> bool {
        if self.property_sets.insert((is_edge, uuid, name.to_owned())) {
            self.counters.properties_set += 1;
            true
        } else {
            false
        }
    }
}

impl MutationState {
    /// Commit a statement's staged files with the existing topology index and
    /// adjacency-delta participant. Property-only commits do not open a writer.
    pub(crate) fn commit_topology(
        &mut self,
        staged: graphforge_storage::RewriteBatch,
        writer: &mut graphforge_storage::GraphWriter,
        dir: &std::path::Path,
        deleted_node_ids: &HashSet<[u8; 16]>,
        deleted_edge_ids: &HashSet<[u8; 16]>,
    ) -> Result<(), graphforge_core::GfError> {
        use graphforge_core::uuid::Uuid;
        // Adjacency delta segment (#765): a statement is pure-append iff it stages
        // no deletes (SET/REMOVE never touch topology). Pure-append → record the
        // created edges so the index serves them without a rebuild; otherwise the
        // statement breaks the chain at this generation (a DELETE invalidates the
        // incremental path), so write no segment and clear any stale file there.
        let pure_append = deleted_node_ids.is_empty() && deleted_edge_ids.is_empty();
        let pending = writer.take_pending_delta();
        let deleted_nodes = deleted_node_ids
            .iter()
            .copied()
            .map(Uuid::from_bytes)
            .collect::<Vec<_>>();
        let deleted_edges = deleted_edge_ids
            .iter()
            .copied()
            .map(Uuid::from_bytes)
            .collect::<Vec<_>>();
        if let Some(generation) =
            writer.commit_topology_aware_with_uuid_index(staged, deleted_nodes, deleted_edges)?
        {
            if pure_append {
                writer.write_segment_best_effort(generation, &pending);
            } else {
                graphforge_storage::adjacency_delta::discard_segment(dir, generation);
            }
        }
        Ok(())
    }
}
