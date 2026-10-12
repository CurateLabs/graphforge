//! The per-relationship predicate of a reachability-mode expansion (#1984).
//!
//! The planner lowers `ALL(e IN r WHERE p)` to a DataFusion expression over one
//! synthetic struct column whose fields are exactly the properties `p` reads
//! (see [`graphforge_plan::EdgeFilter`]). This module owns the physical form of
//! that expression and evaluates it for a batch of edges at once, so the
//! traversal never interprets a predicate itself: the same expression
//! evaluator that runs every other Cypher predicate decides which edges a
//! level may cross.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StructArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use graphforge_core::GfError;
use graphforge_plan::VAR_LEN_EDGE_ELEMENT;

use super::reachability::settle;
use super::{
    ExpandConfig, MemoryReservation, adjacency, build_edge_records, charge, edge_prop_children,
    edge_record_table_bytes, hash_table_bytes, load_edge_prop_source, read_traversed_edges,
};

/// The most distinct relationships whose properties one call reads: a call
/// takes frontier nodes until their relationships reach this many (a single
/// node's relationships are never split). Every call reads the whole property
/// index manifest, so a level is read in one call unless it is larger than this;
/// the bound keeps what a level holds proportional to the work done before an
/// early stop ends the search.
const BATCH_EDGES: usize = 1 << 16;

/// What a search can still use of a relationship, decided from the state at the
/// start of a call. A relationship the search cannot use is not evaluated.
pub(super) struct Wanted<'a> {
    /// Nodes already reached: a relationship to one of them changes nothing.
    pub(super) visited: &'a HashSet<u64>,
    pub(super) seed: u64,
    /// A relationship back to the seed matters only while the seed itself is
    /// still being looked for.
    pub(super) seed_pending: bool,
    /// The seed's cycle search also counts relationships between nodes already
    /// reached, so none can be left out.
    pub(super) every: bool,
}

impl Wanted<'_> {
    fn needs(&self, neighbor: u64) -> bool {
        self.every
            || if neighbor == self.seed {
                self.seed_pending
            } else {
                !self.visited.contains(&neighbor)
            }
    }
}

/// A planned edge predicate and the struct shape it reads.
pub(crate) struct PhysicalEdgeFilter {
    predicate: Arc<dyn PhysicalExpr>,
    /// The struct fields, in the order the predicate's `get_field` calls were
    /// planned against: one per property the predicate reads.
    fields: Vec<FieldRef>,
    /// The schema of the batch the predicate is evaluated over.
    schema: SchemaRef,
}

impl PhysicalEdgeFilter {
    /// Wrap a planned `predicate` over the struct made of `fields`.
    pub(super) fn new(predicate: Arc<dyn PhysicalExpr>, fields: &[FieldRef]) -> Self {
        let element = Field::new(
            VAR_LEN_EDGE_ELEMENT,
            DataType::Struct(Fields::from(fields.to_vec())),
            true,
        );
        Self {
            predicate,
            fields: fields.to_vec(),
            schema: Arc::new(Schema::new(vec![element])),
        }
    }

    /// The predicate, for plan display.
    pub(super) fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The relationships of `nodes`, taken in order, that the search may cross.
    /// Returns them (by `edge_id`) with the number of `nodes` that were taken.
    ///
    /// The properties of every relationship in the batch the search can use are
    /// read once, and the predicate is evaluated for them together. At least one
    /// node is taken. Everything held while reading is charged to `reservation`
    /// and released before this returns.
    pub(super) fn crossable(
        &self,
        cfg: &ExpandConfig,
        adjacency: &mut adjacency::AdjacencyReader<'_>,
        nodes: &[u64],
        wanted: &Wanted<'_>,
        reservation: &mut MemoryReservation,
    ) -> Result<(HashSet<u64>, usize), GfError> {
        let exec_err = |m: String| GfError::Execution(format!("VarLenExpand edge filter: {m}"));
        // Charged for the whole call, released before it returns.
        let mut temporary = 0_usize;

        let mut candidates: HashSet<u64> = HashSet::new();
        let mut taken = 0_usize;
        for &node in nodes {
            adjacency.with_neighbors(node, |neighbors| {
                for (edge, neighbor) in neighbors.iter() {
                    if wanted.needs(neighbor) {
                        candidates.insert(edge);
                    }
                }
            })?;
            taken += 1;
            settle(
                reservation,
                &mut temporary,
                hash_table_bytes(candidates.capacity(), std::mem::size_of::<u64>()),
            )?;
            if candidates.len() >= BATCH_EDGES {
                break;
            }
        }
        if candidates.is_empty() {
            reservation.shrink(temporary);
            return Ok((candidates, taken));
        }

        let ids: Vec<u64> = candidates.iter().copied().collect();
        charge(reservation, &mut temporary, 8 * ids.capacity())?;
        let children = if self.fields.is_empty() {
            // The predicate reads no property: nothing to look up.
            Vec::new()
        } else {
            // The relationships' public identities, which key their properties.
            let edge_batches = read_traversed_edges(cfg, &candidates)?;
            charge(
                reservation,
                &mut temporary,
                edge_batches
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .sum(),
            )?;
            let records = build_edge_records(cfg, &edge_batches)?;
            charge(
                reservation,
                &mut temporary,
                edge_record_table_bytes(&records),
            )?;
            drop(edge_batches);
            let mut uuids = Vec::with_capacity(ids.len());
            for id in &ids {
                let record = records
                    .get(id)
                    .ok_or_else(|| exec_err(format!("no record for edge_id {id}")))?;
                uuids.push(record.edge_uuid);
            }
            drop(records);
            charge(reservation, &mut temporary, 16 * uuids.capacity())?;
            let targets: BTreeSet<[u8; 16]> = uuids.iter().copied().collect();
            // `Absent` and `NotIndexed` fall back to the stored properties
            // inside this call, exactly as the relationship list does.
            let source = load_edge_prop_source(
                cfg.provider.admitted_inventory().as_deref(),
                &cfg.rel_type_name,
                &cfg.dir,
                &self.fields,
                &targets,
            )?;
            charge(reservation, &mut temporary, source.charged_bytes())?;
            let children = edge_prop_children(&source, &self.fields, &uuids)?;
            charge(
                reservation,
                &mut temporary,
                children.iter().map(|c| c.get_array_memory_size()).sum(),
            )?;
            children
        };
        let verdicts = self.passing(children, ids.len())?;
        let crossable: HashSet<u64> = ids
            .iter()
            .zip(verdicts)
            .filter_map(|(&id, pass)| pass.then_some(id))
            .collect();
        reservation.shrink(temporary);
        Ok((crossable, taken))
    }

    /// Evaluate the predicate for `rows` edges whose property values are
    /// `children`, one array per [`fields`](Self::fields) entry in order.
    ///
    /// `true` means the edge may be crossed. `false` and `NULL` both mean it
    /// may not: `ALL` keeps a relationship list only when every element's
    /// predicate is `true`, so a `NULL` element excludes the list exactly as a
    /// `false` one does.
    pub(super) fn passing(
        &self,
        children: Vec<ArrayRef>,
        rows: usize,
    ) -> Result<Vec<bool>, GfError> {
        let exec_err = |m: String| GfError::Execution(format!("VarLenExpand edge filter: {m}"));
        if children.len() != self.fields.len() {
            return Err(exec_err(format!(
                "{} property columns for {} fields",
                children.len(),
                self.fields.len()
            )));
        }
        let mut typed = Vec::with_capacity(children.len());
        for (field, child) in self.fields.iter().zip(children) {
            if child.len() != rows {
                return Err(exec_err(format!(
                    "property {} has {} rows for {rows} edges",
                    field.name(),
                    child.len()
                )));
            }
            typed.push(declared_type(field, child, rows)?);
        }
        let element: ArrayRef = if self.fields.is_empty() {
            Arc::new(StructArray::new_empty_fields(rows, None))
        } else {
            Arc::new(
                StructArray::try_new(Fields::from(self.fields.clone()), typed, None)
                    .map_err(|e| exec_err(e.to_string()))?,
            )
        };
        let batch = RecordBatch::try_new(self.schema.clone(), vec![element])
            .map_err(|e| exec_err(e.to_string()))?;
        let verdict = self
            .predicate
            .evaluate(&batch)
            .and_then(|value| value.into_array(rows))
            .map_err(|e| exec_err(e.to_string()))?;
        let verdict = verdict
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| exec_err(format!("predicate yields {}", verdict.data_type())))?;
        Ok((0..rows)
            .map(|row| verdict.is_valid(row) && verdict.value(row))
            .collect())
    }
}

/// `child` as the field's declared type.
///
/// A property the schema declares but no relation file stores is a `Null`
/// field; its values are all `NULL`. Any other type difference is a widening
/// the union schema made over several relation files, and a cast that cannot
/// be made is an error rather than a silent `NULL`.
fn declared_type(field: &FieldRef, child: ArrayRef, rows: usize) -> Result<ArrayRef, GfError> {
    if child.data_type() == field.data_type() {
        return Ok(child);
    }
    if field.data_type() == &DataType::Null {
        return Ok(new_null_array(&DataType::Null, rows));
    }
    cast(&child, field.data_type()).map_err(|e| {
        GfError::Execution(format!(
            "VarLenExpand edge filter: property {} is {} but is declared {}: {e}",
            field.name(),
            child.data_type(),
            field.data_type()
        ))
    })
}
