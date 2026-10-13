//! Reachability mode of the variable-length expansion: one row per node reached
//! from each seed, with its shortest hop count, instead of one row per trail.
//!
//! The lowerer selects this mode only when the query cannot tell the two apart
//! (`graphforge_ir::reachability_expansions` lists the proof conditions); the
//! trail enumeration stays in the parent module.

use super::Arc;
use super::Array;
use super::ChunkedReservation;
use super::DataFusionError;
use super::Direction;
use super::EXPAND_BATCH_MAX_ROWS;
use super::ExpandConfig;
use super::GfError;
use super::RecordBatch;
use super::SendableRecordBatchStream;
use super::StreamExt;
use super::UInt32Array;
use super::UInt64Array;
use super::adjacency;
use super::charge;
use super::demand;
use super::edge_filter::Wanted;
use super::grow_reservation;
use super::hash_table_bytes;
use super::output_batch_rows;
use super::send_chunk;
use super::topology_for_provider;
use super::u64_column;

/// One node reached by a [`VarLenMode::Reachability`] search:
/// `(input_row, node_id, hops)`.
type Reached = (usize, u64, u64);

/// Charged bytes of one reached node waiting in a batch: its slot plus up to one
/// slot of doubling slack.
const REACHED_BYTES: usize = 2 * std::mem::size_of::<Reached>();

/// Run a [`VarLenMode::Reachability`] expansion and send one row per reached
/// node, with its shortest hop count, as bounded batches.
///
/// Output columns = the input (source) columns, the destination node's
/// `TOPOLOGY_NODES_SCHEMA` columns, then the `hops` column. The lowerer selects
/// this mode only when the query's result is the same as under trail
/// enumeration (`graphforge_ir::reachability_expansions` lists the proof
/// conditions), so the rows are those of [`expand_bfs`] with duplicates removed
/// and the shortest trail's length standing in for the trail.
///
/// Adjacent input rows with the same seed share one search. When the
/// destination is bound (`bound_dst_col_idx`), the search for a seed stops as
/// soon as every destination its rows require has been reached.
///
/// What is held between batches is charged to `reservation` (see
/// [`VAR_LEN_CONSUMER`]): one input batch, one search and one output batch.
pub(super) async fn reach_bfs(
    cfg: &ExpandConfig,
    mut input: SendableRecordBatchStream,
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    reservation: &mut ChunkedReservation,
) -> Result<(), GfError> {
    // The shortest trail is the shortest path only for `min_hops <= 1` (see the
    // proof conditions); the planner never selects the mode for more.
    if cfg.min_hops > 1 {
        return Err(GfError::Execution(format!(
            "reachability expansion needs min_hops <= 1, got {}",
            cfg.min_hops
        )));
    }
    let mut adjacency = {
        let _capture = cfg.lifecycle.attach();
        adjacency::AdjacencyReader::for_capture(
            cfg.provider.as_ref(),
            &cfg.rel_type_name,
            cfg.direction,
            cfg.capture_epoch,
        )?
    };

    let mut output = ReachOutput {
        cfg,
        tx,
        pending: Vec::new(),
        max_rows: EXPAND_BATCH_MAX_ROWS,
        sent_any: false,
    };
    while let Some(batch) = input.next().await {
        let input = batch.map_err(GfError::from_execution_error)?;
        let input_bytes = input.get_array_memory_size();
        grow_reservation(reservation, input_bytes)?;
        if cfg.src_col_idx >= input.num_columns() {
            return Err(GfError::Execution(format!(
                "VarLenExpand source column index {} out of range ({} input columns)",
                cfg.src_col_idx,
                input.num_columns()
            )));
        }
        if cfg.capture_epoch != 0 {
            demand::record_input(cfg.capture_epoch, cfg.edge_var, input.num_rows());
        }
        let src_ids = u64_column(&input, cfg.src_col_idx)?;
        let bound_dst = cfg
            .bound_dst_col_idx
            .map(|index| u64_column(&input, index))
            .transpose()?;
        output.max_rows = output_batch_rows(&input);

        let mut row = 0;
        while row < input.num_rows() {
            if src_ids.is_null(row) {
                row += 1;
                continue;
            }
            let seed = src_ids.value(row);
            let (end, rows) = seed_run(src_ids, bound_dst, row, seed);
            row = end;
            if rows.is_empty() {
                continue;
            }
            let targets: Option<std::collections::HashSet<u64>> = bound_dst
                .is_some()
                .then(|| rows.iter().filter_map(|(_, target)| *target).collect());
            let reached =
                reach_from_seed(cfg, &mut adjacency, seed, targets.as_ref(), reservation)?;
            if !output
                .push_run(&input, &rows, &reached, targets.is_some(), reservation)
                .await?
            {
                return Ok(());
            }
        }
        if !output.flush(&input, reservation).await? {
            return Ok(());
        }
        reservation.shrink(input_bytes);
    }
    // No reached nodes → one empty output under the planned schema. A closed
    // receiver only means the consumer is gone.
    if !output.sent_any {
        let _ = tx
            .send(Ok(RecordBatch::new_empty(cfg.out_schema.clone())))
            .await;
    }
    Ok(())
}

/// The run of input rows starting at `row` that share the seed `seed`: where it
/// ends, and each row with the destination it requires (`None` when any node
/// reached will do).
///
/// A bound destination equates the destination with an input node: a NULL there
/// matches nothing, so the row is dropped; otherwise only that node counts.
fn seed_run(
    src_ids: &UInt64Array,
    bound_dst: Option<&UInt64Array>,
    row: usize,
    seed: u64,
) -> (usize, Vec<(usize, Option<u64>)>) {
    let mut end = row + 1;
    while end < src_ids.len() && src_ids.is_valid(end) && src_ids.value(end) == seed {
        end += 1;
    }
    let rows = (row..end)
        .filter_map(|r| match bound_dst {
            None => Some((r, None)),
            Some(ids) if ids.is_null(r) => None,
            Some(ids) => Some((r, Some(ids.value(r)))),
        })
        .collect();
    (end, rows)
}

/// The reached nodes of the input batch being expanded, waiting to be
/// materialised into an output batch of at most `max_rows` rows.
struct ReachOutput<'a> {
    cfg: &'a ExpandConfig,
    tx: &'a tokio::sync::mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    pending: Vec<Reached>,
    max_rows: usize,
    sent_any: bool,
}

impl ReachOutput<'_> {
    /// Queue `reached`, sending a batch once `max_rows` are waiting. `false`
    /// once the consumer is gone.
    async fn push(
        &mut self,
        input: &RecordBatch,
        reached: Reached,
        reservation: &mut ChunkedReservation,
    ) -> Result<bool, GfError> {
        grow_reservation(reservation, REACHED_BYTES)?;
        self.pending.push(reached);
        if self.pending.len() >= self.max_rows {
            return self.flush(input, reservation).await;
        }
        Ok(true)
    }

    /// Queue what one search reached for every input row of its run: the one
    /// destination a row requires when `required`, otherwise every node reached.
    /// The search's charge is released once its rows are queued. `false` once
    /// the consumer is gone.
    async fn push_run(
        &mut self,
        input: &RecordBatch,
        rows: &[(usize, Option<u64>)],
        reached: &ReachSet,
        required: bool,
        reservation: &mut ChunkedReservation,
    ) -> Result<bool, GfError> {
        let hops_of: std::collections::HashMap<u64, u64> = if required {
            reached.found.iter().copied().collect()
        } else {
            std::collections::HashMap::new()
        };
        for &(input_row, target) in rows {
            if let Some(target) = target {
                if let Some(&hops) = hops_of.get(&target)
                    && !self
                        .push(input, (input_row, target, hops), reservation)
                        .await?
                {
                    return Ok(false);
                }
            } else {
                for &(node, hops) in &reached.found {
                    if !self
                        .push(input, (input_row, node, hops), reservation)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
        }
        reservation.shrink(reached.charged);
        Ok(true)
    }

    /// Materialise and send what is waiting, which came from `input`. `false`
    /// once the consumer is gone.
    async fn flush(
        &mut self,
        input: &RecordBatch,
        reservation: &mut ChunkedReservation,
    ) -> Result<bool, GfError> {
        if self.pending.is_empty() {
            return Ok(true);
        }
        let out = materialise_reached(self.cfg, input, &self.pending, reservation)?;
        reservation.shrink(self.pending.len().saturating_mul(REACHED_BYTES));
        self.pending.clear();
        self.sent_any = true;
        Ok(send_chunk(self.cfg, self.tx, out, reservation).await)
    }
}

/// The nodes one search reached, `(node_id, hops)` in discovery order, and the
/// bytes they are charged for until the caller releases them.
struct ReachSet {
    found: Vec<(u64, u64)>,
    charged: usize,
}

/// Move the reservation from `held` to `now` bytes.
pub(super) fn settle(
    reservation: &mut ChunkedReservation,
    held: &mut usize,
    now: usize,
) -> Result<(), GfError> {
    if now > *held {
        grow_reservation(reservation, now - *held)?;
    } else {
        reservation.shrink(*held - now);
    }
    *held = now;
    Ok(())
}

/// The nodes a search is asked for and the ones it has found so far.
struct Hits<'a> {
    /// `None` wants every node reached.
    targets: Option<&'a std::collections::HashSet<u64>>,
    /// Targets not yet found; `None` when every node is wanted.
    remaining: Option<usize>,
    /// `(node_id, hops)` in discovery order.
    found: Vec<(u64, u64)>,
}

impl Hits<'_> {
    fn wants(&self, node: u64) -> bool {
        self.targets.is_none_or(|targets| targets.contains(&node))
    }

    /// Record that `node` is `hops` relationships from the seed, if it is wanted.
    fn reached(&mut self, node: u64, hops: u64) {
        if self.wants(node) {
            self.found.push((node, hops));
            self.remaining = self.remaining.map(|left| left.saturating_sub(1));
        }
    }

    fn done(&self) -> bool {
        self.remaining == Some(0)
    }

    /// Bytes the results found so far are charged for.
    fn bytes(&self) -> usize {
        std::mem::size_of::<(u64, u64)>() * self.found.capacity()
    }

    /// Record the seed as reached by the shortest closed trail `cycle` has
    /// found, when that fits within `max_hops`.
    fn closed(&mut self, seed: u64, cycle: &SeedCycle, max_hops: Option<u64>) {
        if let Some(length) = cycle.shortest.filter(|&l| max_hops.is_none_or(|m| l <= m)) {
            self.reached(seed, length);
        }
    }
}

/// The shortest closed trail through the seed of an undirected search with
/// `min_hops` 1, found while the search runs.
///
/// A directed search needs none of this: the first frontier node with a
/// relationship back to the seed closes a trail of exactly the length of the
/// level (see `reach_from_seed`). An undirected one cannot walk back along the
/// relationship it came by, so the return is not enough.
///
/// Why the shortest closed trail is a simple cycle through the seed. The
/// relationships of a closed trail form a connected subgraph in which every
/// node has even degree, and in such a graph every relationship lies on a cycle.
/// The seed has a relationship on the trail, so it lies on a cycle of that
/// subgraph, and a cycle is a closed trail that is no longer. A simple cycle
/// through the seed has one of three shapes: a loop on the seed (1), two parallel
/// relationships to one neighbour (2), or two distinct neighbours `p` and `q`
/// joined by a path that avoids the seed (`2 + length`).
///
/// The last shape is a shortest path between two distinct sources in the graph
/// without the seed, the sources being its neighbours. That is found by labelling
/// every node with its `branch` (the seed's neighbour its shortest path starts
/// with; the breadth-first search gives each node the branch of the node that
/// discovered it, so it is the label of a nearest source) and taking the least
/// `hops(u) + hops(v) + 1` over relationships `u - v` whose ends have different
/// branches. Such a relationship gives a walk of that length between two
/// sources, so the least value is not below the shortest path; and the shortest
/// path `p ... q` changes branch somewhere along it, at a relationship `u - v`
/// with `hops(u) <= dist(p, u) + 1` and `hops(v) <= dist(v, q) + 1`, so the least
/// value is not above it either. Parallel relationships to the seed need no
/// separate rule: scanning the seed meets the neighbour twice, once as a new
/// node (branch = itself) and once as a visited node of another branch than the
/// seed's own, which counts `0 + 1 + 1`.
#[derive(Default)]
struct SeedCycle {
    /// `(branch, hops)` of every node reached other than the seed.
    reached: std::collections::HashMap<u64, (u64, u64)>,
    /// Length of the shortest closed trail seen so far.
    shortest: Option<u64>,
}

impl SeedCycle {
    fn improve(&mut self, length: u64) {
        self.shortest = Some(self.shortest.map_or(length, |best| best.min(length)));
    }

    /// The branch of the frontier node `node`, which is `seed` or was reached.
    fn branch_of(&self, node: u64, seed: u64) -> Result<u64, GfError> {
        if node == seed {
            return Ok(seed);
        }
        self.reached
            .get(&node)
            .map(|&(branch, _)| branch)
            .ok_or_else(|| {
                GfError::Execution(format!(
                    "reachability search lost the branch of node {node}"
                ))
            })
    }

    /// A relationship from a node on `branch`, `hops` from the seed, to the
    /// already visited `neighbor`.
    fn cross(&mut self, branch: u64, hops: u64, neighbor: u64) {
        if let Some(&(other, other_hops)) = self.reached.get(&neighbor)
            && other != branch
        {
            self.improve(hops + other_hops + 1);
        }
    }

    /// Whether no closed trail shorter than the shortest one seen can still turn
    /// up. Scanning the nodes `level - 1` from the seed sees a new cycle only
    /// at `2 * level - 1` or longer (a relationship between two such nodes
    /// closes `2 * level - 1`, to a node one level further `2 * level`; one to
    /// a node nearer was seen from that node). A loop or a parallel
    /// relationship at the seed (1 and 2) is seen at level 1. `complete` says
    /// the level has been scanned to the end.
    fn settled(&self, level: u64, complete: bool) -> bool {
        let next_level = if complete { level + 1 } else { level };
        self.shortest.is_some_and(|best| best < 2 * next_level)
    }

    fn bytes(&self) -> usize {
        hash_table_bytes(
            self.reached.capacity(),
            std::mem::size_of::<(u64, (u64, u64))>(),
        )
    }
}

/// Level-synchronous breadth-first search from `seed`: every node reachable
/// within `cfg.max_hops` relationships is visited once, in the order of the
/// first shortest trail that reaches it, with its distance as `hops`.
///
/// `min_hops` is 0 or 1 (checked by the caller), so the shortest trail to a node
/// other than the seed is a shortest path (see the proof conditions in
/// `graphforge_ir::reachability_expansions`): a shortest walk repeats no node, so
/// it repeats no relationship either. The seed is reached with 0 relationships
/// when `min_hops` is 0; with `min_hops` 1 it is reached by the shortest closed
/// trail through it, which is
///
/// - directed: `level` for the first level whose frontier has a relationship
///   back to the seed. The path to that frontier node is a shortest path and
///   never enters the seed, so the closing relationship is not on it; and the
///   shortest closed walk through the seed repeats no node, so no shorter closed
///   trail exists;
/// - undirected: the shortest cycle through the seed, found by [`SeedCycle`].
///
/// With `targets`, only those nodes are returned and the search stops as soon as
/// all have been reached, that is, at the first level that reaches the last one.
///
/// The visited set, frontier and result are charged to `reservation` as they
/// grow; the frontier and visited set are released on return and the result is
/// left charged for the caller (`ReachSet::charged`).
#[allow(clippy::too_many_lines)]
fn reach_from_seed(
    cfg: &ExpandConfig,
    adjacency: &mut adjacency::AdjacencyReader<'_>,
    seed: u64,
    targets: Option<&std::collections::HashSet<u64>>,
    reservation: &mut ChunkedReservation,
) -> Result<ReachSet, GfError> {
    use std::collections::HashSet;

    let _capture = cfg.lifecycle.attach();
    let max_hops = cfg.max_hops.map(u64::from);
    let mut hits = Hits {
        targets,
        remaining: targets.map(HashSet::len),
        found: Vec::new(),
    };
    let mut visited: HashSet<u64> = HashSet::from([seed]);
    let mut frontier = vec![seed];
    let mut next: Vec<u64> = Vec::new();
    let mut held = 0_usize;
    let mut examined = 0_usize;

    // The seed is a result of its own only through a closed trail, which is
    // looked for only when it is wanted.
    let mut seed_pending = false;
    if cfg.min_hops == 0 {
        hits.reached(seed, 0);
    } else {
        seed_pending = hits.wants(seed);
    }
    let mut cycle =
        (seed_pending && cfg.direction == Direction::Undirected).then(SeedCycle::default);

    // With an edge predicate the search runs over the relationships that pass
    // it. Each level's passing relationships are decided together, a batch of
    // frontier nodes at a time (`PhysicalEdgeFilter::crossable`): `crossable`
    // is the batch's answer and `batch_end` is the frontier index it stops at.
    let edge_filter = cfg.edge_filter.as_deref();
    let mut crossable: HashSet<u64> = HashSet::new();

    let mut level = 0_u64;
    'levels: while !hits.done() && !frontier.is_empty() && max_hops.is_none_or(|max| level < max) {
        level += 1;
        let mut batch_end = 0_usize;
        for (index, &node) in frontier.iter().enumerate() {
            if let Some(filter) = edge_filter
                && index == batch_end
            {
                let wanted = Wanted {
                    visited: &visited,
                    seed,
                    seed_pending,
                    every: cycle.is_some() && seed_pending,
                };
                let (edges, taken) =
                    filter.crossable(cfg, adjacency, &frontier[index..], &wanted, reservation)?;
                crossable = edges;
                batch_end = index + taken;
            }
            let branch = match &cycle {
                Some(cycle) => cycle.branch_of(node, seed)?,
                None => seed,
            };
            let mut returns_to_seed = false;
            adjacency.with_neighbors(node, |neighbors| {
                examined = examined.saturating_add(neighbors.len());
                for (edge, neighbor) in neighbors.iter() {
                    if edge_filter.is_some() && !crossable.contains(&edge) {
                        continue;
                    }
                    if neighbor == seed {
                        returns_to_seed = true;
                    } else if visited.insert(neighbor) {
                        next.push(neighbor);
                        hits.reached(neighbor, level);
                        if let Some(cycle) = cycle.as_mut() {
                            let own = if node == seed { neighbor } else { branch };
                            cycle.reached.insert(neighbor, (own, level));
                        }
                    } else if let Some(cycle) = cycle.as_mut() {
                        cycle.cross(branch, level - 1, neighbor);
                    }
                }
            })?;
            if returns_to_seed && seed_pending {
                match cycle.as_mut() {
                    // A loop on the seed. Any other relationship back to the
                    // seed is a relationship the walk came by, or a parallel one
                    // that scanning the seed has already counted.
                    Some(cycle) if node == seed => cycle.improve(1),
                    Some(_) => {}
                    None => {
                        seed_pending = false;
                        hits.reached(seed, level);
                    }
                }
            }
            if let Some(cycle) = &cycle
                && seed_pending
                && cycle.settled(level, false)
            {
                seed_pending = false;
                hits.closed(seed, cycle, max_hops);
            }
            let live = hash_table_bytes(visited.capacity(), std::mem::size_of::<u64>())
                .saturating_add(hash_table_bytes(
                    crossable.capacity(),
                    std::mem::size_of::<u64>(),
                ))
                .saturating_add(8 * (frontier.capacity() + next.capacity()))
                .saturating_add(hits.bytes())
                .saturating_add(cycle.as_ref().map_or(0, SeedCycle::bytes));
            settle(reservation, &mut held, live)?;
            if hits.done() {
                break 'levels;
            }
        }
        if let Some(cycle) = &cycle
            && seed_pending
            && cycle.settled(level, true)
        {
            seed_pending = false;
            hits.closed(seed, cycle, max_hops);
        }
        std::mem::swap(&mut frontier, &mut next);
        next.clear();
    }
    // The search ran out of levels before a closed trail was known to be the
    // shortest: nothing shorter exists within the bound.
    if let Some(cycle) = &cycle
        && seed_pending
    {
        hits.closed(seed, cycle, max_hops);
    }
    drop((visited, frontier, next, cycle, crossable));
    settle(reservation, &mut held, hits.bytes())?;
    if cfg.capture_epoch != 0 {
        demand::record_candidates(cfg.capture_epoch, cfg.edge_var, examined);
    }
    Ok(ReachSet {
        found: hits.found,
        charged: held,
    })
}

/// Build one output batch for `reached` (a bounded slice of a search): the input
/// row each came from, the reached node's topology record and its hop count.
/// Reads the reached nodes only; the traversed relationships are never read.
///
/// What is read is charged while the batch is built and released before this
/// returns; the returned batch stays charged until [`send_chunk`] hands it on.
fn materialise_reached(
    cfg: &ExpandConfig,
    input: &RecordBatch,
    reached: &[Reached],
    reservation: &mut ChunkedReservation,
) -> Result<RecordBatch, GfError> {
    use arrow::compute::concat_batches;
    use arrow::compute::take;

    let exec_err = |m: String| GfError::Execution(m);
    let _capture = cfg.lifecycle.attach();
    let to_u32 = |v: usize| -> Result<u32, GfError> {
        u32::try_from(v).map_err(|_| exec_err(format!("row index {v} exceeds u32")))
    };
    let mut temporary = 0_usize;

    let ids: std::collections::HashSet<u64> = reached.iter().map(|(_, node, _)| *node).collect();
    charge(
        reservation,
        &mut temporary,
        hash_table_bytes(ids.capacity(), std::mem::size_of::<u64>()),
    )?;
    let node_batches = graphforge_storage::read_nodes_filtered_from_files(
        &topology_for_provider(cfg.provider.as_ref(), &cfg.dir)?,
        &ids,
    )
    .map_err(|e| exec_err(e.to_string()))?;
    // With no node batch there is nothing to reach: emit zero rows.
    let Some(first) = node_batches.first() else {
        reservation.shrink(temporary);
        let empty = RecordBatch::new_empty(cfg.out_schema.clone());
        grow_reservation(reservation, empty.get_array_memory_size())?;
        return Ok(empty);
    };
    let node_batch =
        concat_batches(&first.schema(), &node_batches).map_err(|e| exec_err(e.to_string()))?;
    charge(
        reservation,
        &mut temporary,
        node_batch.get_array_memory_size(),
    )?;
    drop(node_batches);
    let node_ids = u64_column(&node_batch, 1)?; // node_id is column 1
    let node_row: std::collections::HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .filter_map(|(i, id)| id.map(|v| (v, i)))
        .collect();
    charge(
        reservation,
        &mut temporary,
        hash_table_bytes(node_row.capacity(), std::mem::size_of::<(u64, usize)>()),
    )?;

    let src_take = UInt32Array::from(
        reached
            .iter()
            .map(|(row, _, _)| to_u32(*row))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let dst_take = UInt32Array::from(
        reached
            .iter()
            .map(|(_, node, _)| {
                let row = node_row.get(node).ok_or_else(|| {
                    exec_err(format!(
                        "VarLenExpand reached unknown destination node_id {node}"
                    ))
                })?;
                to_u32(*row)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let mut columns = Vec::with_capacity(input.num_columns() + node_batch.num_columns() + 1);
    for col in input.columns() {
        columns.push(take(col, &src_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    for col in node_batch.columns() {
        columns.push(take(col, &dst_take, None).map_err(|e| exec_err(e.to_string()))?);
    }
    columns.push(Arc::new(UInt64Array::from_iter_values(
        reached.iter().map(|(_, _, hops)| *hops),
    )));
    let out = RecordBatch::try_new(cfg.out_schema.clone(), columns)
        .map_err(|e| exec_err(e.to_string()))?;
    reservation.shrink(temporary);
    grow_reservation(reservation, out.get_array_memory_size())?;
    Ok(out)
}
