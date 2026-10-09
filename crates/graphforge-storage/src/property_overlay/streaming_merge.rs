//! Streaming newest-wins reads of a property route (#1931).
//!
//! Every fragment is already a run sorted by UUID, so the newest live snapshot
//! of each UUID is a k-way merge of the fragments, and no sorted copy of the
//! rows is needed. A fragment is opened when the merge frontier reaches its
//! smallest UUID and closed when it is exhausted. Fragments that do not
//! overlap are read one after another; a mutation fragment, which spans the
//! route, stays open beside whichever base fragment is current. Reading writes
//! nothing: a snapshot is authenticated in memory and the merge is in memory.
//!
//! The merge is bounded by the same live-byte budget as the spooled merge. A
//! route whose overlapping fragments cannot all be open within that budget is
//! refused with the budget's error rather than spilled to disk, because a read
//! must not write.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};

use super::projected_reads::{
    DecodedRetention, ProjectedMetricSources, ProjectedReaderContext, PropertyParquetRows,
    finalize_projected_metrics, projected_fragment_rows,
};
use super::selective_reads::{PropertyEquality, RowGroupSelection, fragment_may_equal};
use super::{
    Arc, AtomicU64, AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, GfError,
    LiveByteBudget, Mutex, PropertyFragmentId, PropertyOverlayLimits, PropertyOverlayMetrics,
    PropertyRouteKind, PropertySnapshotRow, ReadCounts, SnapshotScratch, corrupt,
};

/// One streaming read of a route.
pub(crate) struct RouteRead<'a> {
    pub(crate) kind: PropertyRouteKind,
    pub(crate) route: &'a str,
    /// Property columns to decode; `None` decodes all of them.
    pub(crate) selected_properties: Option<&'a BTreeSet<String>>,
    /// Restrict the read to these UUIDs. The newest snapshot of each is still
    /// resolved against every fragment that can hold it.
    pub(crate) uuids: Option<&'a BTreeSet<[u8; 16]>>,
    pub(crate) limits: PropertyOverlayLimits,
    /// Collect exact returned work.
    pub(crate) collect: bool,
}

/// Counters shared by every decoder of one read.
struct ReadAccounting {
    counts: ReadCounts,
    authentication_bytes: Option<Arc<AtomicU64>>,
    authentication_block_equivalents: Option<Arc<AtomicU64>>,
    authentication_read_calls: Option<Arc<AtomicU64>>,
    budget: Arc<LiveByteBudget>,
    decoded: Arc<Mutex<DecodedRetention>>,
}

impl ReadAccounting {
    fn new(collect: bool, limits: PropertyOverlayLimits) -> Self {
        Self {
            counts: ReadCounts::new(collect),
            authentication_bytes: collect.then(|| Arc::new(AtomicU64::new(0))),
            authentication_block_equivalents: collect.then(|| Arc::new(AtomicU64::new(0))),
            authentication_read_calls: collect.then(|| Arc::new(AtomicU64::new(0))),
            budget: Arc::new(LiveByteBudget::new(limits.max_buffered_bytes)),
            decoded: Arc::new(Mutex::new(DecodedRetention::default())),
        }
    }

    fn finalize(
        &self,
        metrics: &mut PropertyOverlayMetrics,
        fragments: &[AuthenticatedPropertyFragment],
    ) {
        finalize_projected_metrics(
            metrics,
            &ProjectedMetricSources {
                counts: &self.counts,
                authentication_bytes: &self.authentication_bytes,
                authentication_block_equivalents: &self.authentication_block_equivalents,
                authentication_read_calls: &self.authentication_read_calls,
                decoded: &self.decoded,
                budget: self.budget.as_ref(),
                authenticated_snapshot_peak_bytes: fragments
                    .iter()
                    .flat_map(|fragment| fragment.parts.iter().map(|part| part.entry.byte_length))
                    .max()
                    .unwrap_or(0),
            },
        );
    }
}

/// One open fragment: its decoder and the row it currently offers the merge.
struct Cursor {
    rows: PropertyParquetRows,
    id: PropertyFragmentId,
    range: Option<([u8; 16], [u8; 16])>,
    prior: Option<[u8; 16]>,
    head: Option<PropertySnapshotRow>,
}

impl Cursor {
    /// Move to the next row this read wants, validating the fragment's order.
    fn advance(
        &mut self,
        uuids: Option<&BTreeSet<[u8; 16]>>,
        metrics: &mut PropertyOverlayMetrics,
    ) -> Result<bool, GfError> {
        self.head = None;
        for row in self.rows.by_ref() {
            let row = row?;
            if row.tombstone && !row.values.is_empty() {
                return Err(corrupt("property tombstone carries live values"));
            }
            if self.prior.is_some_and(|uuid| uuid >= row.uuid) {
                return Err(corrupt("property fragment UUIDs are duplicate or unsorted"));
            }
            if self
                .range
                .is_some_and(|(min, max)| row.uuid < min || row.uuid > max)
            {
                return Err(corrupt(
                    "property fragment row lies outside its declared UUID range",
                ));
            }
            self.prior = Some(row.uuid);
            metrics.physical_rows = metrics.physical_rows.saturating_add(1);
            if uuids.is_some_and(|wanted| !wanted.contains(&row.uuid)) {
                continue;
            }
            self.head = Some(row);
            return Ok(true);
        }
        Ok(false)
    }
}

type HeapKey = Reverse<([u8; 16], Reverse<PropertyFragmentId>, usize)>;

impl AuthenticatedPropertyInventory {
    /// Visit the newest live snapshot of each UUID of a route in UUID order,
    /// without writing. `emit` returns `false` to stop early.
    pub(crate) fn visit_route_streaming<F>(
        &self,
        read: &RouteRead<'_>,
        mut emit: F,
    ) -> Result<PropertyOverlayMetrics, GfError>
    where
        F: FnMut(PropertySnapshotRow) -> Result<bool, GfError>,
    {
        let Some(fragments) = self.routes.get(&(read.kind, read.route.to_owned())) else {
            return Ok(PropertyOverlayMetrics::default());
        };
        // Resolve the route's footer summary before any fragment is trusted, so
        // a route that cannot be summarized refuses here rather than reading.
        self.route_summary(read.kind, read.route)?;
        let mut plan = Vec::with_capacity(fragments.len());
        for fragment in fragments {
            let footer = self.fragment_footer_of(fragment, read.kind, read.route)?;
            let range = footer.uuid_range;
            if let (Some(wanted), Some((min, max))) = (read.uuids, range)
                && wanted.range(min..=max).next().is_none()
            {
                continue;
            }
            plan.push((range.map_or([0; 16], |(min, _)| min), range, fragment));
        }
        plan.sort_unstable_by_key(|(min, _, fragment)| (*min, fragment.id));

        let accounting = ReadAccounting::new(read.collect, read.limits);
        let scratch = self.lazy_snapshot_scratch()?;
        let context = ProjectedReaderContext {
            inventory: self,
            scratch: SnapshotScratch::Lazy(&scratch),
            row_groups: read
                .uuids
                .map_or(RowGroupSelection::All, RowGroupSelection::Uuids),
            limits: read.limits,
            kind: read.kind,
            route: read.route,
            selected_properties: read.selected_properties,
            counts: &accounting.counts,
            budget: &accounting.budget,
            decoded: &accounting.decoded,
            authentication_bytes: &accounting.authentication_bytes,
            authentication_block_equivalents: &accounting.authentication_block_equivalents,
            authentication_read_calls: &accounting.authentication_read_calls,
        };
        let mut metrics = PropertyOverlayMetrics::default();
        let mut cursors: Vec<Option<Cursor>> = Vec::new();
        let mut heap: BinaryHeap<HeapKey> = BinaryHeap::new();
        let mut next_fragment = 0;
        loop {
            // A fragment cannot offer a UUID below its smallest, so every
            // fragment that could hold the frontier UUID or an earlier one is
            // open before the frontier row is taken.
            while let Some((min, range, fragment)) = plan.get(next_fragment) {
                let frontier = heap.peek().map(|Reverse((uuid, ..))| *uuid);
                if frontier.is_some_and(|frontier| *min > frontier) {
                    break;
                }
                next_fragment += 1;
                metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
                let mut cursor = Cursor {
                    rows: projected_fragment_rows(fragment, &context),
                    id: fragment.id,
                    range: *range,
                    prior: None,
                    head: None,
                };
                if cursor.advance(read.uuids, &mut metrics)? {
                    let index = cursors.len();
                    let uuid = cursor.head.as_ref().expect("advanced to a row").uuid;
                    heap.push(Reverse((uuid, Reverse(cursor.id), index)));
                    cursors.push(Some(cursor));
                }
            }
            let Some(Reverse((uuid, _, index))) = heap.pop() else {
                break;
            };
            let newest = Self::take_head(&mut cursors, index, read.uuids, &mut heap, &mut metrics)?;
            // Older snapshots of the same UUID are shadowed.
            while heap.peek().is_some_and(|Reverse((next, ..))| *next == uuid) {
                let Some(Reverse((_, _, shadowed))) = heap.pop() else {
                    break;
                };
                Self::take_head(&mut cursors, shadowed, read.uuids, &mut heap, &mut metrics)?;
                metrics.shadowed_rows = metrics.shadowed_rows.saturating_add(1);
            }
            if newest.tombstone {
                metrics.tombstones = metrics.tombstones.saturating_add(1);
            } else {
                metrics.logical_rows = metrics.logical_rows.saturating_add(1);
                if !emit(newest)? {
                    break;
                }
            }
        }
        drop(cursors);
        accounting.finalize(&mut metrics, fragments);
        Ok(metrics)
    }

    /// Take a cursor's head row and offer its next one to the merge.
    fn take_head(
        cursors: &mut [Option<Cursor>],
        index: usize,
        uuids: Option<&BTreeSet<[u8; 16]>>,
        heap: &mut BinaryHeap<HeapKey>,
        metrics: &mut PropertyOverlayMetrics,
    ) -> Result<PropertySnapshotRow, GfError> {
        let cursor = cursors[index]
            .as_mut()
            .ok_or_else(|| corrupt("property merge cursor is closed"))?;
        let row = cursor
            .head
            .take()
            .ok_or_else(|| corrupt("property merge cursor has no row"))?;
        if cursor.advance(uuids, metrics)? {
            let next = cursor.head.as_ref().expect("advanced to a row").uuid;
            heap.push(Reverse((next, Reverse(cursor.id), index)));
        } else {
            // Exhausted: close the decoder and release its reservations now.
            cursors[index] = None;
        }
        Ok(row)
    }

    /// The UUIDs whose newest snapshot may satisfy `equality`, found by
    /// reading only the compared column of the fragments whose statistics admit
    /// the value. `None` when the predicate cannot be answered this way: the
    /// column is not stored with the compared type in every fragment, or more
    /// than `cap` rows are candidates.
    ///
    /// Every row that holds the value in some fragment is a candidate, so a
    /// UUID whose newest snapshot satisfies the predicate is never missed. A
    /// candidate whose newest snapshot differs is removed when the candidates
    /// are resolved against every fragment.
    pub(crate) fn equality_candidates(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        equality: &PropertyEquality,
        limits: PropertyOverlayLimits,
        cap: usize,
        collect: bool,
    ) -> Result<Option<(BTreeSet<[u8; 16]>, PropertyOverlayMetrics)>, GfError> {
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(Some((BTreeSet::new(), PropertyOverlayMetrics::default())));
        };
        let summary = self.route_summary(kind, route)?;
        let expected = equality.value.data_type();
        if summary.is_some_and(|summary| {
            summary
                .schema
                .field_with_name(&equality.column)
                .is_ok_and(|field| field.data_type() != &expected)
        }) {
            return Ok(None);
        }
        let mut wanted = Vec::new();
        for fragment in fragments {
            let footer = self.fragment_footer_of(fragment, kind, route)?;
            match footer.schema.field_with_name(&equality.column) {
                Ok(field) if field.data_type() != &expected => return Ok(None),
                Err(_) => continue,
                Ok(_) => {}
            }
            if fragment_may_equal(&footer.metadata, equality) {
                wanted.push(fragment);
            }
        }
        let accounting = ReadAccounting::new(collect, limits);
        let scratch = self.lazy_snapshot_scratch()?;
        let selected = BTreeSet::from([equality.column.clone()]);
        let context = ProjectedReaderContext {
            inventory: self,
            scratch: SnapshotScratch::Lazy(&scratch),
            row_groups: RowGroupSelection::Equals(equality),
            limits,
            kind,
            route,
            selected_properties: Some(&selected),
            counts: &accounting.counts,
            budget: &accounting.budget,
            decoded: &accounting.decoded,
            authentication_bytes: &accounting.authentication_bytes,
            authentication_block_equivalents: &accounting.authentication_block_equivalents,
            authentication_read_calls: &accounting.authentication_read_calls,
        };
        let mut metrics = PropertyOverlayMetrics::default();
        let mut candidates = BTreeSet::new();
        for fragment in wanted {
            metrics.fragments_considered = metrics.fragments_considered.saturating_add(1);
            let mut cursor = Cursor {
                rows: projected_fragment_rows(fragment, &context),
                id: fragment.id,
                range: self.fragment_footer_of(fragment, kind, route)?.uuid_range,
                prior: None,
                head: None,
            };
            while cursor.advance(None, &mut metrics)? {
                let row = cursor.head.take().expect("advanced to a row");
                if !row.tombstone && equality.holds(&row.values) {
                    candidates.insert(row.uuid);
                    if candidates.len() > cap {
                        return Ok(None);
                    }
                }
            }
        }
        accounting.finalize(&mut metrics, fragments);
        Ok(Some((candidates, metrics)))
    }
}
