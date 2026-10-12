//! Pull cursor over the logical rows of a [`ShardedCsrIndex`].

use super::{CsrIndex, CsrRow, CsrShardRecord, ShardedCsrIndex, corrupt_index};
use super::{GfError, read_authenticated_shard};

const EMPTY_ROW: CsrRow<'static> = CsrRow {
    edge_ids: &[],
    neighbor_ids: &[],
};

/// The logical row being concatenated because it continues from one shard into
/// the next.
#[derive(Default)]
struct SpanningRow {
    node: Option<u64>,
    edge_ids: Vec<u64>,
    neighbor_ids: Vec<u64>,
}

impl SpanningRow {
    fn start(&mut self, node: u64, row: CsrRow<'_>) {
        self.node = Some(node);
        self.append(row);
    }

    fn append(&mut self, row: CsrRow<'_>) {
        self.edge_ids.extend_from_slice(row.edge_ids);
        self.neighbor_ids.extend_from_slice(row.neighbor_ids);
    }

    const fn row(&self) -> CsrRow<'_> {
        CsrRow {
            edge_ids: self.edge_ids.as_slice(),
            neighbor_ids: self.neighbor_ids.as_slice(),
        }
    }

    /// Release the visited row's buffers so a hub row is not held longer than
    /// the row that needed it.
    fn finish(&mut self) {
        self.node = None;
        self.edge_ids = Vec::new();
        self.neighbor_ids = Vec::new();
    }
}

/// The one decoded shard a cursor holds.
struct LoadedShard {
    csr: CsrIndex,
    first_node: u64,
    node_count: u64,
    next_local: u64,
    /// The next shard continues this shard's last row.
    continues_next: bool,
}

/// What the next call returns, decided before any borrow of the cursor is made.
enum Step {
    Empty(u64),
    Shard { node: u64, local: u64 },
    Spanning(u64),
    Done,
}

/// Pulls the rows `0..node_count` of one [`ShardedCsrIndex`] in node order,
/// decoding shards sequentially in manifest order.
///
/// Each row is exactly what [`ShardedCsrIndex::row`] returns for that node: a
/// row that continues across shards arrives concatenated in shard order, and a
/// node that no shard covers arrives empty. Every shard goes through the same
/// authenticate-then-decode path as random access, so a missing, mis-sized,
/// checksum-failing or undecodable shard is refused. Decoded shards are dropped
/// as soon as their rows are consumed and never enter the index's decoded-shard
/// cache, so the cursor holds one encoded and one decoded shard at a time, plus
/// the entries of a row that spans shards (which `row` also concatenates).
///
/// The writer lets consecutive shards share only the last row of the earlier
/// one. A manifest with any other overlap is refused on the first pull, before
/// any decode, since a single sequential pass could not reproduce `row` for it.
///
/// After an error, a further pull retries the shard that failed.
pub struct CsrRowCursor<'a> {
    index: &'a ShardedCsrIndex,
    next_shard: usize,
    shard: Option<LoadedShard>,
    spanning: SpanningRow,
    /// A spanning row was returned and its buffers can be released.
    release_spanning: bool,
    /// Every node below this one has been returned.
    next_node: u64,
    checked: bool,
}

impl<'a> CsrRowCursor<'a> {
    pub(super) const fn new(index: &'a ShardedCsrIndex) -> Self {
        Self {
            index,
            next_shard: 0,
            shard: None,
            spanning: SpanningRow {
                node: None,
                edge_ids: Vec::new(),
                neighbor_ids: Vec::new(),
            },
            release_spanning: false,
            next_node: 0,
            checked: false,
        }
    }

    /// The next `(node_id, row)`, or `None` after node `node_count - 1`. The row
    /// is valid until the next pull.
    pub fn next_row(&mut self) -> Result<Option<(u64, CsrRow<'_>)>, GfError> {
        if !self.checked {
            for pair in self.index.manifest.shards.windows(2) {
                if pair[1].first_node < pair[0].first_node + pair[0].node_count - 1 {
                    return Err(corrupt_index(
                        "CSR shards overlap beyond a shared boundary row",
                    ));
                }
            }
            self.checked = true;
        }
        if self.release_spanning {
            self.spanning.finish();
            self.release_spanning = false;
        }
        match self.advance()? {
            Step::Done => Ok(None),
            Step::Empty(node) => Ok(Some((node, EMPTY_ROW))),
            Step::Spanning(node) => {
                self.release_spanning = true;
                Ok(Some((node, self.spanning.row())))
            }
            Step::Shard { node, local } => {
                let shard = self.shard.as_ref().expect("a returned shard row is loaded");
                Ok(Some((node, shard.csr.row(local))))
            }
        }
    }

    fn advance(&mut self) -> Result<Step, GfError> {
        loop {
            let Some(shard) = self.shard.as_mut() else {
                let Some(record) = self.index.manifest.shards.get(self.next_shard) else {
                    return Ok(if self.next_node < self.index.manifest.node_count {
                        self.next_node += 1;
                        Step::Empty(self.next_node - 1)
                    } else {
                        Step::Done
                    });
                };
                self.shard = Some(self.load(record)?);
                self.next_shard += 1;
                continue;
            };
            if shard.next_local == shard.node_count {
                self.shard = None;
                continue;
            }
            let local = shard.next_local;
            let node = shard.first_node + local;
            if self.next_node < node {
                self.next_node += 1;
                return Ok(Step::Empty(self.next_node - 1));
            }
            shard.next_local += 1;
            let defers = shard.continues_next && local + 1 == shard.node_count;
            let row = shard.csr.row(local);
            if self.spanning.node == Some(node) {
                self.spanning.append(row);
                if defers {
                    continue;
                }
                self.next_node = node + 1;
                return Ok(Step::Spanning(node));
            }
            if defers {
                self.spanning.start(node, row);
                continue;
            }
            self.next_node = node + 1;
            return Ok(Step::Shard { node, local });
        }
    }

    fn load(&self, record: &CsrShardRecord) -> Result<LoadedShard, GfError> {
        let csr = read_authenticated_shard(&self.index.root.join(&record.file), record)?;
        self.index
            .cache
            .lock()
            .map_err(|_| GfError::Storage("CSR shard cache lock poisoned".into()))?
            .decodes += 1;
        let last_node = record.first_node + record.node_count - 1;
        Ok(LoadedShard {
            csr,
            first_node: record.first_node,
            node_count: record.node_count,
            next_local: 0,
            continues_next: self
                .index
                .manifest
                .shards
                .get(self.next_shard + 1)
                .is_some_and(|following| following.first_node <= last_node),
        })
    }
}
