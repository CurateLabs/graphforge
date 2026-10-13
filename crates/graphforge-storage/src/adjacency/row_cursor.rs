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
/// cache, so the cursor holds one shard at a time: its encoded bytes twice
/// while Arrow reads the batch (the read buffer and Arrow's block buffer), its
/// decoded buffers and its CSR copy, plus the entries of a row that spans
/// shards (which `row` also concatenates).
///
/// The writer lets consecutive shards share only the last row of the earlier
/// one. A manifest with any other overlap is refused on the first pull, before
/// any decode, since a single sequential pass could not reproduce `row` for it.
///
/// Every error is terminal. A refusal (checksum, length, structure, missing
/// shard, overlap) stays the cursor's answer to every later pull and releases
/// whatever it had assembled, so a row that spans the refused shard can never be
/// completed by bytes that reappear afterwards. A transient I/O error is not
/// told apart from a refusal: the consumer holds a half-built export either way,
/// and a fresh cursor starts over.
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
    /// The error every pull returns once one has occurred.
    failed: Option<GfError>,
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
            failed: None,
        }
    }

    /// The next `(node_id, row)`, or `None` after node `node_count - 1`. The row
    /// is valid until the next pull. After an error, every pull returns that
    /// error.
    pub fn next_row(&mut self) -> Result<Option<(u64, CsrRow<'_>)>, GfError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let step = match self.step() {
            Ok(step) => step,
            Err(error) => {
                self.shard = None;
                self.spanning.finish();
                self.release_spanning = false;
                self.failed = Some(error.clone());
                return Err(error);
            }
        };
        match step {
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

    fn step(&mut self) -> Result<Step, GfError> {
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
        self.advance()
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
            .write()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adjacency::write_sharded_csr;

    /// Node 1 holds twelve entries, which at three per shard span shards A-D.
    fn hub_index(directory: &std::path::Path) -> ShardedCsrIndex {
        let mut csr = CsrIndex {
            offsets: vec![0, 0],
            ..CsrIndex::default()
        };
        for entry in 0..12 {
            csr.edge_ids.push(100 + entry);
            csr.neighbor_ids.push(entry);
        }
        csr.offsets.push(12);
        csr.offsets.push(12);
        let path = directory.join("HUB.out.csr");
        write_sharded_csr(&path, &csr, 3).unwrap();
        let index = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!(index.manifest.shards.len(), 4);
        assert!(
            index
                .manifest
                .shards
                .iter()
                .all(|record| record.first_node == 1)
        );
        index
    }

    #[test]
    fn a_refusal_is_terminal_and_the_hub_is_never_completed_across_it() {
        let directory = tempfile::tempdir().unwrap();
        let index = hub_index(directory.path());
        let shard_c = index.root.join(&index.manifest.shards[2].file);
        let original = std::fs::read(&shard_c).unwrap();
        let mut corrupt = original.clone();
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 1;
        std::fs::write(&shard_c, corrupt).unwrap();

        // The walk and the cursor are refused alike, and the hub is never seen.
        let fresh = ShardedCsrIndex::open(&index.root.with_file_name("HUB.out.csr")).unwrap();
        let mut visited = Vec::new();
        let walked = fresh.walk_rows(|node, row| {
            visited.push((node, row.len()));
            Ok(())
        });
        assert!(walked.unwrap_err().to_string().contains("checksum"));
        assert_eq!(visited, vec![(0, 0)]);

        let fresh = ShardedCsrIndex::open(&index.root.with_file_name("HUB.out.csr")).unwrap();
        let mut cursor = fresh.row_cursor();
        let first = cursor.next_row().unwrap().map(|(node, _)| node);
        assert_eq!(first, Some(0));
        let refusal = cursor.next_row().unwrap_err().to_string();
        assert!(refusal.contains("checksum"), "{refusal}");
        assert!(cursor.spanning.edge_ids.capacity() == 0 && cursor.spanning.node.is_none());
        let decodes = fresh.shard_decode_count();

        // The bytes come back; the cursor still refuses and never yields the hub.
        std::fs::write(&shard_c, &original).unwrap();
        for _ in 0..3 {
            assert_eq!(cursor.next_row().unwrap_err().to_string(), refusal);
        }
        assert_eq!(fresh.shard_decode_count(), decodes, "no shard is re-read");

        // A new cursor starts over and yields the whole hub.
        let mut again = fresh.row_cursor();
        let mut hub = None;
        while let Some((node, row)) = again.next_row().unwrap() {
            if node == 1 {
                hub = Some(row.len());
            }
        }
        assert_eq!(hub, Some(12));
    }
}
