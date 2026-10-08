//! The UUID-ordered identity delta, generated in memory.

use std::io::Read;

/// Width of a shaped identity record: UUID, kind, retained marker, surrogate.
const RECORD: usize = crate::construction_record_layout::BASE_IDENTITY_WIDTH;

/// Node and edge identities merged by UUID, byte for byte the records the
/// staged shaper writes to its identity run: `uuid | kind | 0 | surrogate`
/// with a big-endian dense rank per kind.
pub(super) struct IdentityStream<'a> {
    nodes: &'a [[u8; 16]],
    edges: &'a [[u8; 16]],
    node: usize,
    edge: usize,
}

impl<'a> IdentityStream<'a> {
    pub(super) fn new(nodes: &'a [[u8; 16]], edges: &'a [[u8; 16]]) -> Self {
        Self {
            nodes,
            edges,
            node: 0,
            edge: 0,
        }
    }

    pub(super) fn byte_len(&self) -> u64 {
        (self.nodes.len() + self.edges.len()) as u64 * RECORD as u64
    }
}

impl Read for IdentityStream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while buffer.len() - written >= RECORD {
            let take_node = match (self.nodes.get(self.node), self.edges.get(self.edge)) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(node), Some(edge)) => node < edge,
            };
            let record = &mut buffer[written..written + RECORD];
            if take_node {
                record[..16].copy_from_slice(&self.nodes[self.node]);
                record[16] = 0;
                self.node += 1;
                record[18..].copy_from_slice(&(self.node as u64).to_be_bytes());
            } else {
                record[..16].copy_from_slice(&self.edges[self.edge]);
                record[16] = 1;
                self.edge += 1;
                record[18..].copy_from_slice(&(self.edge as u64).to_be_bytes());
            }
            record[17] = 0;
            written += RECORD;
        }
        Ok(written)
    }
}
