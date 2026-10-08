//! The UUID-ordered identity delta, generated in memory or from scratch.

use std::io::Read;
use std::path::PathBuf;

use super::scratch::{BlockReader, Scratch};

/// Width of a shaped identity record: UUID, kind, retained marker, surrogate.
const RECORD: usize = crate::construction_record_layout::BASE_IDENTITY_WIDTH;

/// The sorted edge UUIDs, resident or in per-partition scratch files.
pub(super) enum EdgeUuids<'a> {
    Memory {
        uuids: &'a [[u8; 16]],
        position: usize,
    },
    Scratch {
        scratch: &'a Scratch,
        files: std::vec::IntoIter<PathBuf>,
        reader: Option<BlockReader<'a>>,
        block: Vec<u8>,
        offset: usize,
    },
}

impl<'a> EdgeUuids<'a> {
    pub(super) fn memory(uuids: &'a [[u8; 16]]) -> Self {
        Self::Memory { uuids, position: 0 }
    }

    pub(super) fn scratch(scratch: &'a Scratch, files: Vec<PathBuf>) -> Self {
        Self::Scratch {
            scratch,
            files: files.into_iter(),
            reader: None,
            block: Vec::new(),
            offset: 0,
        }
    }

    /// The next UUID without consuming it.
    fn peek(&mut self) -> std::io::Result<Option<[u8; 16]>> {
        match self {
            Self::Memory { uuids, position } => Ok(uuids.get(*position).copied()),
            Self::Scratch {
                scratch,
                files,
                reader,
                block,
                offset,
            } => loop {
                if *offset + 16 <= block.len() {
                    return Ok(Some(
                        block[*offset..*offset + 16].try_into().expect("16 bytes"),
                    ));
                }
                if let Some(open) = reader {
                    if open
                        .next_block(block)
                        .map_err(|error| std::io::Error::other(error.to_string()))?
                    {
                        *offset = 0;
                        continue;
                    }
                    *reader = None;
                }
                let Some(path) = files.next() else {
                    return Ok(None);
                };
                block.clear();
                *offset = 0;
                *reader = Some(
                    BlockReader::open(scratch, &path)
                        .map_err(|error| std::io::Error::other(error.to_string()))?,
                );
            },
        }
    }

    fn advance(&mut self) {
        match self {
            Self::Memory { position, .. } => *position += 1,
            Self::Scratch { offset, .. } => *offset += 16,
        }
    }
}

/// Node and edge identities merged by UUID, byte for byte the records the
/// staged shaper writes to its identity run: `uuid | kind | 0 | surrogate`
/// with a big-endian dense rank per kind.
pub(super) struct IdentityStream<'a> {
    nodes: &'a [[u8; 16]],
    edges: EdgeUuids<'a>,
    edge_count: u64,
    node: usize,
    edge: u64,
}

impl<'a> IdentityStream<'a> {
    pub(super) fn new(nodes: &'a [[u8; 16]], edges: EdgeUuids<'a>, edge_count: u64) -> Self {
        Self {
            nodes,
            edges,
            edge_count,
            node: 0,
            edge: 0,
        }
    }

    pub(super) fn byte_len(&self) -> u64 {
        (self.nodes.len() as u64 + self.edge_count) * RECORD as u64
    }
}

impl Read for IdentityStream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let mut written = 0;
        while buffer.len() - written >= RECORD {
            let next_edge = self.edges.peek()?;
            let take_node = match (self.nodes.get(self.node), next_edge) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(node), Some(edge)) => *node < edge,
            };
            let record = &mut buffer[written..written + RECORD];
            if take_node {
                record[..16].copy_from_slice(&self.nodes[self.node]);
                record[16] = 0;
                self.node += 1;
                record[18..].copy_from_slice(&(self.node as u64).to_be_bytes());
            } else {
                record[..16].copy_from_slice(&next_edge.expect("an edge was peeked"));
                record[16] = 1;
                self.edges.advance();
                self.edge += 1;
                record[18..].copy_from_slice(&self.edge.to_be_bytes());
            }
            record[17] = 0;
            written += RECORD;
        }
        Ok(written)
    }
}
