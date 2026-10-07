//! Streaming verification of the payload inventory, `manifest-sha256.txt`.
//!
//! The inventory has one row per `data/` entry, so it grows with the entry
//! count: about 21,500 rows and 5.1 MB for the S26 Graph500 project, and up to
//! `max_entries` rows of `max_path_bytes` paths in general (#900). Canonical
//! order places every `data/` entry before it, so the reader checks each row
//! against the entries it has already authenticated as the bytes stream past.
//! It holds one row at a time, never the inventory.

use super::{Entry, hex, sha, validate_path};
use graphforge_core::portable::{PortableV2Error, PortableV2ErrorCode};

pub(super) const INVENTORY_PATH: &str = "manifest-sha256.txt";

/// Outcome of checking one streamed inventory. It is reported when the bag
/// manifests are validated, so the structural checks that ran before the old
/// whole-inventory parse still run first. Within the inventory, the first bad
/// row decides the reason.
pub(super) type InventoryVerdict = Result<(), PortableV2Error>;

/// Checks a streamed inventory against the `data/` entries that precede it.
pub(super) struct InventoryCheck<'a> {
    preceding: &'a [Entry],
    max_path_bytes: usize,
    next: usize,
    previous: Option<usize>,
    row: Vec<u8>,
    length: u64,
    failure: Option<PortableV2Error>,
}

impl<'a> InventoryCheck<'a> {
    /// `preceding` is every entry read before the inventory, in read order;
    /// `max_path_bytes` is the bound those entries' paths were admitted under.
    pub(super) fn new(preceding: &'a [Entry], max_path_bytes: usize) -> Self {
        Self {
            preceding,
            max_path_bytes,
            next: 0,
            previous: None,
            row: Vec::new(),
            length: 0,
            failure: None,
        }
    }

    /// Consume the next authenticated inventory bytes.
    pub(super) fn update(&mut self, mut bytes: &[u8]) {
        self.length = self.length.saturating_add(bytes.len() as u64);
        // Longest admissible row without its LF: digest, two spaces, path.
        let row_max = 64 + 2 + self.max_path_bytes;
        while self.failure.is_none() && !bytes.is_empty() {
            let (part, complete) = match bytes.iter().position(|byte| *byte == b'\n') {
                Some(end) => (&bytes[..end], true),
                None => (bytes, false),
            };
            if self.row.len() + part.len() > row_max {
                self.failure = Some(structure("tag manifest record length"));
                return;
            }
            self.row.extend_from_slice(part);
            bytes = &bytes[part.len() + usize::from(complete)..];
            if complete {
                if let Err(error) = self.check_row() {
                    self.failure = Some(error);
                }
                self.row.clear();
            }
        }
    }

    fn check_row(&mut self) -> Result<(), PortableV2Error> {
        if self.row.contains(&b'\r') {
            return Err(structure("tag manifest line ending"));
        }
        let text = std::str::from_utf8(&self.row).map_err(|_| structure("tag manifest UTF-8"))?;
        let (digest, path) = text
            .split_once("  ")
            .ok_or_else(|| structure("tag manifest record"))?;
        validate_path(path, self.max_path_bytes)?;
        let previous = self
            .previous
            .map(|index| self.preceding[index].path.as_str());
        if !sha(digest) || previous >= Some(path) {
            return Err(structure("tag manifest order/duplicate"));
        }
        while self
            .preceding
            .get(self.next)
            .is_some_and(|entry| !entry.path.starts_with("data/"))
        {
            self.next += 1;
        }
        let entry = self.preceding.get(self.next).ok_or_else(mismatch)?;
        if entry.path != path || hex(&entry.digest) != digest {
            return Err(mismatch());
        }
        self.previous = Some(self.next);
        self.next += 1;
        Ok(())
    }

    /// Finish the stream: every `data/` entry must have exactly one row.
    pub(super) fn finish(self) -> InventoryVerdict {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        if self.length == 0 || !self.row.is_empty() {
            return Err(structure("tag manifest termination"));
        }
        if self.preceding[self.next..]
            .iter()
            .any(|entry| entry.path.starts_with("data/"))
        {
            return Err(mismatch());
        }
        Ok(())
    }
}

fn structure(detail: &'static str) -> PortableV2Error {
    PortableV2Error::at(
        PortableV2ErrorCode::InvalidStructure,
        INVENTORY_PATH,
        detail,
    )
}

fn mismatch() -> PortableV2Error {
    PortableV2Error::at(
        PortableV2ErrorCode::DigestMismatch,
        INVENTORY_PATH,
        "data inventory manifest",
    )
}
