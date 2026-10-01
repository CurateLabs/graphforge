//! Range-authentication prototype (#1388): a query pays for the blocks it
//! touches, a same-inode, same-length flip in a touched block is refused, and
//! a flip in an untouched block is never read.
//!
//! The fixture's first construction-published node object holds 32,768 rows
//! (several `PAYLOAD_BLOCK_BYTES` blocks). The generic node scan
//! `MATCH (n) RETURN n.node_uuid AS id LIMIT 3` reads the Parquet footer and
//! the first data page of the `node_uuid` column chunk and nothing else, so:
//!
//! - a flip inside the **last data page** of that column (a page `LIMIT 3`
//!   never reaches) is not read: the query answers correctly and its read
//!   bytes stay well below the object length, while whole-object admission of
//!   the same object refuses (the control that the flip is real);
//! - a flip in the **footer block** or in the **first page's block** is
//!   refused with the block-checksum error.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::PathBuf;

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{GraphForge, LifecycleIoCapture, lifecycle_io_snapshot};
use graphforge_storage::{graph_object_path, resolve_project_generation};
use parquet::file::reader::{FileReader, SerializedFileReader};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const NODES: usize = 1 << 16;
const FAN_OUT: usize = 2;
const BLOCK: u64 = 16 * 1024;
const SCAN: &str = "MATCH (n) RETURN n.node_uuid AS id LIMIT 3";

struct Fixture {
    _project: tempfile::TempDir,
    path: PathBuf,
    object: PathBuf,
    object_length: u64,
    /// Byte range of the last data page of the `node_uuid` column chunk.
    last_page: (u64, u64),
}

fn fixture() -> Fixture {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    bulk_fixture::generate_bulk_graph_with_index(&path, NODES, FAN_OUT, false);
    let generation = resolve_project_generation(&path).expect("project resolves");
    let inventory = generation
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation");
    let mut nodes: Vec<_> = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("topology/nodes/"))
        .collect();
    nodes.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    // The fixture writes 32,768-row windows: the first object holds nodes 0..
    let entry = nodes[0];
    eprintln!(
        "node objects={} first={} ({} bytes)",
        nodes.len(),
        entry.relative_path,
        entry.byte_length
    );
    assert_eq!(
        entry.block_xxh64.len() as u64,
        entry.byte_length.div_ceil(BLOCK),
        "the entry carries a full block table"
    );
    assert!(
        entry.byte_length >= 3 * BLOCK,
        "object must span several blocks: {} bytes",
        entry.byte_length
    );
    let object = graph_object_path(generation.container_root(), &entry.content_sha256).unwrap();
    // Test-side inspection of the layout: plain parquet over the CAS object.
    let reader = SerializedFileReader::new_with_options(
        File::open(&object).unwrap(),
        parquet::file::serialized_reader::ReadOptionsBuilder::new()
            .with_page_index()
            .build(),
    )
    .unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 1);
    let column = metadata.row_group(0).column(0);
    assert_eq!(column.column_path().string(), "node_uuid");
    let offset_index =
        metadata.offset_index().expect("offset index is written")[0][0].page_locations();
    assert!(offset_index.len() >= 2, "several data pages per column");
    let last = &offset_index[offset_index.len() - 1];
    Fixture {
        _project: project,
        path,
        object,
        object_length: entry.byte_length,
        last_page: (
            last.offset as u64,
            last.offset as u64 + last.compressed_page_size as u64,
        ),
    }
}

impl Fixture {
    fn flip(&self, offset: u64) {
        // Content-store objects are installed read-only; mutate in place and
        // restore the mode so nothing but the byte changed.
        let mode = std::fs::metadata(&self.object).unwrap().permissions();
        let mut writable = mode.clone();
        writable.set_readonly(false);
        std::fs::set_permissions(&self.object, writable).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.object)
            .unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        assert_eq!(file.metadata().unwrap().len(), self.object_length);
        drop(file);
        std::fs::set_permissions(&self.object, mode).unwrap();
    }

    /// Open, run the scan, return (result, execution read bytes).
    fn scan(&self) -> (Result<Vec<Vec<u8>>, String>, u64) {
        let _capture = LifecycleIoCapture::install();
        let forge = GraphForge::new(Some(self.path.to_str().unwrap())).unwrap();
        let after_open = lifecycle_io_snapshot().unwrap();
        let pending = graphforge_storage::graph_admission::pending_admissions();
        let result = forge.execute(SCAN).map_err(|error| error.to_string());
        let execution = lifecycle_io_snapshot().unwrap().since(&after_open).unwrap();
        let mut phases: Vec<_> = execution
            .phases
            .iter()
            .filter(|(_, totals)| totals.read_bytes != 0)
            .map(|(phase, totals)| {
                format!(
                    "{phase:?}={}B/{}calls/{}blocks",
                    totals.read_bytes, totals.read_calls, totals.block_count
                )
            })
            .collect();
        phases.sort();
        eprintln!(
            "scan: tickets pending before={pending} after={} phases={phases:?}",
            graphforge_storage::graph_admission::pending_admissions()
        );
        let ids = result.map(|result| {
            let mut ids = Vec::new();
            for batch in &result.batches {
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                ids.extend((0..batch.num_rows()).map(|row| column.value(row).to_vec()));
            }
            ids
        });
        (ids, execution.totals.read_bytes)
    }
}

fn first_three() -> Vec<Vec<u8>> {
    (0..3)
        .map(|node| bulk_fixture::fixture_node_uuid(node).as_bytes().to_vec())
        .collect()
}

/// What a real query touches. The generic scan `MATCH (n) ... LIMIT 3` reads
/// every block of the first node object: the published Parquet objects hold
/// one row group, and the reader consumes the whole column chunk before the
/// limit cuts the stream. Block admission therefore buys nothing on this
/// path (8 of 8 blocks admitted, read bytes about twice the object: the
/// admission pass plus the Parquet reads); a flip anywhere in the object is
/// refused. Recorded as the finding that decides against the format change.
#[test]
fn a_limited_generic_scan_touches_every_block_of_the_object_it_opens() {
    let fixture = fixture();
    let blocks = fixture.object_length.div_ceil(BLOCK);
    let (ids, clean_read) = fixture.scan();
    assert_eq!(ids.unwrap(), first_three());
    eprintln!(
        "clean LIMIT 3 scan read {clean_read} bytes of a {} byte object ({blocks} blocks)",
        fixture.object_length
    );
    assert!(
        clean_read >= fixture.object_length,
        "the single-row-group object is read whole: {clean_read} < {}",
        fixture.object_length
    );
    // A flip in the last data page, which LIMIT 3 never decodes, is still
    // read by the column-chunk reader and refused.
    let offset = fixture.last_page.0 + 5;
    fixture.flip(offset);
    let error = fixture.scan().0.unwrap_err();
    assert!(error.contains("block checksum"), "{error}");
    fixture.flip(offset);
    assert_eq!(fixture.scan().0.unwrap(), first_three(), "restored");
}

/// The block mechanism itself, on the one read shape that touches a strict
/// subset of an object: a footer-only read through `ReadPathFile` (what
/// planning did before row counts came from the shard name). Only the blocks
/// holding the footer are admitted; a flip in the first block is never read,
/// a flip in the footer block is refused.
#[test]
fn a_footer_only_read_admits_only_the_footer_blocks() {
    use graphforge_storage::lifecycle_io::ReadPathFile;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let fixture = fixture();
    let blocks = fixture.object_length.div_ceil(BLOCK);
    assert!(blocks >= 3);

    let footer_rows = |fixture: &Fixture| -> Result<i64, String> {
        let _capture = LifecycleIoCapture::install();
        // Opening registers the hydrated tickets; the handle below is the
        // same inode through its content-store name.
        let _forge = GraphForge::new(Some(fixture.path.to_str().unwrap())).unwrap();
        let before = lifecycle_io_snapshot().unwrap();
        let file = ReadPathFile::admitted(File::open(&fixture.object).unwrap())
            .map_err(|error| error.to_string())?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| error.to_string())?;
        let rows = builder.metadata().file_metadata().num_rows();
        let read = lifecycle_io_snapshot().unwrap().since(&before).unwrap();
        let admitted_blocks: u64 = read.phases.values().map(|totals| totals.block_count).sum();
        eprintln!(
            "footer-only read: {} bytes, {admitted_blocks} of {blocks} blocks admitted",
            read.totals.read_bytes
        );
        assert!(
            admitted_blocks >= 1 && admitted_blocks < blocks,
            "{admitted_blocks} of {blocks}"
        );
        assert!(read.totals.read_bytes < fixture.object_length);
        Ok(rows)
    };

    assert_eq!(footer_rows(&fixture).unwrap(), 32_768);

    // First block: never read by a footer-only read.
    fixture.flip(7);
    assert_eq!(footer_rows(&fixture).unwrap(), 32_768);
    // Control: the flip is real; whole-object admission refuses it.
    {
        let _forge = GraphForge::new(Some(fixture.path.to_str().unwrap())).unwrap();
        let error = graphforge_storage::graph_admission::admit_path(&fixture.object).unwrap_err();
        assert!(error.to_string().contains("XXH64"), "{error}");
    }
    fixture.flip(7);

    // Footer block: refused before any metadata is decoded.
    let footer_offset = fixture.object_length - 3;
    fixture.flip(footer_offset);
    let error = footer_rows(&fixture).unwrap_err();
    assert!(error.contains("block checksum"), "{error}");
    fixture.flip(footer_offset);
    assert_eq!(footer_rows(&fixture).unwrap(), 32_768);
}

/// Criterion 4 for the ladder path: with the node-linear planning term gone,
/// the ordered queries never touch node objects, so a node flip is not what
/// they would refuse; the CSR shard they do read is self-authenticating.
#[test]
fn ordered_queries_never_read_node_objects() {
    let fixture = fixture();
    let _capture = LifecycleIoCapture::install();
    let forge = GraphForge::new(Some(fixture.path.to_str().unwrap())).unwrap();
    let after_open = lifecycle_io_snapshot().unwrap();
    let pending_before = graphforge_storage::graph_admission::pending_admissions();
    forge
        .execute("MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 10")
        .unwrap();
    let execution = lifecycle_io_snapshot().unwrap().since(&after_open).unwrap();
    assert_eq!(
        graphforge_storage::graph_admission::pending_admissions(),
        pending_before,
        "no first-touch ticket was consumed by the ordered query"
    );
    eprintln!("ordered one-hop read {} bytes", execution.totals.read_bytes);
    let inventory = resolve_project_generation(&fixture.path)
        .unwrap()
        .unadmitted_graph_files_inventory()
        .unwrap()
        .unwrap();
    let largest_shard = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.ends_with(".csr"))
        .map(|file| file.byte_length)
        .max()
        .unwrap();
    assert!(
        execution.totals.read_bytes <= largest_shard + 16 * 1024,
        "ordered one-hop read {} bytes against one shard of {largest_shard} plus residual",
        execution.totals.read_bytes
    );
}
