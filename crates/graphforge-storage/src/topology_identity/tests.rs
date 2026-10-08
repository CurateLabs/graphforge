//! Identity probes over the published Parquet prune row groups and pages.

use super::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::EnabledStatistics;
use tempfile::TempDir;

const ROWS: usize = 4_000;
const GROUP_ROWS: usize = 1_000;
const PAGE_ROWS: usize = 100;
const GROUPS: u64 = (ROWS / GROUP_ROWS) as u64;
const PAGES: u64 = (ROWS / PAGE_ROWS) as u64;

/// The `index`th identity: spaced by ten, so `identity(i) + 5` is never live.
fn identity(index: usize) -> Uuid {
    Uuid::from_u128((index as u128 + 1) * 10)
}

fn absent_after(index: usize) -> Uuid {
    Uuid::from_u128((index as u128 + 1) * 10 + 5)
}

/// A sorted fragment of `ROWS` identities in `GROUPS` row groups of
/// `PAGES / GROUPS` pages each, `node_id` equal to the one-based rank.
fn write_fragment(
    dir: &Path,
    uuid_column: &str,
    id_column: Option<&str>,
    statistics: EnabledStatistics,
) -> PathBuf {
    write_fragment_as(dir, uuid_column, id_column, statistics, None, ROWS)
}

/// Like [`write_fragment`], with the statistics truncated at `truncate` bytes
/// and `rows` rows starting at the first identity.
fn write_fragment_as(
    dir: &Path,
    uuid_column: &str,
    id_column: Option<&str>,
    statistics: EnabledStatistics,
    truncate: Option<usize>,
    rows: usize,
) -> PathBuf {
    let mut fields = vec![Field::new(
        uuid_column,
        DataType::FixedSizeBinary(16),
        false,
    )];
    fields.extend(id_column.map(|name| Field::new(name, DataType::UInt64, false)));
    let schema = Arc::new(Schema::new(fields));
    let properties = crate::permanent_parquet::writer_properties()
        .set_max_row_group_row_count(Some(GROUP_ROWS))
        .set_data_page_row_count_limit(PAGE_ROWS)
        .set_write_batch_size(PAGE_ROWS / 2)
        .set_statistics_enabled(statistics)
        .set_statistics_truncate_length(truncate)
        .set_column_index_truncate_length(truncate)
        .build();
    let path = dir.join(format!("{uuid_column}.parquet"));
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    for start in (0..rows).step_by(PAGE_ROWS) {
        let rows = start..start + PAGE_ROWS;
        let uuids = FixedSizeBinaryArray::try_from_iter(
            rows.clone().map(|row| identity(row).as_bytes().to_vec()),
        )
        .unwrap();
        let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(uuids)];
        if id_column.is_some() {
            columns.push(Arc::new(UInt64Array::from_iter_values(
                rows.map(|row| row as u64 + 1),
            )));
        }
        writer
            .write(&RecordBatch::try_new(Arc::clone(&schema), columns).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
    path
}

fn probe_over(nodes: Vec<Arc<Fragment>>, edges: Vec<Arc<Fragment>>) -> TopologyIdentityProbe {
    TopologyIdentityProbe {
        nodes,
        edges,
        deleted: Arc::new(Vec::new()),
        generation: 1,
        _leases: Arc::new(Vec::new()),
        authenticated_bytes: 0,
        authenticated_objects: 0,
    }
}

fn node_probe(dir: &Path) -> TopologyIdentityProbe {
    let path = write_fragment(dir, "node_uuid", Some("node_id"), EnabledStatistics::Page);
    probe_over(
        vec![load_fragment(&path, "node_uuid", Some("node_id")).unwrap()],
        Vec::new(),
    )
}

#[test]
fn the_fixture_carries_the_page_layout_the_pruning_tests_depend_on() {
    let dir = TempDir::new().unwrap();
    let probe = node_probe(dir.path());
    let fragment = &probe.nodes[0];
    assert_eq!(fragment.groups.len() as u64, GROUPS);
    for group in fragment.groups.iter() {
        assert_eq!(group.pages.len() as u64, PAGES / GROUPS);
        assert_eq!(group.chunks.len(), 2, "uuid and node_id page layouts");
    }
}

#[test]
fn a_candidate_set_inside_one_page_decodes_exactly_that_page() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let candidates = [identity(1_205), identity(1_250), identity(1_299)];
    let (surrogates, metrics) = probe.lookup_node_surrogates(&candidates).unwrap();
    assert_eq!(surrogates, [Some(1_206), Some(1_251), Some(1_300)]);
    assert_eq!(metrics.found, 3);
    assert_eq!(metrics.pages_considered, PAGES);
    assert_eq!(metrics.pages_read, 1, "one 100-row page of forty");
    assert_eq!(metrics.identity_blocks_read, 1);
    assert_eq!(metrics.file_seeks, 1);
    assert_eq!(metrics.per_record_seeks, 0);
    let whole_group = probe.nodes[0].groups[1].probe_bytes;
    // The chunk's dictionary page is read with any page of it, so a page of
    // ten does not cost a tenth of its row group; it still costs far less.
    assert!(
        metrics.identity_bytes_read * 2 < whole_group,
        "one page of ten reads less than half its row group: {} of {whole_group}",
        metrics.identity_bytes_read
    );
}

#[test]
fn candidates_in_two_row_groups_decode_one_page_in_each() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let (present, metrics) = probe
        .probe(UuidIndexKind::Node, &[identity(5), identity(3_999)])
        .unwrap();
    assert_eq!(present, [true, true]);
    assert_eq!(metrics.pages_read, 2);
    assert_eq!(metrics.identity_blocks_read, 2);
}

#[test]
fn a_candidate_in_a_gap_between_pages_decodes_nothing() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    // Inside the row group's bounds, below the next page's minimum and above
    // the previous page's maximum: only the page index can rule it out.
    let (present, metrics) = probe
        .probe(UuidIndexKind::Node, &[absent_after(199)])
        .unwrap();
    assert_eq!(present, [false]);
    assert_eq!(metrics.pages_considered, PAGES);
    assert_eq!(metrics.pages_read, 0);
    assert_eq!(metrics.identity_blocks_read, 0);
    assert_eq!(metrics.identity_bytes_read, 0);
}

#[test]
fn an_absent_candidate_inside_a_page_is_decoded_and_refused() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let (present, metrics) = probe
        .probe(UuidIndexKind::Node, &[absent_after(250)])
        .unwrap();
    assert_eq!(present, [false]);
    assert_eq!(metrics.pages_read, 1);
}

#[test]
fn a_candidate_beyond_every_row_group_decodes_nothing() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let (present, metrics) = probe
        .probe(UuidIndexKind::Node, &[identity(ROWS + 7)])
        .unwrap();
    assert_eq!(present, [false]);
    assert_eq!(metrics.pages_read, 0);
}

#[test]
fn candidates_across_every_page_decode_every_page() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let candidates = (0..ROWS)
        .step_by(PAGE_ROWS)
        .map(identity)
        .collect::<Vec<_>>();
    let (present, metrics) = probe.probe(UuidIndexKind::Node, &candidates).unwrap();
    assert!(present.iter().all(|present| *present));
    assert_eq!(metrics.pages_read, PAGES);
    assert_eq!(metrics.identity_blocks_read, GROUPS);
}

#[test]
fn edge_probes_prune_pages_without_a_surrogate_column() {
    let dir = TempDir::new().unwrap();
    let path = write_fragment(dir.path(), "edge_uuid", None, EnabledStatistics::Page);
    let mut probe = probe_over(
        Vec::new(),
        vec![load_fragment(&path, "edge_uuid", None).unwrap()],
    );
    let (present, metrics) = probe
        .probe(UuidIndexKind::Edge, &[identity(2_000), absent_after(2_001)])
        .unwrap();
    assert_eq!(present, [true, false]);
    assert_eq!(metrics.pages_read, 1);
    assert_eq!(metrics.pages_considered, PAGES);
}

#[test]
fn a_file_without_a_page_index_is_pruned_by_row_group_alone() {
    let dir = TempDir::new().unwrap();
    let path = write_fragment(
        dir.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::Chunk,
    );
    let fragment = load_fragment(&path, "node_uuid", Some("node_id")).unwrap();
    assert!(fragment.groups.iter().all(|group| group.pages.is_empty()));
    let mut probe = probe_over(vec![fragment], Vec::new());
    let (surrogates, metrics) = probe.lookup_node_surrogates(&[identity(2_500)]).unwrap();
    assert_eq!(surrogates, [Some(2_501)]);
    assert_eq!(metrics.pages_considered, GROUPS, "a group is one page");
    assert_eq!(metrics.pages_read, 1);
    assert_eq!(metrics.identity_blocks_read, 1);
}

#[test]
fn page_pruning_reduces_the_bytes_the_file_system_serves() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let file_len = std::fs::metadata(dir.path().join("node_uuid.parquet"))
        .unwrap()
        .len();
    let read_bytes = |probe: &mut TopologyIdentityProbe, candidates: &[Uuid]| {
        let _capture = crate::lifecycle_io::CaptureScope::install();
        let before = crate::lifecycle_io::snapshot().expect("requested measurement");
        probe.lookup_node_surrogates(candidates).unwrap();
        crate::lifecycle_io::snapshot()
            .expect("requested measurement")
            .since(&before)
            .unwrap()
            .phases[&crate::StorageIoPhase::ReadPathScan]
            .read_bytes
    };
    let narrow = read_bytes(&mut probe, &[identity(1_250)]);
    let every_page = (0..ROWS)
        .step_by(PAGE_ROWS)
        .map(identity)
        .collect::<Vec<_>>();
    let wide = read_bytes(&mut probe, &every_page);
    assert!(narrow > 0, "the probe's reads reach the lifecycle counters");
    assert!(
        narrow * 5 < wide,
        "one page of forty must not read like all of them: {narrow} against {wide}"
    );
    assert!(narrow * 5 < file_len, "{narrow} of a {file_len} byte file");
}

#[test]
fn a_cached_footer_never_sends_a_probe_to_the_path_it_was_first_read_from() {
    // Hydration hard-links one content-addressed object into every workspace, so
    // the process-wide footer cache sees the same file under many paths. A probe
    // built over a later workspace must read that workspace, not the first.
    let first = TempDir::new().unwrap();
    let path = write_fragment(
        first.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::Page,
    );
    let _warmed = load_fragment(&path, "node_uuid", Some("node_id")).unwrap();

    let second = TempDir::new().unwrap();
    let linked = second.path().join("node_uuid.parquet");
    std::fs::hard_link(&path, &linked).unwrap();
    drop(first);
    assert!(!path.exists());

    let mut probe = probe_over(
        vec![load_fragment(&linked, "node_uuid", Some("node_id")).unwrap()],
        Vec::new(),
    );
    let (surrogates, metrics) = probe.lookup_node_surrogates(&[identity(2_500)]).unwrap();
    assert_eq!(surrogates, [Some(2_501)]);
    assert_eq!(metrics.pages_read, 1);
}

#[test]
fn absent_statistics_make_every_group_and_page_possible_and_the_answer_still_exact() {
    let dir = TempDir::new().unwrap();
    let path = write_fragment_as(
        dir.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::None,
        None,
        ROWS,
    );
    let fragment = load_fragment(&path, "node_uuid", Some("node_id")).unwrap();
    assert!(fragment.groups.iter().all(|group| group.bounds.is_none()));
    assert!(fragment.groups.iter().all(|group| group.pages.is_empty()));
    let mut probe = probe_over(vec![fragment], Vec::new());
    let (surrogates, metrics) = probe
        .lookup_node_surrogates(&[identity(3_500), absent_after(10)])
        .unwrap();
    assert_eq!(surrogates, [Some(3_501), None]);
    // Nothing can be ruled out: every row group is decoded.
    assert_eq!(metrics.identity_blocks_read, GROUPS);
}

#[test]
fn truncated_statistics_are_not_trusted_for_pruning() {
    // A bound cut short of a whole UUID is only a prefix; pruning by it could
    // exclude a row group that holds the candidate.
    let dir = TempDir::new().unwrap();
    let path = write_fragment_as(
        dir.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::Page,
        Some(8),
        ROWS,
    );
    let fragment = load_fragment(&path, "node_uuid", Some("node_id")).unwrap();
    assert!(fragment.groups.iter().all(|group| group.bounds.is_none()));
    let mut probe = probe_over(vec![fragment], Vec::new());
    let (surrogates, metrics) = probe.lookup_node_surrogates(&[identity(1_234)]).unwrap();
    assert_eq!(surrogates, [Some(1_235)]);
    assert_eq!(metrics.identity_blocks_read, GROUPS);
}

#[test]
fn a_fragment_replaced_after_its_footer_was_read_is_refused() {
    let dir = TempDir::new().unwrap();
    let mut probe = node_probe(dir.path());
    let path = dir.path().join("node_uuid.parquet");
    // Another file at the same path, as a rewrite that renames over it leaves.
    let other = TempDir::new().unwrap();
    let replacement = write_fragment_as(
        other.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::Page,
        None,
        ROWS / 2,
    );
    std::fs::rename(&replacement, &path).unwrap();
    let error = probe
        .lookup_node_surrogates(&[identity(10)])
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("changed since its footer was read"),
        "{error}"
    );
}

#[test]
fn a_uuid_held_by_two_fragments_is_corrupt_topology() {
    let dir = TempDir::new().unwrap();
    let first = write_fragment(
        dir.path(),
        "node_uuid",
        Some("node_id"),
        EnabledStatistics::Page,
    );
    let copy = dir.path().join("second.parquet");
    std::fs::copy(&first, &copy).unwrap();
    let mut probe = probe_over(
        vec![
            load_fragment(&first, "node_uuid", Some("node_id")).unwrap(),
            load_fragment(&copy, "node_uuid", Some("node_id")).unwrap(),
        ],
        Vec::new(),
    );
    let error = probe
        .lookup_node_surrogates(&[identity(10)])
        .unwrap_err()
        .to_string();
    assert!(error.contains("more than one topology row"), "{error}");
}
