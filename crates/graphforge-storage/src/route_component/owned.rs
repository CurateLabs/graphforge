//! Explicit admission of a private mutable graph workspace.
//!
//! This authority is never used to infer the layout of a published generation.
//! Its caller owns the workspace; published inputs must first pass versioned
//! inventory authentication and materialization.

use std::path::Path;

use graphforge_core::GfError;
use graphforge_filesystem::StableDirectory;

use super::{RouteTable, TABLE_FILE, encode_relative_route, invalid, limit};

const MAX_TABLE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_ROUTES: u64 = 100_000;
// The existing rewrite intent permits 16,384 entries, including its table and
// generation authority. Refuse before staging if one atomic migration cannot fit.
const MAX_MOVED_FILES: usize = 16_382;

pub(crate) fn admit_owned_workspace(root: &Path) -> Result<RouteTable, GfError> {
    // Recovery precedes both table detection and catalog reads: a crash may
    // have installed route files before installing the table in the same intent.
    let absolute =
        std::path::absolute(root).map_err(|_| invalid("owned graph root is not absolute"))?;
    let root = absolute.as_path();
    crate::durable_rewrite::prepare_owned_layout(
        root,
        |directory| prepare_owned_workspace(root, directory),
        |(table, changed)| {
            if changed {
                let (mapped, _) = crate::graph_files::capture_graph_files(root)?;
                crate::graph_files::authenticate_route_table(root, &mapped)
            } else {
                Ok(table)
            }
        },
    )
}

fn prepare_owned_workspace(
    root: &Path,
    directory: &StableDirectory,
) -> Result<(crate::RewriteBatch, (RouteTable, bool)), GfError> {
    if let Some(table) = read_owned_layout_table(directory)? {
        // A live caller may already own another RewriteBatch. Its exact temporary
        // identities are available at commit, not at this writer-open boundary.
        // Batch preparation still authenticates the complete payload/table closure.
        return Ok((crate::RewriteBatch::new(), (table, false)));
    }
    remove_owned_migration_temps(directory)?;
    remove_owned_table_temps(root)?;
    let inventory = crate::graph_files::capture_owned_route_migration_inventory(root)?;
    if inventory.format_version == crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION {
        return Ok((
            crate::RewriteBatch::new(),
            (
                crate::graph_files::authenticate_route_table(root, &inventory)?,
                false,
            ),
        ));
    }
    let mut table = RouteTable::default();
    let mut moves = Vec::new();
    for entry in &inventory.files {
        let destination = encode_relative_route(
            &entry.relative_path,
            &mut table,
            MAX_TABLE_BYTES,
            MAX_ROUTES,
        )?;
        if destination != entry.relative_path {
            if moves.len() == MAX_MOVED_FILES {
                return Err(limit("semantic route migration exceeds atomic file budget"));
            }
            let source = crate::graph_files::resolve_v1_inventory_entry_retained(root, entry)?;
            moves.push((source, destination, entry));
        }
    }
    let table_bytes = table.encode(MAX_TABLE_BYTES)?;
    let mut staged = crate::RewriteBatch::new();
    for (source, relative, expected) in moves {
        create_destination_parent(directory, &relative)?;
        let destination = root.join(relative);
        #[cfg(test)]
        BEFORE_STAGE.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
        staged.stage_semantic_route_move(root, directory, &source.path, &destination)?;
        authenticate_staged_inventory_copy(root, &staged, &destination, &source, expected)?;
    }
    staged.stage_named_control_bytes(
        &root.join(TABLE_FILE),
        &table_bytes,
        "semantic-routes.json.migration.",
    )?;
    Ok((staged, (table, true)))
}

pub(crate) fn read_owned_layout_table(
    root: &StableDirectory,
) -> Result<Option<RouteTable>, GfError> {
    use std::io::Read;
    let mut file = match root.open_child_file(std::ffi::OsStr::new(TABLE_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(invalid("owned route authority cannot be opened")),
    };
    if graphforge_filesystem::file_link_count(&file).ok() != Some(1) {
        return Err(invalid("owned route authority has ambiguous ownership"));
    }
    let identity = graphforge_filesystem::file_identity(&file)
        .map_err(|_| invalid("owned route authority identity failed"))?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(MAX_TABLE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("owned route authority read failed"))?;
    let table = RouteTable::decode(&bytes, MAX_TABLE_BYTES, MAX_ROUTES)?;
    let named = root
        .open_child_file(std::ffi::OsStr::new(TABLE_FILE))
        .map_err(|_| invalid("owned route authority disappeared"))?;
    if graphforge_filesystem::file_identity(&named).ok() != Some(identity)
        || graphforge_filesystem::file_identity(&file).ok() != Some(identity)
        || graphforge_filesystem::file_link_count(&named).ok() != Some(1)
    {
        return Err(invalid("owned route authority changed during admission"));
    }
    Ok(Some(table))
}

fn remove_owned_migration_temps(root: &StableDirectory) -> Result<(), GfError> {
    fn visit(
        directory: &StableDirectory,
        depth: usize,
        visited: &mut usize,
    ) -> Result<(), GfError> {
        if depth > 32 {
            return Err(limit("migration cleanup depth exceeded"));
        }
        let names = directory
            .child_names_bounded(100_000usize.saturating_sub(*visited))
            .map_err(|_| limit("migration cleanup entry budget exceeded"))?;
        *visited = visited
            .checked_add(names.len())
            .ok_or_else(|| limit("migration cleanup count overflow"))?;
        if *visited > 100_000 {
            return Err(limit("migration cleanup entry budget exceeded"));
        }
        for name in names {
            if let Ok(child) = directory.open_child_directory(&name) {
                visit(&child, depth + 1, visited)?;
                continue;
            }
            let owned = name
                .to_str()
                .and_then(|name| name.strip_prefix("semantic-route-move.parquet."))
                .and_then(|name| name.strip_suffix(".tmp"))
                .is_some_and(|random| {
                    !random.is_empty() && random.bytes().all(|byte| byte.is_ascii_alphanumeric())
                });
            if !owned {
                continue;
            }
            let file = directory
                .open_child_file(&name)
                .map_err(|_| invalid("migration temporary is not regular"))?;
            if graphforge_filesystem::file_link_count(&file).ok() != Some(1) {
                return Err(invalid("migration temporary has ambiguous ownership"));
            }
            let identity = graphforge_filesystem::file_identity(&file)
                .map_err(|_| invalid("migration temporary identity failed"))?;
            drop(file);
            directory
                .unlink_child_if_identity(&name, identity)
                .map_err(|_| invalid("migration temporary cleanup failed"))?;
            directory
                .sync()
                .map_err(|_| invalid("migration cleanup sync failed"))?;
        }
        Ok(())
    }
    visit(root, 0, &mut 0)
}

fn create_destination_parent(root: &StableDirectory, relative: &str) -> Result<(), GfError> {
    let mut directory = root
        .try_clone()
        .map_err(|_| invalid("owned graph root changed"))?;
    let parts = relative.split('/').collect::<Vec<_>>();
    for name in &parts[..parts.len() - 1] {
        directory = directory
            .create_child_directory(std::ffi::OsStr::new(name))
            .map_err(|_| invalid("owned route destination parent admission failed"))?;
    }
    directory
        .sync()
        .map_err(|_| invalid("owned route destination parent sync failed"))
}

fn authenticate_staged_inventory_copy(
    root: &Path,
    staged: &crate::RewriteBatch,
    destination: &Path,
    source: &crate::graph_files::RetainedV1InventoryEntry,
    expected: &crate::GraphFileEntry,
) -> Result<(), GfError> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let current = crate::graph_files::resolve_v1_inventory_entry_retained(root, expected)?;
    if current.identity != source.identity {
        return Err(invalid(
            "semantic route source identity changed before staging",
        ));
    }
    let temporary = staged
        .staged_temp(destination)
        .ok_or_else(|| invalid("semantic route staged copy is absent"))?;
    let parent = StableDirectory::open(
        temporary
            .parent()
            .ok_or_else(|| invalid("semantic route staged copy has no parent"))?,
    )
    .map_err(|_| invalid("semantic route staged copy parent cannot be retained"))?;
    let mut file = parent
        .open_child_file(
            temporary
                .file_name()
                .ok_or_else(|| invalid("semantic route staged copy has no name"))?,
        )
        .map_err(|_| {
            invalid("semantic route staged copy cannot be opened without following links")
        })?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 65536].into_boxed_slice();
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| invalid("semantic route staged copy read failed"))?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| limit("semantic route staged length overflow"))?;
        if bytes > expected.byte_length {
            return Err(invalid("semantic route staged copy length changed"));
        }
        digest.update(&buffer[..count]);
    }
    let actual = digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
            output
        });
    if bytes != expected.byte_length || actual != expected.content_sha256 {
        return Err(invalid(
            "semantic route staged copy differs from admitted inventory",
        ));
    }
    Ok(())
}

fn remove_owned_table_temps(root: &Path) -> Result<(), GfError> {
    let directory = StableDirectory::open(root)
        .map_err(|_| invalid("owned route cleanup root admission failed"))?;
    for name in directory
        .child_names_bounded(100_000)
        .map_err(|_| limit("owned route cleanup entry budget exceeded"))?
    {
        let recognized = name
            .to_str()
            .and_then(|name| name.strip_prefix("semantic-routes.json.migration."))
            .and_then(|name| name.strip_suffix(".tmp"))
            .is_some_and(|random| {
                !random.is_empty() && random.bytes().all(|byte| byte.is_ascii_alphanumeric())
            });
        if !recognized {
            continue;
        }
        let file = directory
            .open_child_file(&name)
            .map_err(|_| invalid("owned route temporary is not a regular file"))?;
        if graphforge_filesystem::file_link_count(&file).ok() != Some(1) {
            return Err(invalid("owned route temporary has ambiguous ownership"));
        }
        let identity = graphforge_filesystem::file_identity(&file)
            .map_err(|_| invalid("owned route temporary identity failed"))?;
        drop(file);
        directory
            .unlink_child_if_identity(&name, identity)
            .map_err(|_| invalid("owned route temporary changed during cleanup"))?;
    }
    directory
        .sync()
        .map_err(|_| invalid("owned route cleanup sync failed"))
}

#[cfg(test)]
thread_local! {
    static BEFORE_STAGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use std::sync::Arc;

    fn write_legacy_fixture(root: &Path, route: &str) -> Vec<u8> {
        let property = root.join("properties").join(format!("{route}.parquet"));
        std::fs::create_dir_all(property.parent().unwrap()).unwrap();
        let ids = FixedSizeBinaryArray::try_from_iter([[7_u8; 16]].into_iter()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("rank", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids), Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&property).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let edge = root.join("topology/edges/REL.parquet");
        std::fs::create_dir_all(edge.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(FixedSizeBinaryArray::try_from_iter([[9_u8; 16]].into_iter()).unwrap()),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(edge).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::read(property).unwrap()
    }

    #[test]
    fn concurrent_legacy_admissions_keep_staging_under_rewrite_guard() {
        let root = tempfile::tempdir().unwrap();
        let payload = write_legacy_fixture(root.path(), "Legacy");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let path = root.path();
            let first_barrier = Arc::clone(&barrier);
            let first = scope.spawn(move || {
                BEFORE_STAGE.with(|slot| {
                    *slot.borrow_mut() = Some(Box::new(move || {
                        // Observe the process-wide rewrite-lock flag at this hook.
                        // The joined admissions and final inventory below verify the
                        // concurrent outcome; this flag alone is not ownership proof.
                        assert!(crate::durable_rewrite::rewrite_lock_is_held());
                        entered_tx.send(()).unwrap();
                        first_barrier.wait();
                    }));
                });
                admit_owned_workspace(path).unwrap()
            });
            entered_rx.recv().unwrap();
            let second = scope.spawn(move || {
                barrier.wait();
                admit_owned_workspace(path).unwrap()
            });
            assert_eq!(
                first.join().unwrap().encode(MAX_TABLE_BYTES).unwrap(),
                second.join().unwrap().encode(MAX_TABLE_BYTES).unwrap()
            );
        });
        let (inventory, _) = crate::graph_files::capture_graph_files(root.path()).unwrap();
        crate::graph_files::authenticate_route_table(root.path(), &inventory).unwrap();
        assert!(!root.path().join("properties/Legacy.parquet").exists());
        assert_eq!(
            std::fs::read(
                root.path()
                    .join("properties")
                    .join(format!("{}.parquet", super::super::component("Legacy")))
            )
            .unwrap(),
            payload
        );
    }

    #[test]
    fn legacy_cas_materialization_translates_routes_before_creating_paths() {
        let fixture = tempfile::tempdir().unwrap();
        let payload = write_legacy_fixture(fixture.path(), "Legacy");
        let cas = tempfile::tempdir().unwrap();
        let (digest, _) = crate::install_graph_object_bytes(cas.path(), &payload).unwrap();
        let source_path = crate::graph_object_path(cas.path(), &digest).unwrap();
        let source_identity = graphforge_filesystem::path_identity(&source_path).unwrap();
        let long = "長".repeat(2048);
        let names = ["CON", "con", "A\\B", "ß", long.as_str()];
        let mut files = names
            .iter()
            .map(|route| crate::GraphFileEntry {
                relative_path: format!("properties/{route}.parquet"),
                byte_length: payload.len() as u64,
                content_sha256: digest.clone(),
                role: crate::GraphFileRole::Properties,
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        let inventory = crate::graph_files::inventory_from_entries(files).unwrap();
        let owner = tempfile::tempdir().unwrap();
        let target = owner.path().join("private");
        let evidence = crate::materialize_graph_objects(cas.path(), &inventory, &target).unwrap();
        assert_eq!(evidence.files_copied, names.len() as u64);
        assert_eq!(evidence.files_reused, 0);
        let mapped = crate::capture_graph_files(&target).unwrap().0;
        assert_eq!(
            mapped.format_version,
            crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
        );
        let table = crate::graph_files::authenticate_route_table(&target, &mapped).unwrap();
        for route in names {
            let component = super::super::component(route);
            assert_eq!(table.route(&component).unwrap(), route);
            let path = target
                .join("properties")
                .join(format!("{component}.parquet"));
            assert_eq!(std::fs::read(&path).unwrap(), payload);
            assert_ne!(
                graphforge_filesystem::path_identity(&path).unwrap(),
                source_identity
            );
        }
        drop(
            crate::GraphWriter::open_at(&target, graphforge_core::OntologyMode::Strict, 0).unwrap(),
        );
        assert_eq!(
            std::fs::read(target.join("topology/generation.json"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        drop(
            crate::GraphWriter::open_at(&target, graphforge_core::OntologyMode::Strict, 1).unwrap(),
        );
        assert_eq!(
            std::fs::read(target.join("topology/generation.json"))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert_eq!(std::fs::read(&source_path).unwrap(), payload);
        assert_eq!(
            graphforge_filesystem::path_identity(&source_path).unwrap(),
            source_identity
        );
    }

    #[test]
    fn legacy_expanded_materialization_installs_mapped_owned_authority() {
        let source = tempfile::tempdir().unwrap();
        let payload = write_legacy_fixture(source.path(), "Legacy");
        let inventory = crate::capture_graph_files(source.path()).unwrap().0;
        assert_eq!(inventory.format_version, crate::GRAPH_FILES_RECORD_VERSION);
        let owner = tempfile::tempdir().unwrap();
        let target = owner.path().join("private");
        crate::materialize_graph_tree(source.path(), &inventory, &target).unwrap();
        assert_eq!(
            std::fs::read(
                target
                    .join("properties")
                    .join(format!("{}.parquet", super::super::component("Legacy")))
            )
            .unwrap(),
            payload
        );
        assert!(!target.join("properties/Legacy.parquet").exists());
        drop(
            crate::GraphWriter::open_at(&target, graphforge_core::OntologyMode::Strict, 0).unwrap(),
        );
        assert_eq!(
            std::fs::read(source.path().join("properties/Legacy.parquet")).unwrap(),
            payload
        );
        crate::verify_graph_tree(source.path(), &inventory).unwrap();
    }

    #[test]
    fn bare_writer_migrates_legacy_routes_and_reopens_exact_bytes_and_ids() {
        let root = tempfile::tempdir().unwrap();
        let original = write_legacy_fixture(root.path(), "Legacy");
        drop(
            crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Strict, 0)
                .unwrap(),
        );
        let mapped = root
            .path()
            .join("properties")
            .join(format!("{}.parquet", super::super::component("Legacy")));
        assert!(!root.path().join("properties/Legacy.parquet").exists());
        assert!(!root.path().join("topology/edges/REL.parquet").exists());
        assert_eq!(std::fs::read(&mapped).unwrap(), original);
        let generation = std::fs::read(root.path().join("topology/generation.json")).unwrap();
        drop(
            crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Strict, 1)
                .unwrap(),
        );
        assert_eq!(
            std::fs::read(root.path().join("topology/generation.json")).unwrap(),
            generation
        );
        assert_eq!(std::fs::read(&mapped).unwrap(), original);
        assert_eq!(crate::catalog::max_edge_id(root.path()).unwrap(), 1);
        let mut reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&mapped).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(ids.null_count(), 0);
        assert_eq!(ids.value(0), &[7_u8; 16]);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        let (inventory, participant) = crate::capture_graph_files(root.path()).unwrap();
        assert_eq!(
            inventory.format_version,
            crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
        );
        assert_eq!(
            participant.record_version,
            crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
        );
        let table = crate::graph_files::authenticate_route_table(root.path(), &inventory).unwrap();
        assert_eq!(
            table.route(&super::super::component("Legacy")).unwrap(),
            "Legacy"
        );
        assert_eq!(table.route(&super::super::component("REL")).unwrap(), "REL");
    }
    #[test]
    fn inventory_source_substitution_before_stage_cannot_be_recaptured() {
        for same_bytes in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let original = write_legacy_fixture(root.path(), "Legacy");
            let source = root.path().join("properties/Legacy.parquet");
            let saved = root.path().join("saved-original.parquet");
            let substitute = source.clone();
            BEFORE_STAGE.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    std::fs::rename(&substitute, &saved).unwrap();
                    std::fs::write(
                        substitute,
                        if same_bytes {
                            original.as_slice()
                        } else {
                            b"substituted"
                        },
                    )
                    .unwrap();
                }))
            });
            let error =
                crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Strict, 0)
                    .err()
                    .expect("substitution must refuse");
            assert!(matches!(
                error,
                GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
                    ..
                }
            ));
            assert!(source.exists());
            assert!(!root.path().join(TABLE_FILE).exists());
            assert!(
                !root
                    .path()
                    .join("properties")
                    .join(format!("{}.parquet", super::super::component("Legacy")))
                    .exists()
            );
        }
    }

    #[test]
    fn subprocess_migration_crash_reopens_and_preserves_unknown_neighbor() {
        const CHILD: &str = "GRAPHFORGE_ROUTE_MIGRATION_CHILD";
        if let Ok(root) = std::env::var(CHILD) {
            let _ = crate::GraphWriter::open_at(
                Path::new(&root),
                graphforge_core::OntologyMode::Strict,
                0,
            );
            panic!("migration failpoint did not terminate child");
        }
        for phase in [
            "rewrite.before_intent",
            "rewrite.after_preparing_disarm",
            "rewrite.after_move_install",
            "rewrite.after_source_retirement",
            "rewrite.before_generation_authority",
        ] {
            let root = tempfile::tempdir().unwrap();
            let original = write_legacy_fixture(root.path(), "Legacy");
            let neighbor = root.path().join("semantic-routes.json.unknown-partial.tmp");
            std::fs::write(&neighbor, b"not an owned temporary").unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "route_component::owned::tests::subprocess_migration_crash_reopens_and_preserves_unknown_neighbor", "--nocapture"])
                .env(CHILD, root.path())
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", "graphforge-internal-subprocess-v1")
                .env("GRAPHFORGE_PROJECT_FAILPOINT", phase).status().unwrap();
            assert_eq!(
                status.code(),
                Some(crate::project_failpoint::exit_code()),
                "{phase}"
            );
            drop(
                crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Strict, 1)
                    .unwrap(),
            );
            drop(
                crate::GraphWriter::open_at(root.path(), graphforge_core::OntologyMode::Strict, 2)
                    .unwrap(),
            );
            assert_eq!(std::fs::read(&neighbor).unwrap(), b"not an owned temporary");
            assert!(!root.path().join("properties/Legacy.parquet").exists());
            assert_eq!(
                std::fs::read(
                    root.path()
                        .join("properties")
                        .join(format!("{}.parquet", super::super::component("Legacy")))
                )
                .unwrap(),
                original
            );
            let (inventory, _) = crate::capture_graph_files(root.path()).unwrap();
            assert_eq!(
                inventory.format_version,
                crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
            );
        }
    }
    #[test]
    fn rewrite_baseline_excludes_only_exact_retained_temporary_routes() {
        let root = tempfile::tempdir().unwrap();
        admit_owned_workspace(root.path()).unwrap();
        let before = crate::capture_graph_files(root.path()).unwrap().0;
        let mut batch = crate::RewriteBatch::new();
        let component = batch.route_component(root.path(), "CON").unwrap();
        let output = root
            .path()
            .join("properties")
            .join(format!("{component}.parquet"));
        batch.stage_bytes(&output, b"payload").unwrap();
        assert_eq!(
            crate::graph_files::capture_rewrite_baseline(root.path(), &batch)
                .unwrap()
                .0,
            before
        );
        let temporary = batch.staged_temp(&output).unwrap().to_path_buf();
        let unknown = root.path().join("properties/unknown.partial.tmp");
        std::fs::hard_link(&temporary, &unknown).unwrap();
        assert!(crate::graph_files::capture_rewrite_baseline(root.path(), &batch).is_err());
        std::fs::remove_file(&unknown).unwrap();
        std::fs::write(&unknown, b"unrecognized partial").unwrap();
        assert!(crate::graph_files::capture_rewrite_baseline(root.path(), &batch).is_err());
        std::fs::remove_file(&unknown).unwrap();
        std::fs::rename(&temporary, root.path().join("retained-original")).unwrap();
        std::fs::write(&temporary, b"payload").unwrap();
        assert!(crate::graph_files::capture_rewrite_baseline(root.path(), &batch).is_err());
    }

    #[test]
    fn route_commit_refuses_linked_authority_and_unregistered_outputs() {
        let root = tempfile::tempdir().unwrap();
        admit_owned_workspace(root.path()).unwrap();
        let before = std::fs::read(root.path().join(TABLE_FILE)).unwrap();
        for linked in [true, false] {
            let mut batch = crate::RewriteBatch::new();
            let registered = batch.route_component(root.path(), "registered").unwrap();
            let component = if linked {
                registered
            } else {
                super::super::component("unknown")
            };
            let output = root
                .path()
                .join("properties")
                .join(format!("{component}.parquet"));
            batch.stage_bytes(&output, b"payload").unwrap();
            let alias = root.path().join("table-alias");
            if linked {
                std::fs::hard_link(root.path().join(TABLE_FILE), &alias).unwrap();
            }
            assert!(matches!(
                batch.commit_at(root.path()),
                Err(GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
                    ..
                })
            ));
            assert!(!output.exists());
            assert_eq!(std::fs::read(root.path().join(TABLE_FILE)).unwrap(), before);
            if linked {
                std::fs::remove_file(alias).unwrap();
            }
        }
    }

    #[test]
    fn route_authority_reverification_refuses_new_hardlink() {
        let root = tempfile::tempdir().unwrap();
        admit_owned_workspace(root.path()).unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let mut routes = super::PendingRoutes::default();
        let component = routes.register(root.path(), "registered").unwrap();
        let mut batch = crate::RewriteBatch::new();
        batch
            .stage_bytes(
                &root
                    .path()
                    .join("properties")
                    .join(format!("{component}.parquet")),
                b"payload",
            )
            .unwrap();
        let prior = routes
            .prepare(root.path(), &directory, &mut batch)
            .unwrap()
            .unwrap();
        std::fs::hard_link(
            root.path().join(TABLE_FILE),
            root.path().join("table-alias"),
        )
        .unwrap();
        assert!(matches!(
            prior.verify(&directory),
            Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
                ..
            })
        ));
    }

    #[test]
    fn concurrent_batches_extend_current_table_and_reject_independent_override() {
        let root = tempfile::tempdir().unwrap();
        admit_owned_workspace(root.path()).unwrap();
        let mut first = crate::RewriteBatch::new();
        let mut second = crate::RewriteBatch::new();
        for (batch, route) in [(&mut first, "CON"), (&mut second, "con")] {
            let component = batch.route_component(root.path(), route).unwrap();
            batch
                .stage_bytes(
                    &root
                        .path()
                        .join("properties")
                        .join(format!("{component}.parquet")),
                    route.as_bytes(),
                )
                .unwrap();
        }
        first.commit_at(root.path()).unwrap();
        second.commit_at(root.path()).unwrap();
        let (inventory, _) = crate::capture_graph_files(root.path()).unwrap();
        let table = crate::graph_files::authenticate_route_table(root.path(), &inventory).unwrap();
        assert_eq!(table.route(&super::super::component("CON")).unwrap(), "CON");
        assert_eq!(table.route(&super::super::component("con")).unwrap(), "con");
        let before = std::fs::read(root.path().join(TABLE_FILE)).unwrap();
        let mut invalid_batch = crate::RewriteBatch::new();
        let component = invalid_batch.route_component(root.path(), "third").unwrap();
        let output = root
            .path()
            .join("properties")
            .join(format!("{component}.parquet"));
        invalid_batch.stage_bytes(&output, b"third").unwrap();
        invalid_batch
            .stage_bytes(&root.path().join(TABLE_FILE), b"independent override")
            .unwrap();
        assert!(matches!(
            invalid_batch.commit_at(root.path()),
            Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
                ..
            })
        ));
        assert_eq!(std::fs::read(root.path().join(TABLE_FILE)).unwrap(), before);
        assert!(!output.exists());
    }
}

/// Semantic registrations belong to one existing rewrite, never a global cache.
#[derive(Default)]
pub(crate) struct PendingRoutes {
    root: Option<std::path::PathBuf>,
    routes: std::collections::BTreeSet<String>,
    bytes: u64,
}

impl PendingRoutes {
    pub(crate) fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    pub(crate) fn register(&mut self, root: &Path, route: &str) -> Result<String, GfError> {
        super::validate_semantic_route(route)?;
        let root =
            std::path::absolute(root).map_err(|_| invalid("pending route root is invalid"))?;
        if self.root.as_ref().is_some_and(|prior| prior != &root) {
            return Err(invalid(
                "one rewrite cannot register routes for different roots",
            ));
        }
        if !self.routes.contains(route) {
            let bytes = self
                .bytes
                .checked_add(route.len() as u64)
                .ok_or_else(|| limit("pending route bytes overflow"))?;
            if bytes > MAX_TABLE_BYTES || self.routes.len() as u64 >= MAX_ROUTES {
                return Err(limit("pending semantic route budget exceeded"));
            }
            self.routes.insert(route.to_owned());
            self.bytes = bytes;
            self.root = Some(root);
        }
        Ok(super::component(route))
    }

    pub(crate) fn prepare(
        self,
        root_path: &Path,
        root: &StableDirectory,
        batch: &mut crate::RewriteBatch,
    ) -> Result<Option<TablePrior>, GfError> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        if self.routes.is_empty() {
            return Ok(None);
        }
        if self.root.as_deref()
            != Some(
                std::path::absolute(root_path)
                    .map_err(|_| invalid("route commit root is invalid"))?
                    .as_path(),
            )
        {
            return Err(invalid(
                "registered route root differs from rewrite authority",
            ));
        }
        if batch
            .staged_paths()
            .any(|path| path == root_path.join(TABLE_FILE))
        {
            return Err(invalid(
                "rewrite cannot replace route authority independently of registered routes",
            ));
        }
        let mut original = root
            .open_child_file(std::ffi::OsStr::new(TABLE_FILE))
            .map_err(|_| invalid("route write requires an admitted owned layout"))?;
        if graphforge_filesystem::file_link_count(&original).ok() != Some(1) {
            return Err(invalid("owned route table must have one link"));
        }
        let identity = graphforge_filesystem::file_identity(&original)
            .map_err(|_| invalid("route table identity unavailable"))?;
        let mut bytes = Vec::new();
        (&mut original)
            .take(MAX_TABLE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("route table read failed"))?;
        let mut table = RouteTable::decode(&bytes, MAX_TABLE_BYTES, MAX_ROUTES)?;
        let prior = TablePrior {
            _file: original,
            identity,
            bytes: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        };
        let mut emitted = std::collections::BTreeSet::new();
        for path in batch.staged_paths() {
            let relative = path
                .strip_prefix(root_path)
                .map_err(|_| invalid("registered route destination escaped root"))?;
            let parts = relative
                .components()
                .map(|part| part.as_os_str().to_str())
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| invalid("registered route destination is not UTF-8"))?;
            if let Some(component) = super::route_position(&parts.join("/"))? {
                emitted.insert(component.to_owned());
            }
        }
        for route in self.routes {
            if emitted.contains(&super::component(&route)) {
                table.insert(&route, MAX_TABLE_BYTES, MAX_ROUTES)?;
            }
        }
        for component in emitted {
            table.route(&component)?;
        }
        let updated = table.encode(MAX_TABLE_BYTES)?;
        if updated != bytes {
            batch.stage_named_control_bytes(
                &root_path.join(TABLE_FILE),
                &updated,
                "semantic-routes.json.",
            )?;
        }
        prior.verify(root)?;
        Ok(Some(prior))
    }
}

pub(crate) struct TablePrior {
    _file: std::fs::File,
    identity: graphforge_filesystem::FileIdentity,
    bytes: u64,
    digest: [u8; 32],
}

impl TablePrior {
    pub(crate) fn verify(&self, root: &StableDirectory) -> Result<(), GfError> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let file = root
            .open_child_file(std::ffi::OsStr::new(TABLE_FILE))
            .map_err(|_| invalid("route table disappeared before commit"))?;
        if graphforge_filesystem::file_link_count(&file).ok() != Some(1) {
            return Err(invalid(
                "owned route table link count changed before commit",
            ));
        }
        if graphforge_filesystem::file_identity(&file).ok() != Some(self.identity) {
            return Err(invalid("route table identity changed before commit"));
        }
        let mut bytes = Vec::new();
        file.take(self.bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("route table reauthentication failed"))?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() as u64 != self.bytes || digest != self.digest {
            return Err(invalid("route table changed before commit"));
        }
        Ok(())
    }
}
