//! Selected export carries accepted history without promoting it to local authority.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer};

#[test]
fn selected_accepted_lineage_roundtrips_without_private_ancestor_or_live_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let (graph, proposal) = fixture(&directory.path().join("source"));
    let node_uuid = proposal.fields[0].object_uuid;
    let branch_uuid = proposal.source_branch_uuid;
    let version_uuid = proposal.source_version_uuid;
    let before = graph.research_version_retention().unwrap();
    assert_eq!(before.proposals.accepted.len(), 2);
    let mapping = before
        .proposals
        .accepted
        .values()
        .find(|m| m.unit.field == "property:score")
        .unwrap()
        .clone();
    let omitted = before
        .proposals
        .accepted
        .values()
        .find(|m| m.unit.field == "property:other")
        .unwrap();
    assert_eq!(mapping.proof_version_uuid, omitted.proof_version_uuid);
    let original_proof = crate::research_versions::materialize_version(
        &graph,
        &before.versions[&mapping.proof_version_uuid],
    )
    .unwrap();
    assert!(
        crate::branches::fields::read(&original_proof, &CancellationToken::new())
            .unwrap()
            .contains_key(&("node".into(), node_uuid, "property:other".into()))
    );
    let projection_uuid = Uuid::now_v7();
    let package = directory.path().join("selected");
    export_selected(&graph, &proposal, node_uuid, projection_uuid, &package);
    assert_no_private_parquet_values(&package);
    let target = directory.path().join("imported");
    GraphForge::import_portable(
        &target,
        &PortableImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    graphforge_storage::execute_project_cleanup(
        &target,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        graphforge_storage::ProjectRetentionLimits::default(),
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    let registry = imported.research_version_retention().unwrap();
    assert!(registry.proposals.accepted.is_empty());
    assert!(registry.branches.is_empty());
    assert!(registry.heads.is_empty());
    let archive = &registry.interchange[&projection_uuid];
    assert_eq!(archive.accepted.len(), 1);
    assert_eq!(archive.accepted[&mapping.mapping_uuid], mapping);
    assert_eq!(
        serde_json::to_vec(&archive.accepted[&mapping.mapping_uuid]).unwrap(),
        serde_json::to_vec(&mapping).unwrap()
    );
    assert!(!archive.accepted.contains_key(&omitted.mapping_uuid));
    let proof_uuid = archive.proof_exports[&mapping.proof_version_uuid];
    assert_ne!(proof_uuid, mapping.proof_version_uuid);
    assert_eq!(registry.versions.len(), 2);
    for id in [
        version_uuid,
        before.branches[&branch_uuid].base_version_uuid,
        before.branches[&branch_uuid].origin_version_uuid,
        mapping.proof_version_uuid,
    ] {
        assert!(!registry.versions.contains_key(&id));
        assert!(imported.open_research_version(id).is_err());
    }
    verify_baselines(&graph, version_uuid, &imported, projection_uuid);
    verify_historical_self_comparison(&imported, projection_uuid);
    verify_selected_content(&imported, projection_uuid, node_uuid, true);
    verify_selected_content(&imported, proof_uuid, node_uuid, false);
    assert_eq!(
        registry.versions[&proof_uuid].content.source_version,
        Some(mapping.proof_version_uuid)
    );
    let reference = imported
        .research_reference(
            &ResearchReferenceTarget::Version {
                version_uuid: projection_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(reference.version.content.source_version, Some(version_uuid));
    assert_eq!(reference.genealogy[0], before.branches[&branch_uuid]);
    verify_reexport(
        &imported,
        projection_uuid,
        &directory.path().join("reexport"),
    );
    assert!(
        imported
            .research_reference(
                &ResearchReferenceTarget::Branch { branch_uuid },
                &CancellationToken::new()
            )
            .is_err()
    );
}

fn verify_selected_content(graph: &GraphForge, version: Uuid, node: Uuid, deferred: bool) {
    let record = graph.research_version(version).unwrap();
    let view = crate::research_versions::materialize_version(graph, &record).unwrap();
    let fields = crate::branches::fields::read(&view, &CancellationToken::new()).unwrap();
    let nodes: std::collections::BTreeSet<_> = fields
        .keys()
        .filter(|k| k.0 == "node")
        .map(|k| k.1)
        .collect();
    assert_eq!(nodes, [node].into());
    for field in ["property:other", "property:private_note", "property:secret"] {
        assert!(!fields.contains_key(&("node".into(), node, field.into())));
    }
    assert_eq!(
        fields.contains_key(&("node".into(), node, "property:deferred".into())),
        deferred
    );
    let result = view.execute("MATCH(n:Character) RETURN n.score").unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

fn fixture(root: &std::path::Path) -> (GraphForge, SubmitResearchProposalRequest) {
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character {score:0, other:0, deferred:0, private_note:'private-selected-note'})").unwrap();
    let private_nodes = (0..32)
        .map(|i| format!("(:Private {{secret:'ancestor-private-{i}'}})"))
        .collect::<Vec<_>>()
        .join(",");
    graph.execute(&format!("CREATE {private_nodes}")).unwrap();
    let node_uuid = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let version_uuid = edit(
        &mut graph,
        branch_uuid,
        "MATCH(n:Character) SET n.score=1, n.other=3, n.deferred=2",
    );
    let proposal = submit(
        &mut graph,
        branch_uuid,
        version_uuid,
        node_uuid,
        &["property:score", "property:other", "property:deferred"],
    );
    let review = decision(&graph, proposal.proposal_uuid, |field| {
        if field == "property:deferred" {
            Defer
        } else {
            Accept
        }
    });
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    (graph, proposal)
}

/// A research bundle read as a stream gets the verdict the file path gives: the
/// same native owner checks, the same report apart from `source`.
#[test]
fn research_bundle_streams_to_the_file_verdict() {
    let directory = tempfile::tempdir().unwrap();
    let (graph, proposal) = fixture(&directory.path().join("source"));
    let projection = Uuid::now_v7();
    let node = proposal.fields[0].object_uuid;
    let package = directory.path().join("selected.gfpb");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: proposal.source_version_uuid,
                output: package.clone(),
                bundled: true,
                projection: Some(ResearchExportProjection {
                    version_uuid: projection,
                    frozen_ipc: proposal.frozen_ipc.clone(),
                    fields: ["$object", "$labels", "property:score", "property:deferred"]
                        .into_iter()
                        .map(|field| ResearchFieldIdentity {
                            object_kind: "node".into(),
                            object_uuid: node,
                            field: field.into(),
                        })
                        .collect(),
                    created_at: 9,
                }),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let limits = graphforge_core::portable::PortableLimits::default();
    let file = crate::verify_portable(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert!(file.research_interchange);
    let scratch = tempfile::tempdir().unwrap();
    // The storage helper proves the native validator runs on the staged files.
    let mut native_runs = 0;
    let staged = stream_with_validator(scratch.path(), &package, limits, &mut native_runs);
    assert!(native_runs > 0, "native research validation must run");
    // The public facade, from a plain reader, gives the same verdict.
    let stream = crate::verify_portable_stream(
        &mut std::io::BufReader::new(std::fs::File::open(&package).unwrap()),
        Some(scratch.path()),
        limits,
        None,
    )
    .unwrap();
    let strip_source = |report: &graphforge_core::portable::PortableReport| {
        let mut value = serde_json::to_value(report).unwrap();
        value.as_object_mut().unwrap().remove("source").unwrap();
        value
    };
    assert_eq!(strip_source(&staged), strip_source(&stream));
    assert_eq!(file.source, graphforge_core::portable::PortableSource::File);
    assert_eq!(
        stream.source,
        graphforge_core::portable::PortableSource::Stream
    );
    let strip = |report: &graphforge_core::portable::PortableReport| {
        let mut value = serde_json::to_value(report).unwrap();
        value.as_object_mut().unwrap().remove("source").unwrap();
        value
    };
    assert_eq!(strip(&file), strip(&stream));
    // The private stage is gone once the verdict is returned.
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    native_failure_is_refused_alike_from_a_file_and_a_stream(directory.path(), &package, limits);
    // A changed research object is refused alike from a file and a stream.
    let mut bytes = std::fs::read(&package).unwrap();
    let name = b"research-retained/";
    let header = bytes
        .windows(name.len())
        .position(|window| window == name)
        .unwrap();
    let header = header - header % 512;
    bytes[header + 512] ^= 1;
    let tampered = directory.path().join("tampered.gfpb");
    std::fs::write(&tampered, &bytes).unwrap();
    let by_file = crate::verify_portable(
        &PortableVerifyRequest {
            input: tampered,
            mode: graphforge_core::portable::PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap_err();
    let by_stream = crate::verify_portable_stream(
        &mut std::io::Cursor::new(bytes),
        Some(directory.path()),
        limits,
        None,
    )
    .unwrap_err();
    assert_eq!(by_file.code, by_stream.code);
    assert_eq!(by_file.entry, by_stream.entry);
}

/// Every digest in the package is correct, but one accepted mapping commits to
/// a value the Version does not hold, which only native validation can see.
fn native_failure_is_refused_alike_from_a_file_and_a_stream(
    directory: &std::path::Path,
    package: &std::path::Path,
    limits: graphforge_core::portable::PortableLimits,
) {
    use graphforge_core::portable::PortableErrorCode;
    let forged = reauthenticated(&std::fs::read(package).unwrap(), |registry| {
        let archive = registry.interchange.values_mut().next().unwrap();
        let mut mapping = archive.accepted.values().next().unwrap().clone();
        archive.accepted.remove(&mapping.mapping_uuid);
        let mut value = mapping.value_sha256.expect("the accepted value");
        value[0] ^= 1;
        mapping.value_sha256 = Some(value);
        mapping.mapping_uuid = mapping.identity().unwrap();
        archive.accepted.insert(mapping.mapping_uuid, mapping);
    });
    let path = directory.join("forged.gfpb");
    std::fs::write(&path, &forged).unwrap();
    // Storage-level verification authenticates every byte and passes.
    graphforge_storage::verify_portable(
        &path,
        graphforge_core::portable::PortableMode::Full,
        limits,
        None,
    )
    .expect("the forged package is authentic");
    let by_file = crate::verify_portable(
        &PortableVerifyRequest {
            input: path,
            mode: graphforge_core::portable::PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap_err();
    let by_stream = crate::verify_portable_stream(
        &mut std::io::Cursor::new(forged),
        Some(directory),
        limits,
        None,
    )
    .unwrap_err();
    assert_eq!(by_file.code, PortableErrorCode::Incompatible);
    assert_eq!(by_stream.code, by_file.code);
    assert_eq!(by_stream.entry, by_file.entry);
}

/// Rewrite the research registry of a canonical bundle with `edit`, then
/// recompute every digest that covers it: the manifest file record, the
/// package digest, the payload inventory and the tag manifest.
fn reauthenticated(
    bundle: &[u8],
    edit: impl FnOnce(&mut graphforge_storage::research_versions::ResearchRegistry),
) -> Vec<u8> {
    let mut edit = Some(edit);
    use sha2::{Digest, Sha256};
    const MANIFEST: &str = "data/graphforge-project.json";
    let hex = |bytes: &[u8]| {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    // Split the bundle into `(raw header run, path, payload)` units.
    let mut units: Vec<(Vec<u8>, String, Vec<u8>)> = Vec::new();
    let mut at = 0;
    let mut pax_prefix = Vec::new();
    while bundle[at..at + 512].iter().any(|byte| *byte != 0) {
        let header = &bundle[at..at + 512];
        let size = usize::from_str_radix(
            std::str::from_utf8(&header[124..136])
                .unwrap()
                .trim_matches(['\0', ' ']),
            8,
        )
        .unwrap();
        let end = at + 512 + size.div_ceil(512) * 512;
        if header[156] == b'x' {
            pax_prefix.extend_from_slice(&bundle[at..end]);
        } else {
            let name = std::str::from_utf8(&header[..100])
                .unwrap()
                .trim_end_matches('\0');
            let prefix = std::str::from_utf8(&header[345..500])
                .unwrap()
                .trim_end_matches('\0');
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            units.push((
                std::mem::take(&mut pax_prefix),
                path,
                bundle[at + 512..at + 512 + size].to_vec(),
            ));
            units.last_mut().unwrap().0.extend_from_slice(header);
        }
        at = end;
    }
    let registry_path = units
        .iter()
        .map(|unit| unit.1.clone())
        .find(|path| path.contains("research-registry-"))
        .expect("a research registry entry");
    let mut digests = std::collections::BTreeMap::new();
    for unit in &mut units {
        if unit.1 == registry_path {
            let mut registry: graphforge_storage::research_versions::ResearchRegistry =
                serde_json::from_slice(&unit.2).unwrap();
            let before = serde_json::to_vec(&registry).unwrap();
            assert_eq!(before, unit.2, "the registry is canonical JSON");
            edit.take().expect("one registry")(&mut registry);
            unit.2 = serde_json::to_vec(&registry).unwrap();
        }
        digests.insert(unit.1.clone(), (hex(&unit.2), unit.2.len()));
    }
    for unit in &mut units {
        if unit.1 == MANIFEST {
            let mut manifest: serde_json::Value = serde_json::from_slice(&unit.2).unwrap();
            for component in manifest["components"].as_array_mut().unwrap() {
                for file in component["files"].as_array_mut().unwrap() {
                    if file["path"] == registry_path.as_str() {
                        file["sha256"] = digests[&registry_path].0.clone().into();
                        file["length"] = digests[&registry_path].1.into();
                    }
                }
            }
            manifest.as_object_mut().unwrap().remove("package_digest");
            let canonical = serde_json::to_vec(&manifest).unwrap();
            let digest = hex(&[b"graphforge-project/2\0".as_slice(), &canonical].concat());
            manifest["package_digest"] = format!("sha256:{digest}").into();
            unit.2 = serde_json::to_vec(&manifest).unwrap();
            digests.insert(MANIFEST.into(), (hex(&unit.2), unit.2.len()));
        }
    }
    let rewrite_rows =
        |bytes: &[u8], digests: &std::collections::BTreeMap<String, (String, usize)>| {
            std::str::from_utf8(bytes)
                .unwrap()
                .lines()
                .map(|line| {
                    let (_, path) = line.split_once("  ").unwrap();
                    let digest = digests.get(path).map_or_else(
                        || line.split_once("  ").unwrap().0.to_owned(),
                        |(digest, _)| digest.clone(),
                    );
                    format!("{digest}  {path}\n")
                })
                .collect::<String>()
                .into_bytes()
        };
    for unit in &mut units {
        if unit.1 == "manifest-sha256.txt" {
            unit.2 = rewrite_rows(&unit.2, &digests);
            digests.insert(unit.1.clone(), (hex(&unit.2), unit.2.len()));
        }
    }
    for unit in &mut units {
        if unit.1 == "tagmanifest-sha256.txt" {
            unit.2 = rewrite_rows(&unit.2, &digests);
        }
    }
    let mut out = Vec::new();
    for (mut run, path, payload) in units {
        // Fix the regular header's size and checksum; PAX runs are unchanged.
        let header_at = run.len() - 512;
        let header = &mut run[header_at..];
        let size = format!("{:011o}\0", payload.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[148..156].fill(b' ');
        let sum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        let _ = path;
        out.extend(run);
        out.extend(&payload);
        out.resize(out.len().div_ceil(512) * 512, 0);
    }
    out.extend([0u8; 1024]);
    out
}

fn stream_with_validator(
    scratch: &std::path::Path,
    package: &std::path::Path,
    limits: graphforge_core::portable::PortableLimits,
    runs: &mut usize,
) -> graphforge_core::portable::PortableReport {
    let mut reader = std::io::BufReader::new(std::fs::File::open(package).unwrap());
    let mut count =
        |generation: &graphforge_storage::ResolvedProjectGeneration,
         version: &graphforge_storage::research_versions::ResearchVersionRecord,
         registry: &graphforge_storage::research_versions::ResearchRegistry| {
            *runs += 1;
            crate::research_interchange::validation::validate(generation, version, registry)
        };
    graphforge_storage::validate_research_package_stream(
        &mut reader,
        Some(scratch),
        limits,
        None,
        &mut count,
    )
    .unwrap()
}

fn export_selected(
    graph: &GraphForge,
    proposal: &SubmitResearchProposalRequest,
    node_uuid: Uuid,
    projection_uuid: Uuid,
    package: &std::path::Path,
) {
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: proposal.source_version_uuid,
                output: package.to_path_buf(),
                bundled: false,
                projection: Some(ResearchExportProjection {
                    version_uuid: projection_uuid,
                    frozen_ipc: proposal.frozen_ipc.clone(),
                    fields: ["$object", "$labels", "property:score", "property:deferred"]
                        .into_iter()
                        .map(|field| ResearchFieldIdentity {
                            object_kind: "node".into(),
                            object_uuid: node_uuid,
                            field: field.into(),
                        })
                        .collect(),
                    created_at: 9,
                }),
            },
            &CancellationToken::new(),
        )
        .unwrap();
}

fn verify_baselines(source: &GraphForge, original: Uuid, imported: &GraphForge, selected: Uuid) {
    let source_view = crate::research_versions::materialize_version(
        source,
        &source.research_version(original).unwrap(),
    )
    .unwrap();
    let imported_view = crate::research_versions::materialize_version(
        imported,
        &imported.research_version(selected).unwrap(),
    )
    .unwrap();
    let original = crate::branches::baseline::read(&source_view).unwrap();
    let selected = crate::branches::baseline::read(&imported_view).unwrap();
    assert_eq!(selected.len(), 4);
    for (key, actual) in selected {
        let expected = &original[&key];
        assert_eq!(
            (
                actual.origin,
                actual.incorporated,
                actual.contribution,
                &actual.role
            ),
            (
                expected.origin,
                expected.incorporated,
                expected.contribution,
                &expected.role
            )
        );
        assert_eq!(
            (&actual.original, &actual.baseline, &actual.current),
            (&expected.original, &expected.baseline, &expected.current)
        );
    }
}

fn assert_no_private_parquet_values(root: &std::path::Path) {
    let mut pending = vec![root.to_path_buf()];
    let mut parquet_files = 0;
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        if !bytes.starts_with(b"PAR1") {
            continue;
        }
        parquet_files += 1;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap();
        assert_private_absent(&format!("{:?}", builder.metadata()), &path);
        assert_private_absent(&format!("{:?}", builder.schema()), &path);
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            for column in batch.columns() {
                for row in 0..column.len() {
                    let value =
                        arrow::util::display::array_value_to_string(column.as_ref(), row).unwrap();
                    assert_private_absent(&value, &path);
                }
            }
        }
    }
    assert!(
        parquet_files > 0,
        "fixture must inspect actual exported Parquet bytes"
    );
}

fn assert_private_absent(value: &str, path: &std::path::Path) {
    for secret in ["private-selected-note", "ancestor-private-"] {
        assert!(
            !value.contains(secret),
            "exported physical Parquet retains private value {secret}: {}",
            path.display()
        );
    }
}

fn verify_reexport(imported: &GraphForge, selected: Uuid, root: &std::path::Path) {
    std::fs::create_dir(root).unwrap();
    let package = root.join("package");
    imported
        .export_research(
            &ExportResearchRequest {
                version_uuid: selected,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_no_private_parquet_values(&package);
    crate::verify_portable(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: graphforge_core::portable::PortableMode::Full,
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let target = root.join("imported");
    GraphForge::import_portable(
        &target,
        &PortableImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let again = GraphForge::new(target.to_str()).unwrap();
    let original = imported.research_version_retention().unwrap();
    let actual = again.research_version_retention().unwrap();
    assert_eq!(
        actual.interchange[&selected].accepted,
        original.interchange[&selected].accepted
    );
    assert_eq!(
        actual.interchange[&selected].proof_exports,
        original.interchange[&selected].proof_exports
    );
    for id in original.interchange[&selected].proof_exports.values() {
        assert_eq!(actual.versions[id], original.versions[id]);
    }
    assert!(actual.proposals.accepted.is_empty());
}

fn verify_historical_self_comparison(graph: &GraphForge, version: Uuid) {
    let endpoint = crate::ResearchComparisonEndpoint::Version {
        version_uuid: version,
    };
    let result = graph
        .compare_research(
            &crate::ResearchComparisonRequest {
                left: endpoint.clone(),
                right: endpoint,
                left_authority: None,
                right_authority: None,
                detail: crate::ResearchComparisonDetail::Changes,
                accepted: vec![],
                max_fields: 40_000,
                max_bytes: 64 * 1024 * 1024,
                page_size: 1000,
                after: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    // Imported genealogy and preserved incorporated rows do not need live Branch
    // heads or ancestor payloads to compare an immutable selected Version.
    let changes = result.batches[0]
        .column_by_name("change")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(changes.iter().flatten().all(|value| value == "equivalent"));
}
