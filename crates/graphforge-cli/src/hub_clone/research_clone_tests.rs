//! End-to-end research lineage: a real Fork with two Branches and a Proposal is
//! published through the registry-derived lineage builder, served as Hub
//! documents and objects, listed by a consumer, and cloned through the real
//! `gf clone --ref` / `--version-uuid` path into destinations that reopen.

use super::*;
use arrow::array::{Array as _, FixedSizeBinaryArray, Int64Array};
use graphforge_api::{
    BranchSource, BuildResearchLineageRequest, CancellationToken, CreateResearchBranchRequest,
    ExecuteResearchBranchRequest, ExportResearchRequest, ForkResearchRequest,
    PortableExportRequest, PortableOutput, PortableSelection, PortableSelectionProfile,
    ResearchFieldIdentity, ResearchReference, ResearchReferenceTarget, SliceMembers, SliceRequest,
    SliceSelector, SliceSource, SubmitResearchProposalRequest, WorkspaceResearchMetadata,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, PORTABLE_FORMAT, PORTABLE_MEDIA_TYPE, PortablePackageReference,
    ProtocolRequirement, ProtocolVersion, RESEARCH_LINEAGE_FORMAT, RESEARCH_LINEAGE_MEDIA_TYPE,
    RepositoryRef, ResearchLineageReference, Sha256Digest,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use uuid::Uuid;

const REPOSITORY: &str = "curate/claims-fork";
const ORIGIN_REPOSITORY: &str = "curate/claims";

/// In-memory Hub: discovery documents and objects addressed by exact URL.
struct Hub {
    documents: BTreeMap<String, Vec<u8>>,
    requested: Mutex<Vec<String>>,
}

impl Hub {
    fn requested(&self) -> Vec<String> {
        self.requested.lock().unwrap().clone()
    }
}

impl Transport for Hub {
    fn get(
        &self,
        url: &Url,
        _range: Option<u64>,
        _if_range: Option<&str>,
        _limit: u64,
    ) -> Result<HttpResponse, graphforge_api::GfError> {
        self.requested.lock().unwrap().push(url.as_str().to_owned());
        let (status, body) = self
            .documents
            .get(url.as_str())
            .map_or((404, Vec::new()), |bytes| (200, bytes.clone()));
        Ok(HttpResponse {
            status,
            location: None,
            content_range: None,
            etag: Some("\"hub\"".into()),
            retry_after: None,
            body: Box::new(std::io::Cursor::new(body)),
        })
    }
}

fn sha256(bytes: &[u8]) -> Sha256Digest {
    Sha256Digest(hash_reader(&mut std::io::Cursor::new(bytes)).unwrap())
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes
        .iter()
        .fold(String::from("sha256:"), |mut output, byte| {
            use std::fmt::Write as _;
            write!(output, "{byte:02x}").unwrap();
            output
        })
}

fn endpoint_url(name: &str) -> String {
    let (_, base) = parse_input(REPOSITORY).unwrap();
    endpoint(&base, name).to_string()
}

fn object_url(digest: &Sha256Digest) -> String {
    format!(
        "https://objects.example/sha256/{}",
        digest.0.trim_start_matches("sha256:")
    )
}

fn generation(graph: &GraphForge) -> Uuid {
    graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid
}

fn create_branch(
    graph: &mut GraphForge,
    branch_uuid: Uuid,
    version_uuid: Uuid,
    source: BranchSource,
    label: &str,
) {
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                description: None,
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                source,
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: label.into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
}

fn edit(graph: &mut GraphForge, branch_uuid: Uuid, score: i64) -> Uuid {
    let version_uuid = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                label: None,
                description: None,
                author: None,
                committer: None,
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                created_at: 2,
                query: format!("MATCH (n:Item) SET n.score={score}"),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

fn item_node(graph: &GraphForge) -> Uuid {
    let result = graph
        .execute("MATCH (n:Item) RETURN n.node_uuid AS id")
        .unwrap();
    let ids = result.batches[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.len(), 1);
    Uuid::from_slice(ids.value(0)).unwrap()
}

fn score(graph: &GraphForge) -> i64 {
    let result = graph.execute("MATCH (n:Item) RETURN n.score AS s").unwrap();
    result.batches[0]
        .column_by_name("s")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn reference(graph: &GraphForge, version_uuid: Uuid) -> ResearchReference {
    graph
        .research_reference(
            &ResearchReferenceTarget::Version { version_uuid },
            &CancellationToken::new(),
        )
        .unwrap()
}

/// Research history of a published Fork and the Project it was forked from.
struct Published {
    _root: tempfile::TempDir,
    origin: GraphForge,
    origin_version: Uuid,
    fork: GraphForge,
    fork_project: Uuid,
    main_branch: Uuid,
    main_base: Uuid,
    main_head: Uuid,
    feature_branch: Uuid,
    feature_head: Uuid,
    proposal: Uuid,
    payload: Uuid,
    packages: BTreeMap<Uuid, (PortablePackageReference, Vec<u8>)>,
    project_package: (PortablePackageReference, Vec<u8>),
}

fn publish_history() -> Published {
    publish_history_with_padding(0)
}

/// [`publish_history`] over a graph that also holds `padding` nodes of about
/// 200 bytes each, which every published Version carries.
#[allow(clippy::too_many_lines)]
fn publish_history_with_padding(padding: u32) -> Published {
    publish_history_shaped(padding, 0, 0, false)
}

/// Incompressible text of `bytes` bytes, distinct per `seed`.
fn incompressible_text(bytes: usize, seed: u64) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + u8::try_from(state % 26).unwrap())
        })
        .collect()
}

/// Republish a durable project's current generation as an expanded (V1)
/// graph/files record, the form every Version of a project written before
/// compact roots carries and a research package still admits.
fn expand_project(path: &Path) {
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome, capture_graph_files,
        materialize_graph_objects, resolve_project_generation,
        stage_project_generation_with_graph_tree_mode,
    };
    let current = resolve_project_generation(path).unwrap();
    let inventory = current.graph_files_inventory().unwrap().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    materialize_graph_objects(current.container_root(), &inventory, workspace.path()).unwrap();
    let (_, graph_participant) = capture_graph_files(workspace.path()).unwrap();
    let capabilities = current
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect();
    let mut participants = current
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|snapshot| {
            snapshot.capability_id != graphforge_storage::GRAPH_CAPABILITY_ID
                || snapshot.record_family_id != graphforge_storage::GRAPH_FILES_FAMILY
        })
        .map(|snapshot| ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "parquet" => ProjectParticipantEncoding::Parquet,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                other => panic!("unsupported fixture participant encoding {other}"),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    participants.push(graph_participant);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::new_v4(),
        generation_uuid: Uuid::new_v4(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) = stage_project_generation_with_graph_tree_mode(
        path,
        &request,
        Some(workspace.path()),
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Durable,
    )
    .unwrap() else {
        panic!("expanded graph publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}

/// [`publish_history`] over a graph that also holds `padding` small nodes and
/// `blobs` nodes carrying `blob_bytes` of incompressible text each, which every
/// published Version carries. With `expanded`, the origin's graph/files record
/// is the expanded form, so its Version (and every Version forked from it)
/// retains the graph as an expanded tree.
fn publish_history_shaped(
    padding: u32,
    blobs: usize,
    blob_bytes: usize,
    expanded: bool,
) -> Published {
    let cancellation = CancellationToken::new();
    let root = tempfile::tempdir().unwrap();
    // The origin Project: one retained Version to fork from.
    let mut origin = GraphForge::new(root.path().join("origin").to_str()).unwrap();
    origin.execute("CREATE (:Item {score:0})").unwrap();
    if padding > 0 {
        origin
            .execute(&format!(
                "UNWIND range(1, {padding}) AS i CREATE (:Pad {{id: i, body: '{}'}})",
                "x".repeat(200)
            ))
            .unwrap();
    }
    for blob in 0..blobs {
        origin
            .execute(&format!(
                "CREATE (:Blob {{id: {blob}, body: '{}'}})",
                incompressible_text(blob_bytes, blob as u64 + 1)
            ))
            .unwrap();
    }
    if expanded {
        drop(origin);
        expand_project(&root.path().join("origin"));
        origin = GraphForge::new(root.path().join("origin").to_str()).unwrap();
    }
    let origin_version = Uuid::now_v7();
    create_branch(
        &mut origin,
        Uuid::now_v7(),
        origin_version,
        BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        "origin",
    );
    // The Fork: an independent Project that cites the origin Version.
    let fork_project = Uuid::now_v7();
    let fork_dir = root.path().join("fork");
    origin
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: fork_project,
                version_uuid: origin_version,
                projection: None,
                target: fork_dir.clone(),
                actor_uuid: Uuid::now_v7(),
                governance: "Independent local review".into(),
                adopt_selected_ontology: true,
                metadata: WorkspaceResearchMetadata::empty(),
            },
            &cancellation,
        )
        .unwrap();
    let mut fork = GraphForge::new(fork_dir.to_str()).unwrap();
    // Branch one: an immutable base Version and a later head.
    let main_branch = Uuid::now_v7();
    let main_base = Uuid::now_v7();
    create_branch(
        &mut fork,
        main_branch,
        main_base,
        BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        "main",
    );
    let main_head = edit(&mut fork, main_branch, 1);
    // Branch two, forked from Branch one's head, with its own head.
    let feature_branch = Uuid::now_v7();
    let feature_base = Uuid::now_v7();
    create_branch(
        &mut fork,
        feature_branch,
        feature_base,
        BranchSource::Branch {
            branch_uuid: main_branch,
        },
        "feature/score",
    );
    let feature_head = edit(&mut fork, feature_branch, 2);
    // A Proposal: a frozen, field-selected projection of the feature head.
    let node = item_node(&fork);
    let frozen = fork
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: feature_head,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        nodes: [node].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &cancellation,
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, &frozen.schema).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let proposal = Uuid::now_v7();
    fork.submit_research_proposal(
        &SubmitResearchProposalRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: generation(&fork),
            proposal_uuid: proposal,
            source_branch_uuid: feature_branch,
            source_version_uuid: feature_head,
            frozen_ipc,
            fields: vec![ResearchFieldIdentity {
                object_kind: "node".into(),
                object_uuid: node,
                field: "property:score".into(),
            }],
            actor_uuid: Uuid::now_v7(),
            created_at: 3,
            motivation: "Review the score".into(),
            policy: String::new(),
        },
        &cancellation,
    )
    .unwrap();
    let registry = fork.research_version_retention().unwrap();
    let payload = registry.proposals.proposals[&proposal].payload_version_uuid;
    // One portable package per published Version, through the research
    // interchange exporter, plus the complete Project package.
    let exports = root.path().join("exports");
    std::fs::create_dir(&exports).unwrap();
    let mut packages = BTreeMap::new();
    for version_uuid in [main_base, main_head, feature_base, feature_head, payload] {
        let output = exports.join(format!("{version_uuid}.gfpb"));
        let exported = fork
            .export_research(
                &ExportResearchRequest {
                    version_uuid,
                    output: output.clone(),
                    bundled: true,
                    projection: None,
                },
                &cancellation,
            )
            .unwrap();
        let bytes = std::fs::read(&output).unwrap();
        packages.insert(
            version_uuid,
            (
                PortablePackageReference {
                    format: PORTABLE_FORMAT.into(),
                    package_digest: Sha256Digest(exported.package_digest),
                    producing_core_version: graphforge_api::GRAPHFORGE_VERSION.into(),
                    object_digest: sha256(&bytes),
                },
                bytes,
            ),
        );
    }
    let output = exports.join("project.gfpb");
    // A Project with live research history exports research only through the
    // research interchange (operational heads never enter a portable package),
    // so the Project package here carries the graph data components. Research
    // clones never fetch it.
    let exported = fork
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: output.clone(),
                representation: PortableOutput::Bundle,
                profile: PortableSelectionProfile::DataComponents,
                subset: None,
                limits: PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let bytes = std::fs::read(&output).unwrap();
    let project_package = (
        PortablePackageReference {
            format: PORTABLE_FORMAT.into(),
            package_digest: Sha256Digest(exported.package_digest),
            producing_core_version: graphforge_api::GRAPHFORGE_VERSION.into(),
            object_digest: sha256(&bytes),
        },
        bytes,
    );
    Published {
        _root: root,
        origin,
        origin_version,
        fork,
        fork_project,
        main_branch,
        main_base,
        main_head,
        feature_branch,
        feature_head,
        proposal,
        payload,
        packages,
        project_package,
    }
}

fn lineage_request(published: &Published) -> BuildResearchLineageRequest {
    BuildResearchLineageRequest {
        repository: RepositoryIdentity::parse(REPOSITORY).unwrap(),
        immutable_version: published.project_package.0.package_digest.clone(),
        project_uuid: published.fork_project,
        branch_ref_names: BTreeMap::from([
            (published.main_branch, "main".to_owned()),
            (published.feature_branch, "feature/score".to_owned()),
        ]),
        version_packages: published
            .packages
            .iter()
            .map(|(version, (package, _))| (*version, package.clone()))
            .collect(),
        fork_origin_repository: Some(RepositoryIdentity::parse(ORIGIN_REPOSITORY).unwrap()),
        published_proposals: BTreeSet::from([published.proposal]),
    }
}

/// Serve canonical manifest, refs, lineage, and every package object.
fn serve(published: &Published, lineage: &ResearchLineage) -> Hub {
    let repository = RepositoryIdentity::parse(REPOSITORY).unwrap();
    let ref_names: BTreeSet<&str> = lineage
        .branches
        .iter()
        .map(|branch| branch.ref_name.as_str())
        .collect();
    let default = if ref_names.contains("main") {
        "main"
    } else {
        ref_names.first().copied().unwrap()
    };
    let lineage_bytes = lineage.to_canonical_json().unwrap();
    let lineage_object = sha256(&lineage_bytes);
    let mut documents = BTreeMap::new();
    let mut objects = Vec::new();
    let mut publish = |digest: &Sha256Digest, media_type: &str, bytes: &[u8]| {
        if documents
            .insert(object_url(digest), bytes.to_vec())
            .is_none()
        {
            objects.push(ObjectDescriptor {
                digest: digest.clone(),
                length: bytes.len() as u64,
                media_type: media_type.into(),
                locations: vec![object_url(digest)],
            });
        }
    };
    publish(
        &published.project_package.0.object_digest,
        PORTABLE_MEDIA_TYPE,
        &published.project_package.1,
    );
    for (package, bytes) in published.packages.values() {
        publish(&package.object_digest, PORTABLE_MEDIA_TYPE, bytes);
    }
    publish(&lineage_object, RESEARCH_LINEAGE_MEDIA_TYPE, &lineage_bytes);
    objects.sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: default.into(),
        resolved_ref: default.into(),
        immutable_version: lineage.immutable_version.clone(),
        package: published.project_package.0.clone(),
        summary: None,
        ontology: None,
        lineage: Some(ResearchLineageReference {
            format: RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest: lineage.canonical_digest().unwrap(),
            object_digest: lineage_object,
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable".into(),
            major: 1,
        }],
        capabilities: vec![],
        objects,
        extensions: BTreeMap::new(),
    };
    let manifest_bytes = manifest.to_canonical_json().unwrap();
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository,
        default_ref: default.into(),
        refs: ref_names
            .into_iter()
            .map(|name| RepositoryRef {
                name: name.into(),
                target: manifest.immutable_version.clone(),
                validator: sha256(&manifest_bytes),
            })
            .collect(),
        extensions: BTreeMap::new(),
    };
    documents.insert(endpoint_url("manifest"), manifest_bytes);
    documents.insert(endpoint_url("refs"), refs.to_canonical_json().unwrap());
    Hub {
        documents,
        requested: Mutex::new(Vec::new()),
    }
}

fn clone_into(
    hub: &Hub,
    destination: &Path,
    git_ref: Option<&str>,
    version_uuid: Option<Uuid>,
) -> Result<serde_json::Value, graphforge_api::GfError> {
    let mut output = Vec::new();
    run_clone_with(
        hub,
        CloneArgs {
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            repository: REPOSITORY.into(),
            destination: Some(destination.to_path_buf()),
            telemetry_endpoint: None,
            git_ref: git_ref.map(str::to_owned),
            version_uuid: version_uuid.map(|id| id.to_string()),
        },
        true,
        &mut output,
    )?;
    Ok(serde_json::from_slice(&output).unwrap())
}

/// A consumer reads the served documents exactly as a Hub page would.
fn consume(hub: &Hub) -> (DiscoveryManifest, ResearchLineage) {
    let limits = DiscoveryLimits::default();
    let manifest =
        DiscoveryManifest::from_json(&hub.documents[&endpoint_url("manifest")], limits).unwrap();
    let refs = RefSet::from_json(&hub.documents[&endpoint_url("refs")], limits).unwrap();
    refs.validate_manifest(&manifest).unwrap();
    let object = manifest.lineage_object().unwrap();
    let bytes = &hub.documents[&object.locations[0]];
    assert_eq!(sha256(bytes), object.digest);
    let lineage = ResearchLineage::from_json(bytes, limits).unwrap();
    manifest.bind_lineage(&refs, &lineage).unwrap();
    (manifest, lineage)
}

#[test]
#[allow(clippy::too_many_lines)]
fn fork_branches_and_versions_clone_into_distinct_destinations_with_identical_lineage() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let hub = serve(&published, &lineage);
    let (manifest, listed) = consume(&hub);
    assert_eq!(listed, lineage);

    // List Branches and Versions.
    let branches: BTreeMap<_, _> = listed
        .branches
        .iter()
        .map(|branch| (branch.ref_name.as_str(), branch))
        .collect();
    assert_eq!(
        branches.keys().copied().collect::<Vec<_>>(),
        ["feature/score", "main"]
    );
    let main = branches["main"];
    assert_eq!(main.branch_uuid, published.main_branch.to_string());
    assert_eq!(main.head_version_uuid, published.main_head.to_string());
    assert_eq!(main.base_version_uuid, published.main_base.to_string());
    assert_eq!(main.parent_branch_uuid, None);
    let feature = branches["feature/score"];
    assert_eq!(
        feature.head_version_uuid,
        published.feature_head.to_string()
    );
    assert_eq!(
        feature.parent_branch_uuid.as_deref(),
        Some(published.main_branch.to_string().as_str())
    );
    assert_eq!(listed.versions.len(), published.packages.len());
    for (version_uuid, (package, _)) in &published.packages {
        let entry = listed.version(&version_uuid.to_string()).expect("listed");
        assert_eq!(entry.package.as_ref(), Some(package));
        assert_eq!(
            entry.identity_digest.0,
            hex(&reference(&published.fork, *version_uuid).identity_sha256)
        );
    }

    // Resolve "forked from" to the origin Project identity.
    let fork = listed.fork.as_ref().expect("Fork origin is advertised");
    let origin_reference = reference(&published.origin, published.origin_version);
    assert_eq!(
        fork.origin_repository,
        RepositoryIdentity::parse(ORIGIN_REPOSITORY).unwrap()
    );
    assert_eq!(
        fork.origin_project_uuid,
        origin_reference.project_uuid.to_string()
    );
    assert_ne!(fork.origin_project_uuid, listed.project_uuid);
    assert_eq!(listed.project_uuid, published.fork_project.to_string());
    assert_eq!(
        fork.origin_version_uuid,
        published.origin_version.to_string()
    );
    assert_eq!(
        fork.origin_version_identity.0,
        hex(&origin_reference.identity_sha256)
    );

    // Clone the Branch head and the immutable base into distinct destinations.
    let root = tempfile::tempdir().unwrap();
    let head_dir = root.path().join("head");
    let base_dir = root.path().join("base");
    let head_clone = clone_into(&hub, &head_dir, Some("main"), None).unwrap();
    let base_clone = clone_into(&hub, &base_dir, None, Some(published.main_base)).unwrap();
    let package_urls: BTreeSet<_> = published
        .packages
        .values()
        .map(|(package, _)| object_url(&package.object_digest))
        .collect();
    let fetched: Vec<_> = hub
        .requested()
        .into_iter()
        .filter(|url| {
            package_urls.contains(url) || *url == object_url(&manifest.package.object_digest)
        })
        .collect();
    assert_eq!(
        fetched,
        [
            object_url(&published.packages[&published.main_head].0.object_digest),
            object_url(&published.packages[&published.main_base].0.object_digest),
        ],
        "each clone fetches exactly its Version package, never the Project package"
    );
    for (clone, version) in [
        (&head_clone, published.main_head),
        (&base_clone, published.main_base),
    ] {
        assert_eq!(clone["research_version_uuid"], version.to_string());
        assert_eq!(clone["research_version_kind"], "complete");
        assert_eq!(
            clone["package_digest"],
            published.packages[&version].0.package_digest.0
        );
    }
    assert_ne!(
        head_clone["generation_uuid"], base_clone["generation_uuid"],
        "different Versions of one snapshot import as distinct generations"
    );

    // Both destinations reopen with the published lineage identities.
    let head = GraphForge::new(head_dir.to_str()).unwrap();
    let base = GraphForge::new(base_dir.to_str()).unwrap();
    assert_eq!(
        generation(&head).to_string(),
        head_clone["generation_uuid"].as_str().unwrap()
    );
    assert_eq!(
        generation(&base).to_string(),
        base_clone["generation_uuid"].as_str().unwrap()
    );
    for (graph, version) in [(&head, published.main_head), (&base, published.main_base)] {
        let cloned = reference(graph, version);
        let source = reference(&published.fork, version);
        assert_eq!(
            hex(&cloned.identity_sha256),
            listed
                .version(&version.to_string())
                .unwrap()
                .identity_digest
                .0
        );
        assert_eq!(cloned.identity_sha256, source.identity_sha256);
        assert_eq!(cloned.version, source.version);
        assert_eq!(cloned.genealogy, source.genealogy);
        assert_eq!(cloned.origin_project_uuid, source.origin_project_uuid);
    }
    // ADR 0044: the Version clone stays on the base after the head moved.
    assert_eq!(score(&head), 1);
    assert_eq!(score(&base), 0);
    assert!(
        base.research_reference(
            &ResearchReferenceTarget::Version {
                version_uuid: published.main_head,
            },
            &CancellationToken::new(),
        )
        .is_err(),
        "the base clone never follows the later head"
    );
}

#[test]
fn proposal_projection_clones_as_a_projection_of_its_source_never_as_the_source() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let source_digest = lineage
        .version(&published.feature_head.to_string())
        .unwrap()
        .identity_digest
        .clone();
    let projection = lineage
        .version(&published.payload.to_string())
        .expect("payload projection is listed")
        .clone();
    assert_eq!(projection.kind, "projection");
    assert_eq!(
        projection.source_version_uuid.as_deref(),
        Some(published.feature_head.to_string().as_str())
    );
    assert_ne!(projection.identity_digest, source_digest);
    let proposal = &lineage.proposals[0];
    assert_eq!(proposal.payload_version_uuid, published.payload.to_string());
    assert_eq!(
        proposal.source_version_uuid,
        published.feature_head.to_string()
    );
    assert_eq!(
        proposal.source_branch_uuid,
        published.feature_branch.to_string()
    );

    let hub = serve(&published, &lineage);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("proposal");
    let cloned = clone_into(&hub, &destination, None, Some(published.payload)).unwrap();
    assert_eq!(
        cloned["research_version_uuid"],
        published.payload.to_string()
    );
    assert_eq!(cloned["research_version_kind"], "projection");
    let graph = GraphForge::new(destination.to_str()).unwrap();
    let reopened = reference(&graph, published.payload);
    assert_eq!(hex(&reopened.identity_sha256), projection.identity_digest.0);
    assert_ne!(hex(&reopened.identity_sha256), source_digest.0);
    assert_eq!(
        reopened.version.content.source_version,
        Some(published.feature_head)
    );

    // A Hub that serves the projection's package as the source Version is
    // refused before any destination exists: the package's registry does not
    // carry the source Version with the source identity.
    let mut masquerade = lineage.clone();
    let payload_package = projection.package.clone().unwrap();
    masquerade
        .versions
        .iter_mut()
        .find(|version| version.version_uuid == published.feature_head.to_string())
        .unwrap()
        .package = Some(payload_package);
    let hub = serve(&published, &masquerade);
    let destination = root.path().join("masquerade");
    let error = clone_into(&hub, &destination, Some("feature/score"), None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("hub.package.research_version_mismatch"),
        "{error}"
    );
    assert!(!destination.exists());
}

fn minimal_documents(
    manifest_requirements: serde_json::Value,
    lineage: Option<&[u8]>,
) -> (Hub, String) {
    let repository = serde_json::json!({"owner":"curate","repository":"claims-fork"});
    let immutable = format!("sha256:{}", "a".repeat(64));
    let package_object = format!("sha256:{}", "c".repeat(64));
    let mut objects = vec![serde_json::json!({
        "digest": package_object, "length": 1,
        "media_type": PORTABLE_MEDIA_TYPE,
        "locations": ["https://objects.example/sha256/package"]
    })];
    let mut manifest = serde_json::json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":1},
        "repository":repository.clone(),"default_ref":"main","resolved_ref":"main",
        "immutable_version":immutable,
        "package":{
            "format":"graphforge-project/2",
            "package_digest":format!("sha256:{}", "b".repeat(64)),
            "producing_core_version":"0.6.0",
            "object_digest":package_object
        },
        "requirements":manifest_requirements,"capabilities":[],
    });
    let mut documents = BTreeMap::new();
    documents.insert(
        "https://objects.example/sha256/package".to_owned(),
        b"must not be read".to_vec(),
    );
    if let Some(bytes) = lineage {
        let digest = sha256(bytes);
        objects.push(serde_json::json!({
            "digest": digest.0, "length": bytes.len(),
            "media_type": RESEARCH_LINEAGE_MEDIA_TYPE,
            "locations": ["https://objects.example/sha256/lineage"]
        }));
        manifest["lineage"] = serde_json::json!({
            "format": RESEARCH_LINEAGE_FORMAT,
            "lineage_digest": format!("sha256:{}", "e".repeat(64)),
            "object_digest": digest.0,
        });
        documents.insert(
            "https://objects.example/sha256/lineage".to_owned(),
            bytes.to_vec(),
        );
    }
    objects.sort_by(|left, right| left["digest"].as_str().cmp(&right["digest"].as_str()));
    manifest["objects"] = serde_json::Value::Array(objects);
    let refs = serde_json::json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":1},
        "repository":repository,"default_ref":"main",
        "refs":[{"name":"main","target":immutable,"validator":format!("sha256:{}", "d".repeat(64))}]
    });
    documents.insert(endpoint_url("refs"), serde_json::to_vec(&refs).unwrap());
    documents.insert(
        endpoint_url("manifest"),
        serde_json::to_vec(&manifest).unwrap(),
    );
    (
        Hub {
            documents,
            requested: Mutex::new(Vec::new()),
        },
        "https://objects.example/sha256/package".to_owned(),
    )
}

#[test]
fn unknown_required_lineage_capability_fails_before_package_reads_or_project_mutation() {
    let lineage = serde_json::to_vec(&serde_json::json!({
        "boundary_anchors":[],"format":"graphforge-research-lineage/2","version":{"major":1,"minor":1},
        "repository":{"owner":"curate","repository":"claims-fork"},
        "immutable_version":format!("sha256:{}", "a".repeat(64)),
        "project_uuid":"01900000-0000-7000-8000-000000000001",
        "requirements":[{"capability":"research-lineage","major":3}],"capabilities":[],
        "branches":[],"versions":[],"proposals":[]
    }))
    .unwrap();
    let (hub, package_url) = minimal_documents(
        serde_json::json!([{"capability":"portable","major":1}]),
        Some(&lineage),
    );
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("project");
    let error = clone_into(&hub, &destination, Some("main"), None).unwrap_err();
    assert!(
        error.to_string().contains("hub.unsupported_future"),
        "{error}"
    );
    let requested = hub.requested();
    assert_eq!(
        requested,
        [
            endpoint_url("refs"),
            endpoint_url("manifest"),
            "https://objects.example/sha256/lineage".to_owned(),
        ]
    );
    assert!(!requested.contains(&package_url));
    assert!(!destination.exists(), "no project was created");
}

#[test]
fn unknown_required_manifest_research_capability_fails_before_any_object_read() {
    let (hub, _) = minimal_documents(
        serde_json::json!([
            {"capability":"portable","major":1},
            {"capability":"research-lineage","major":3}
        ]),
        None,
    );
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("project");
    let error = clone_into(&hub, &destination, Some("main"), None).unwrap_err();
    assert!(
        error.to_string().contains("hub.unsupported_future"),
        "{error}"
    );
    assert_eq!(
        hub.requested(),
        [endpoint_url("refs"), endpoint_url("manifest")]
    );
    assert!(!destination.exists(), "no project was created");
}

const RESEARCH_HELPER: &str = "hub_clone::research_clone_tests::subprocess_research_clone_helper";

/// Clone the research fixture named by the environment; a no-op otherwise.
#[test]
fn subprocess_research_clone_helper() {
    let Ok(fixture) = std::env::var("GRAPHFORGE_RESEARCH_CLONE_FIXTURE") else {
        return;
    };
    let fixture = PathBuf::from(fixture);
    let index: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(fixture.join("index.json")).unwrap()).unwrap();
    let hub = Hub {
        documents: index
            .into_iter()
            .map(|(url, file)| (url, std::fs::read(fixture.join(file)).unwrap()))
            .collect(),
        requested: Mutex::new(Vec::new()),
    };
    let destination =
        PathBuf::from(std::env::var("GRAPHFORGE_RESEARCH_CLONE_DESTINATION").unwrap());
    match clone_into(&hub, &destination, Some("main"), None) {
        Ok(result) => println!("CLONE_OK {}", result["generation_uuid"]),
        Err(error) => println!("CLONE_ERROR {error}"),
    }
}

#[test]
fn research_clone_writes_no_file_to_the_temporary_directory() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let hub = serve(&published, &lineage);
    let fixture = tempfile::tempdir().unwrap();
    let mut index = BTreeMap::new();
    for (number, (url, bytes)) in hub.documents.iter().enumerate() {
        let file = format!("document-{number}");
        std::fs::write(fixture.path().join(&file), bytes).unwrap();
        index.insert(url.clone(), file);
    }
    std::fs::write(
        fixture.path().join("index.json"),
        serde_json::to_vec(&index).unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("head");
    // The child's temporary directory is watched for its whole run. A tiny
    // one can hold empty directories (every facade view creates one for its
    // lazy adjacency cache) but no package bytes, so no regular file may
    // ever appear in it: research validation stages beside the destination.
    let temporary = root.path().join("temporary");
    std::fs::create_dir(&temporary).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            RESEARCH_HELPER,
            "--nocapture",
            "--test-threads=1",
        ])
        .env("GRAPHFORGE_RESEARCH_CLONE_FIXTURE", fixture.path())
        .env("GRAPHFORGE_RESEARCH_CLONE_DESTINATION", &destination)
        .env("TMPDIR", &temporary)
        .env("TMP", &temporary)
        .env("TEMP", &temporary)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut files = BTreeSet::new();
    let status = loop {
        collect_files(&temporary, &mut files);
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    let mut stdout = String::new();
    std::io::Read::read_to_string(&mut child.stdout.take().unwrap(), &mut stdout).unwrap();
    assert!(status.success(), "{stdout}");
    assert!(
        files.is_empty(),
        "files written to the temporary directory: {files:?}"
    );
    assert!(stdout.contains("CLONE_OK"), "{stdout}");
    let cloned = GraphForge::new(destination.to_str()).unwrap();
    assert!(
        stdout.contains(&generation(&cloned).to_string()),
        "{stdout}"
    );
    assert_eq!(score(&cloned), 1);
}

/// Record every regular file under `directory`; entries may vanish mid-walk.
fn collect_files(directory: &Path, files: &mut BTreeSet<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => collect_files(&entry.path(), files),
            Ok(_) => {
                files.insert(entry.path());
            }
            Err(_) => {}
        }
    }
}

/// Change the advertised default without changing native research identities.
fn default_ref(hub: &mut Hub, name: &str) {
    let limits = DiscoveryLimits::default();
    let mut manifest =
        DiscoveryManifest::from_json(&hub.documents[&endpoint_url("manifest")], limits).unwrap();
    manifest.default_ref = name.into();
    manifest.resolved_ref = name.into();
    let bytes = manifest.to_canonical_json().unwrap();
    let mut refs = RefSet::from_json(&hub.documents[&endpoint_url("refs")], limits).unwrap();
    refs.default_ref = name.into();
    if !refs.refs.iter().any(|reference| reference.name == name) {
        let mut reference = refs.refs[0].clone();
        reference.name = name.into();
        refs.refs.push(reference);
        refs.refs.sort_by(|a, b| a.name.cmp(&b.name));
    }
    for reference in &mut refs.refs {
        reference.validator = sha256(&bytes);
    }
    hub.documents.insert(endpoint_url("manifest"), bytes);
    hub.documents
        .insert(endpoint_url("refs"), refs.to_canonical_json().unwrap());
}

#[test]
fn plain_clone_uses_advertised_default_research_branch_and_explicit_selectors_override_it() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let mut hub = serve(&published, &lineage);
    default_ref(&mut hub, "feature/score");
    let root = tempfile::tempdir().unwrap();
    let mut default_generation = None;
    for (label, git_ref, version, expected) in [
        ("default", None, None, published.feature_head),
        (
            "explicit-default",
            Some("feature/score"),
            None,
            published.feature_head,
        ),
        ("explicit", Some("main"), None, published.main_head),
        (
            "version",
            None,
            Some(published.main_base),
            published.main_base,
        ),
    ] {
        let destination = root.path().join(label);
        let result = clone_into(&hub, &destination, git_ref, version).unwrap();
        assert_eq!(result["research_version_uuid"], expected.to_string());
        if label == "default" {
            default_generation = Some(result["generation_uuid"].clone());
        }
        if label == "explicit-default" {
            assert_eq!(Some(result["generation_uuid"].clone()), default_generation);
        }
        let reopened = GraphForge::new(destination.to_str()).unwrap();
        assert_eq!(
            reference(&reopened, expected).identity_sha256,
            reference(&published.fork, expected).identity_sha256
        );
        assert_eq!(
            score(&reopened),
            if expected == published.main_base {
                0
            } else if expected == published.main_head {
                1
            } else {
                2
            }
        );
    }
    assert!(
        !hub.requested()
            .contains(&object_url(&published.project_package.0.object_digest))
    );
    // Advertised lineage must never downgrade to a Project clone on failure.
    for (label, lineage) in [
        ("missing-default", lineage.clone()),
        ("missing-package", {
            let mut changed = lineage.clone();
            changed
                .versions
                .iter_mut()
                .find(|version| version.version_uuid == published.feature_head.to_string())
                .unwrap()
                .package = None;
            changed
        }),
    ] {
        let mut bad = serve(&published, &lineage);
        default_ref(
            &mut bad,
            if label == "missing-default" {
                "unpublished"
            } else {
                "feature/score"
            },
        );
        let destination = root.path().join(label);
        assert!(clone_into(&bad, &destination, None, None).is_err());
        assert!(!destination.exists());
        assert!(
            !bad.requested()
                .contains(&object_url(&published.project_package.0.object_digest))
        );
    }
}

#[test]
fn clone_refuses_a_lineage_whose_label_differs_from_its_ref() {
    let published = publish_history();
    // The publisher wrote a ref name that is not the Branch label: the cloned
    // Branch could not be pushed back under the ref it came from.
    let mut request = lineage_request(&published);
    request
        .branch_ref_names
        .insert(published.main_branch, "trunk".to_owned());
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(&request, &CancellationToken::new())
        .unwrap();
    let mut hub = serve(&published, &lineage);
    default_ref(&mut hub, "trunk");
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("trunk");
    let error = clone_into(&hub, &destination, Some("trunk"), None).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("hub.malformed_lineage: Branch label differs from its ref name"),
        "{error}"
    );
    assert!(!destination.exists());
}

#[test]
fn clone_installs_lineage_tags() {
    let mut published = publish_history();
    for (name, version) in [
        ("release/v1", published.main_head),
        ("feature/v0", published.feature_head),
    ] {
        let request = graphforge_api::SetResearchTagRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: generation(&published.fork),
            name: name.into(),
            version_uuid: Some(version),
        };
        published
            .fork
            .set_research_tag(&request, &CancellationToken::new())
            .unwrap();
    }
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(lineage.tags.len(), 2);
    let hub = serve(&published, &lineage);
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("main");
    clone_into(&hub, &destination, Some("main"), None).unwrap();
    let cloned = GraphForge::new(destination.to_str()).unwrap();
    // A tag installs with the Version it names; the feature Branch is not cloned.
    assert_eq!(
        cloned.research_version_retention().unwrap().tags,
        BTreeMap::from([("release/v1".to_owned(), published.main_head)])
    );
}

fn branchless_hub(
    package_digest: Sha256Digest,
    bytes: Vec<u8>,
) -> (Hub, Sha256Digest, Sha256Digest) {
    let project_object = sha256(&bytes);
    let lineage = ResearchLineage {
        boundary_anchors: vec![],
        format: RESEARCH_LINEAGE_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: RepositoryIdentity::parse(REPOSITORY).unwrap(),
        immutable_version: package_digest.clone(),
        project_uuid: Uuid::now_v7().to_string(),
        requirements: vec![ProtocolRequirement {
            capability: "research-lineage".into(),
            major: 2,
        }],
        capabilities: vec![],
        fork: None,
        branches: vec![],
        versions: vec![],
        proposals: vec![],
        tags: vec![],
        extensions: BTreeMap::new(),
    };
    let lineage_bytes = lineage.to_canonical_json().unwrap();
    let lineage_digest = sha256(&lineage_bytes);
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: lineage.repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
        immutable_version: package_digest.clone(),
        package: PortablePackageReference {
            format: PORTABLE_FORMAT.into(),
            package_digest,
            producing_core_version: graphforge_api::GRAPHFORGE_VERSION.into(),
            object_digest: project_object.clone(),
        },
        summary: None,
        ontology: None,
        lineage: Some(ResearchLineageReference {
            format: RESEARCH_LINEAGE_FORMAT.into(),
            lineage_digest: lineage.canonical_digest().unwrap(),
            object_digest: lineage_digest.clone(),
        }),
        requirements: vec![ProtocolRequirement {
            capability: "portable".into(),
            major: 1,
        }],
        capabilities: vec![],
        extensions: BTreeMap::new(),
        objects: {
            let mut objects = vec![
                ObjectDescriptor {
                    digest: project_object.clone(),
                    length: bytes.len() as u64,
                    media_type: PORTABLE_MEDIA_TYPE.into(),
                    locations: vec![object_url(&project_object)],
                },
                ObjectDescriptor {
                    digest: lineage_digest.clone(),
                    length: lineage_bytes.len() as u64,
                    media_type: RESEARCH_LINEAGE_MEDIA_TYPE.into(),
                    locations: vec![object_url(&lineage_digest)],
                },
            ];
            objects.sort_by(|a, b| a.digest.cmp(&b.digest));
            objects
        },
    };
    let manifest_bytes = manifest.to_canonical_json().unwrap();
    let refs = RefSet {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: lineage.repository.clone(),
        default_ref: "main".into(),
        refs: vec![RepositoryRef {
            name: "main".into(),
            target: manifest.immutable_version.clone(),
            validator: sha256(&manifest_bytes),
        }],
        extensions: BTreeMap::new(),
    };
    let hub = Hub {
        documents: BTreeMap::from([
            (endpoint_url("manifest"), manifest_bytes),
            (endpoint_url("refs"), refs.to_canonical_json().unwrap()),
            (object_url(&project_object), bytes),
            (object_url(&lineage_digest), lineage_bytes),
        ]),
        requested: Mutex::new(vec![]),
    };
    (hub, project_object, lineage_digest)
}

#[test]
fn branchless_lineage_preserves_complete_project_clone_and_corruption_never_downgrades() {
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().join("source").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:42})").unwrap();
    let output = root.path().join("complete.gfbundle");
    let exported = graph
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: output.clone(),
                representation: PortableOutput::Bundle,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits: PortableLimits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let bytes = std::fs::read(output).unwrap();
    let package_digest = Sha256Digest(exported.package_digest);
    let (mut hub, project_object, lineage_digest) = branchless_hub(package_digest, bytes);
    let destination = root.path().join("clone");
    let result = clone_into(&hub, &destination, None, None).unwrap();
    assert!(result.get("research_version_uuid").is_none());
    assert_eq!(score(&GraphForge::new(destination.to_str()).unwrap()), 42);
    assert!(hub.requested().contains(&object_url(&project_object)));
    hub.documents
        .insert(object_url(&lineage_digest), b"{}".to_vec());
    hub.requested.lock().unwrap().clear();
    let refused = root.path().join("corrupt");
    assert!(clone_into(&hub, &refused, None, None).is_err());
    assert!(!refused.exists());
    assert!(!hub.requested().contains(&object_url(&project_object)));
}

#[test]
fn branch_activation_retries_exact_native_receipt_but_never_resets_advanced_staging() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let hub = serve(&published, &lineage);
    let fixture = tempfile::tempdir().unwrap();
    let mut index = BTreeMap::new();
    for (number, (url, bytes)) in hub.documents.iter().enumerate() {
        let file = format!("document-{number}");
        std::fs::write(fixture.path().join(&file), bytes).unwrap();
        index.insert(url.clone(), file);
    }
    std::fs::write(
        fixture.path().join("index.json"),
        serde_json::to_vec(&index).unwrap(),
    )
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("must-not-be-initialized");
    let selected = lineage
        .branches
        .iter()
        .find(|b| b.ref_name == "main")
        .unwrap();
    assert!(
        GraphForge::continue_imported_research_branch(
            &absent,
            selected,
            &lineage,
            "https://graphforge.sh/curate/claims-fork",
            Uuid::now_v7()
        )
        .is_err()
    );
    assert!(!absent.exists());
    assert!(graphforge_api::read_committed_generation_identity(&absent).is_err());
    assert!(!absent.exists());
    let expected_import = graphforge_api::hub_research_clone_operation(
        REPOSITORY,
        &lineage.immutable_version.0,
        &selected.head_version_uuid,
        &lineage
            .version(&selected.head_version_uuid)
            .unwrap()
            .identity_digest
            .0,
    );
    for (name, failpoint, advance) in [
        ("before", "clone.before_branch_continuation", false),
        ("after", "clone.after_branch_continuation", false),
        ("advanced", "clone.after_branch_continuation", true),
        ("foreign", "clone.before_branch_continuation", false),
        ("unreadable", "clone.before_branch_continuation", false),
        ("import-lock", "project.after_writer_lock", false),
        ("import-manifest", "project.after_manifest_fsync", false),
    ] {
        let destination = root.path().join(name);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                RESEARCH_HELPER,
                "--nocapture",
                "--test-threads=1",
            ])
            .env("GRAPHFORGE_RESEARCH_CLONE_FIXTURE", fixture.path())
            .env("GRAPHFORGE_RESEARCH_CLONE_DESTINATION", &destination)
            .env("GRAPHFORGE_CLONE_FAILPOINT", failpoint)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                expected_import.to_string(),
            )
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(if name.starts_with("import-") {
                86
            } else {
                CLONE_FAILPOINT_EXIT
            }),
            "{}",
            String::from_utf8_lossy(&child.stdout)
        );
        assert!(!destination.exists());
        let staging = staging_path(&destination).unwrap();
        let import_operation =
            Uuid::parse_str(&std::fs::read_to_string(staging.join("operation")).unwrap()).unwrap();
        let staged = staging.join("project");
        let prior = if advance {
            let mut graph = GraphForge::new(staged.to_str()).unwrap();
            edit(&mut graph, published.main_branch, 77);
            Some(generation(&graph))
        } else if name == "foreign" {
            std::fs::rename(&staged, staging.join("saved-original-project")).unwrap();
            let graph = GraphForge::new(staged.to_str()).unwrap();
            graph.execute("CREATE (:Foreign {marker:42})").unwrap();
            Some(generation(&graph))
        } else {
            None
        };
        if name == "unreadable" {
            std::fs::write(staged.join("CURRENT"), b"unreadable staged CURRENT\n").unwrap();
            assert!(graphforge_api::read_committed_generation_identity(&staged).is_err());
        }
        let result = clone_into(&hub, &destination, Some("main"), None);
        if name == "unreadable" {
            assert!(result.is_err());
            assert!(!destination.exists());
            assert_eq!(
                std::fs::read(staged.join("CURRENT")).unwrap(),
                b"unreadable staged CURRENT\n"
            );
        } else if let Some(prior) = prior {
            assert!(result.is_err());
            assert!(!destination.exists());
            assert!(
                staged.exists(),
                "{name}: refused staging must not be removed"
            );
            assert_eq!(
                generation(&GraphForge::new(staged.to_str()).unwrap()),
                prior
            );
        } else {
            result.unwrap();
            let graph = GraphForge::new(destination.to_str()).unwrap();
            let state = graph.research_version_retention().unwrap();
            assert_eq!(state.heads[&published.main_branch], published.main_head);
            assert_eq!(state.heads.len(), 1);
            assert!(!state.branches.contains_key(&published.feature_branch));
            assert!(!state.materialized.contains(&published.feature_head));
            assert!(!state.versions.contains_key(&published.feature_head));
            assert!(
                state
                    .remote_branches
                    .values()
                    .any(|r| r.branch_uuid == published.feature_branch)
            );
            assert_eq!(score(&graph), 1);
            let prior = generation(&graph);
            assert_eq!(
                graphforge_api::read_committed_generation_identity(&destination).unwrap(),
                graph.committed_generation_identity().unwrap()
            );
            let mut graph = graph;
            assert!(
                graph
                    .execute_research_branch(
                        &ExecuteResearchBranchRequest {
                            label: None,
                            description: None,
                            author: None,
                            committer: None,
                            operation_uuid: Uuid::now_v7(),
                            expected_generation_uuid: prior,
                            branch_uuid: published.feature_branch,
                            version_uuid: Uuid::now_v7(),
                            created_at: 3,
                            query: "MATCH (n:Item) SET n.score=99".into(),
                        },
                        &CancellationToken::new()
                    )
                    .is_err()
            );
            assert_eq!(generation(&graph), prior);
            let selected = lineage
                .branches
                .iter()
                .find(|b| b.ref_name == "main")
                .unwrap();
            let receipt = GraphForge::continue_imported_research_branch(
                &destination,
                selected,
                &lineage,
                "https://graphforge.sh/curate/claims-fork",
                import_operation,
            )
            .unwrap()
            .unwrap();
            assert_eq!(receipt.generation_uuid, prior);
            let mut altered = lineage.clone();
            altered
                .branches
                .iter_mut()
                .find(|b| b.ref_name == "feature/score")
                .unwrap()
                .ref_name = "feature/renamed".into();
            assert!(
                GraphForge::continue_imported_research_branch(
                    &destination,
                    selected,
                    &altered,
                    "https://graphforge.sh/curate/claims-fork",
                    import_operation,
                )
                .is_err(),
                "a changed bound remote snapshot cannot reuse the activation receipt"
            );
            assert_eq!(generation(&graph), prior);
            let mut altered_metadata = lineage.clone();
            altered_metadata
                .branches
                .iter_mut()
                .find(|b| b.ref_name == "feature/score")
                .unwrap()
                .label
                .push_str(" changed");
            assert!(
                GraphForge::continue_imported_research_branch(
                    &destination,
                    selected,
                    &altered_metadata,
                    "https://graphforge.sh/curate/claims-fork",
                    import_operation,
                )
                .is_err(),
                "the entire bound discovery snapshot is committed, including remote metadata"
            );
            assert_eq!(generation(&graph), prior);
            assert!(
                GraphForge::continue_imported_research_branch(
                    &destination,
                    selected,
                    &lineage,
                    "https://graphforge.sh/curate/claims-fork",
                    Uuid::now_v7(),
                )
                .is_err(),
                "same operation cannot replay with another import generation"
            );
            assert_eq!(generation(&graph), prior);
        }
    }
}

#[test]
fn forged_project_boundary_anchor_is_not_native_history_authority() {
    let published = publish_history();
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    assert!(!lineage.boundary_anchors.is_empty());
    let root = tempfile::tempdir().unwrap();
    for variant in ["identity", "origin"] {
        let mut forged = lineage.clone();
        let anchor = &mut forged.boundary_anchors[0];
        if variant == "identity" {
            anchor.identity_digest = Sha256Digest(format!("sha256:{}", "0".repeat(64)));
        } else {
            anchor.origin_branch_uuid = Uuid::now_v7().to_string();
        }
        // The transport binds the altered document digest correctly; native
        // package genealogy, not a self-consistent transport hash, must refuse.
        let hub = serve(&published, &forged);
        let destination = root.path().join(variant);
        assert!(clone_into(&hub, &destination, Some("main"), None).is_err());
        assert!(!destination.exists());
    }
}

/// Allocated bytes (blocks, each hard-linked file once) under `root`.
#[cfg(unix)]
fn allocated_bytes(root: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    fn visit(directory: &Path, seen: &mut BTreeSet<(u64, u64)>, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = std::fs::symlink_metadata(entry.path()) else {
                continue;
            };
            if metadata.is_dir() {
                visit(&entry.path(), seen, total);
            } else if metadata.is_file() && seen.insert((metadata.dev(), metadata.ino())) {
                *total += metadata.blocks() * 512;
            }
        }
    }
    let mut total = 0;
    visit(root, &mut BTreeSet::new(), &mut total);
    total
}

/// The budget a research clone checks before it downloads bounds the disk the
/// clone holds beside its destination, measured from outside by sampling every
/// allocated block of the clone's parent directory: the downloaded package, the
/// import's stage, the validation object store and the rebuilt Version's
/// object store, each live at once.
///
/// The footprint read from the package's own controls bounds the same peak,
/// and the production budget, which knows only the package's length, must
/// never be below that exact footprint. This compact-graph fixture holds fewer
/// copies than the footprint counts (it has no temporary expanded graph), so
/// it is not a tightness check; the expanded-graph test below is.
#[cfg(unix)]
#[test]
fn the_budget_bounds_the_measured_peak_of_a_research_clone() {
    let published = publish_history_with_padding(800);
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let hub = serve(&published, &lineage);
    let (_, bundle) = &published.packages[&published.main_head];
    let package = tempfile::tempdir().unwrap();
    std::fs::write(package.path().join("head.gfpb"), bundle).unwrap();
    let footprint = graphforge_storage::portable_import_footprint(
        package.path().join("head.gfpb"),
        PortableLimits::default(),
        None,
    )
    .unwrap();
    assert!(footprint.research_bytes > 0, "{footprint:?}");

    let root = tempfile::tempdir().unwrap();
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let flag = std::sync::Arc::clone(&stop);
    let watched = root.path().to_path_buf();
    let sampler = std::thread::spawn(move || {
        let mut peak = 0;
        while !flag.load(Ordering::Relaxed) {
            peak = peak.max(allocated_bytes(&watched));
            std::thread::sleep(Duration::from_millis(1));
        }
        peak
    });
    clone_into(&hub, &root.path().join("head"), Some("main"), None).unwrap();
    stop.store(true, Ordering::Relaxed);
    let observed = sampler.join().unwrap();
    // The clone's parent holds the download as well as the import.
    let length = bundle.len() as u64;
    let exact = length + footprint.peak_bytes();
    let budget = required_space(length, 0, 0, false, true);
    println!(
        "research clone: package={length} payload={} research={} generated={} entries={} observed={observed} exact={exact} budget={budget}",
        footprint.payload_bytes,
        footprint.research_bytes,
        footprint.generated_bytes,
        footprint.entries
    );
    assert!(
        observed <= budget,
        "a research clone held {observed} bytes, more than the {budget} it budgets"
    );
    assert!(
        budget >= exact,
        "the budget {budget} is below the package's own footprint {exact}"
    );
    assert!(
        observed <= exact,
        "observed {observed} exceeds footprint {exact}"
    );
}

/// Verification and import are measured against their budgets on a package
/// whose largest Version is an expanded graph/files record of a few MiB, the
/// shape that holds the most copies at once: the stage, the validation store,
/// the temporary expanded graph and the Version's project, all alive while a
/// Version is rebuilt.
///
/// The real clone runs `verify_discovered_research_version` and then the real
/// import in its own scratch root while a sampler thread records the allocated
/// bytes under the root (the download, staging and target) every millisecond,
/// split at the start of the import. Sampling can only miss a peak, so an
/// observed peak above its budget is a real overrun and never a flake. The
/// invariant is observed peak ≤ budget and nothing more: a budget far above a
/// sampled peak is not a failure, so removing a copy never breaks the test.
#[cfg(unix)]
#[test]
fn research_verification_and_import_stay_within_their_budgets_with_an_expanded_graph() {
    let published = publish_history_shaped(0, 12, 512 * 1024, true);
    let lineage = published
        .fork
        .build_research_lineage_for_discovery(
            &lineage_request(&published),
            &CancellationToken::new(),
        )
        .unwrap();
    let hub = serve(&published, &lineage);
    let (_, bundle) = &published.packages[&published.main_base];
    let package = tempfile::tempdir().unwrap();
    std::fs::write(package.path().join("base.gfpb"), bundle).unwrap();
    let footprint = graphforge_storage::portable_import_footprint(
        package.path().join("base.gfpb"),
        PortableLimits::default(),
        None,
    )
    .unwrap();
    assert!(footprint.research_bytes > 2 << 20, "{footprint:?}");

    let root = tempfile::tempdir().unwrap();
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let verification_peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, stop, watched) = (
            std::sync::Arc::clone(&peak),
            std::sync::Arc::clone(&stop),
            root.path().to_path_buf(),
        );
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(allocated_bytes(&watched), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let boundary = {
        let (peak, verification_peak) = (
            std::sync::Arc::clone(&peak),
            std::sync::Arc::clone(&verification_peak),
        );
        move || verification_peak.store(peak.swap(0, Ordering::Relaxed), Ordering::Relaxed)
    };
    let delays = CloneDelays {
        before_import: Some(Box::new(boundary)),
        ..CloneDelays::default()
    };
    run_clone_profiled_with_delays(
        &hub,
        CloneArgs {
            hub: None,
            token_env: None,
            protection_bypass_env: None,
            repository: REPOSITORY.into(),
            destination: Some(root.path().join("base")),
            telemetry_endpoint: None,
            git_ref: None,
            version_uuid: Some(published.main_base.to_string()),
        },
        true,
        &mut Vec::new(),
        &TelemetryRuntime::default(),
        &delays,
        &mut CloneEnv::quiet(),
    )
    .unwrap();
    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let verification = verification_peak.load(Ordering::Relaxed);
    let import = peak.load(Ordering::Relaxed);

    // Both phases hold the downloaded package as well.
    let length = bundle.len() as u64;
    let verification_budget = length + footprint.verification_peak_bytes();
    let import_budget = length + footprint.peak_bytes();
    let clone_budget = required_space(length, 0, 0, false, true);
    println!(
        "expanded research: package={length} payload={} research={} entries={} \
         verification observed={verification} budget={verification_budget} \
         import observed={import} budget={import_budget} clone budget={clone_budget}",
        footprint.payload_bytes, footprint.research_bytes, footprint.entries
    );
    assert!(
        verification <= verification_budget,
        "verification held {verification} bytes, more than its {verification_budget} \
         (import held {import} against {import_budget}; package {length}, payload {}, research {})",
        footprint.payload_bytes,
        footprint.research_bytes
    );
    assert!(
        import <= import_budget,
        "the import held {import} bytes, more than its {import_budget} \
         (verification held {verification} against {verification_budget}; package {length}, \
         payload {}, research {})",
        footprint.payload_bytes,
        footprint.research_bytes
    );
    assert!(
        verification.max(import) <= clone_budget,
        "the clone budgets {clone_budget} for a peak of {} (verification {verification}, \
         import {import}, exact budgets {verification_budget} and {import_budget})",
        verification.max(import)
    );
}
