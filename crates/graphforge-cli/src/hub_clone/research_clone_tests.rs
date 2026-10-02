//! End-to-end research lineage: a real Fork with two Branches and a Proposal is
//! published through the registry-derived lineage builder, served as Hub
//! documents and objects, listed by a consumer, and cloned through the real
//! `gf clone --ref` / `--version-uuid` path into destinations that reopen.

use super::*;
use arrow::array::{Array as _, FixedSizeBinaryArray, Int64Array};
use graphforge_api::{
    BranchSource, BuildResearchLineageRequest, CancellationToken, CreateResearchBranchRequest,
    ExecuteResearchBranchRequest, ExportResearchRequest, ForkResearchRequest, PortableSelection,
    PortableV2ExportRequest, PortableV2Output, PortableV2SelectionProfile, ResearchFieldIdentity,
    ResearchReference, ResearchReferenceTarget, SliceMembers, SliceRequest, SliceSelector,
    SliceSource, SubmitResearchProposalRequest, WorkspaceResearchMetadata,
};
use graphforge_discovery::{
    DISCOVERY_FORMAT, PORTABLE_V2_FORMAT, PORTABLE_V2_MEDIA_TYPE, PortablePackageReference,
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

#[allow(clippy::too_many_lines)]
fn publish_history() -> Published {
    let cancellation = CancellationToken::new();
    let root = tempfile::tempdir().unwrap();
    // The origin Project: one retained Version to fork from.
    let mut origin = GraphForge::new(root.path().join("origin").to_str()).unwrap();
    origin.execute("CREATE (:Item {score:0})").unwrap();
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
        "feature",
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
                    format: PORTABLE_V2_FORMAT.into(),
                    package_digest: Sha256Digest(exported.package_digest),
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
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: output.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::DataComponents,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let bytes = std::fs::read(&output).unwrap();
    let project_package = (
        PortablePackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(exported.package_digest),
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
        PORTABLE_V2_MEDIA_TYPE,
        &published.project_package.1,
    );
    for (package, bytes) in published.packages.values() {
        publish(&package.object_digest, PORTABLE_V2_MEDIA_TYPE, bytes);
    }
    publish(&lineage_object, RESEARCH_LINEAGE_MEDIA_TYPE, &lineage_bytes);
    objects.sort_by(|left, right| left.digest.0.cmp(&right.digest.0));
    let manifest = DiscoveryManifest {
        format: DISCOVERY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: repository.clone(),
        default_ref: "main".into(),
        resolved_ref: "main".into(),
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
            capability: "portable-v2".into(),
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
        default_ref: "main".into(),
        refs: ["feature/score", "main"]
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
        "media_type": PORTABLE_V2_MEDIA_TYPE,
        "locations": ["https://objects.example/sha256/package"]
    })];
    let mut manifest = serde_json::json!({
        "format":"graphforge-discovery/1","version":{"major":1,"minor":1},
        "repository":repository.clone(),"default_ref":"main","resolved_ref":"main",
        "immutable_version":immutable,
        "package":{
            "format":"graphforge-project/2",
            "package_digest":format!("sha256:{}", "b".repeat(64)),
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
        "format":"graphforge-research-lineage/1","version":{"major":1,"minor":1},
        "repository":{"owner":"curate","repository":"claims-fork"},
        "immutable_version":format!("sha256:{}", "a".repeat(64)),
        "project_uuid":"01900000-0000-7000-8000-000000000001",
        "requirements":[{"capability":"research-lineage","major":2}],"capabilities":[],
        "branches":[],"versions":[],"proposals":[]
    }))
    .unwrap();
    let (hub, package_url) = minimal_documents(
        serde_json::json!([{"capability":"portable-v2","major":1}]),
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
            {"capability":"portable-v2","major":1},
            {"capability":"research-lineage","major":2}
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
