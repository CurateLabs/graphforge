use super::*;
use crate::{CommitRequest, PublishIntent, canonical_json, sha256_digest};
use graphforge_discovery::{
    LineageBranch, LineageVersion, ProtocolRequirement, ProtocolVersion, RESEARCH_LINEAGE_FORMAT,
    RepositoryIdentity, Sha256Digest,
};
use serde_json::json;

fn uuid(n: u8) -> String {
    format!("00000000-0000-4000-8000-{n:012}")
}
fn lineage() -> ResearchLineage {
    let digest = Sha256Digest(sha256_digest(b"immutable"));
    ResearchLineage {
        format: RESEARCH_LINEAGE_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: RepositoryIdentity::parse("curate/claims").unwrap(),
        immutable_version: digest.clone(),
        project_uuid: uuid(1),
        requirements: vec![ProtocolRequirement {
            capability: "research-lineage".into(),
            major: 2,
        }],
        capabilities: vec![],
        fork: None,
        boundary_anchors: vec![],
        proposals: vec![],
        extensions: BTreeMap::default(),
        branches: vec![LineageBranch {
            branch_uuid: uuid(2),
            ref_name: "main".into(),
            project_uuid: uuid(1),
            head_version_uuid: uuid(4),
            parent_branch_uuid: None,
            origin_version_uuid: uuid(3),
            base_version_uuid: uuid(3),
            selection_sha256: digest.clone(),
            label: "main".into(),
        }],
        versions: [(3, vec![]), (4, vec![uuid(3)])]
            .into_iter()
            .map(|(n, parents)| LineageVersion {
                version_uuid: uuid(n),
                parents,
                identity_digest: digest.clone(),
                kind: "complete".into(),
                branch_uuid: uuid(2),
                source_version_uuid: None,
                author: None,
                committer: None,
                package: None,
            })
            .collect(),
    }
}
#[test]
fn rewrite_is_explicit_and_never_weakens_immutable_authority() {
    let old = lineage();
    let mut next = old.clone();
    let mut sibling = next.versions[1].clone();
    sibling.version_uuid = uuid(5);
    next.versions.push(sibling);
    next.branches[0].head_version_uuid = uuid(5);
    let refs = vec!["main".into()];
    let limits = DiscoveryLimits::default();
    assert_eq!(
        validate_history_transition(&old, &next, &refs, false, false, limits)
            .unwrap_err()
            .code,
        HubPublishErrorCode::NonFastForward
    );
    assert!(
        validate_history_transition(&old, &next, &refs, true, false, limits)
            .unwrap()
            .is_empty()
    );
    next.branches[0].label = "different creation".into();
    assert!(validate_history_transition(&old, &next, &refs, true, true, limits).is_err());
    next = old.clone();
    next.versions[1].identity_digest = Sha256Digest(sha256_digest(b"forged"));
    assert!(validate_history_transition(&old, &next, &refs, true, true, limits).is_err());
}
#[test]
fn credit_may_be_added_to_a_pre_credit_entry_but_never_changed_or_removed() {
    let credit = |name: &str| graphforge_discovery::CreditSignature {
        name: name.into(),
        email: None,
        orcid: None,
    };
    let old = lineage();
    let refs = vec!["main".into()];
    let limits = DiscoveryLimits::default();
    // A repository published before credit was displayed gains it: identity,
    // parents and payload are unchanged, so nothing committed has moved.
    let mut credited = old.clone();
    credited.versions[1].author = Some(credit("Alice"));
    credited.versions[1].committer = Some(credit("Alice"));
    assert!(validate_history_transition(&old, &credited, &refs, false, false, limits).is_ok());
    // Once published, credit is permanent: it can be neither changed nor removed.
    let mut changed = credited.clone();
    changed.versions[1].author = Some(credit("Mallory"));
    assert!(validate_history_transition(&credited, &changed, &refs, true, true, limits).is_err());
    let mut removed = credited.clone();
    removed.versions[1].committer = None;
    assert!(validate_history_transition(&credited, &removed, &refs, true, true, limits).is_err());
}
fn citation(
    author: Option<&str>,
    committer: Option<&str>,
) -> graphforge_discovery::LineageForkOrigin {
    let credit = |name: &str| graphforge_discovery::CreditSignature {
        name: name.into(),
        email: None,
        orcid: None,
    };
    graphforge_discovery::LineageForkOrigin {
        origin_repository: RepositoryIdentity::parse("curate/origin").unwrap(),
        origin_project_uuid: uuid(7),
        origin_version_uuid: uuid(8),
        origin_version_identity: Sha256Digest(sha256_digest(b"origin identity")),
        origin_author: author.map(credit),
        origin_committer: committer.map(credit),
    }
}

#[test]
fn fork_origin_credit_may_be_filled_but_never_changed_or_removed() {
    let refs = vec!["main".into()];
    let limits = DiscoveryLimits::default();
    let transition = |old: Option<graphforge_discovery::LineageForkOrigin>,
                      new: Option<graphforge_discovery::LineageForkOrigin>| {
        let mut previous = lineage();
        previous.fork = old;
        let mut next = previous.clone();
        next.fork = new;
        validate_history_transition(&previous, &next, &refs, false, false, limits)
    };
    // A citation published before credit display gains it (both, or either).
    assert!(
        transition(
            Some(citation(None, None)),
            Some(citation(Some("Ada"), Some("Grace")))
        )
        .is_ok()
    );
    assert!(
        transition(
            Some(citation(None, None)),
            Some(citation(Some("Ada"), None))
        )
        .is_ok()
    );
    assert!(
        transition(
            Some(citation(Some("Ada"), None)),
            Some(citation(Some("Ada"), Some("Grace")))
        )
        .is_ok()
    );
    // An identical citation is untouched.
    assert!(
        transition(
            Some(citation(Some("Ada"), Some("Grace"))),
            Some(citation(Some("Ada"), Some("Grace")))
        )
        .is_ok()
    );
    // Published credit is permanent: changed or removed is a different origin.
    assert!(
        transition(
            Some(citation(Some("Ada"), None)),
            Some(citation(Some("Mallory"), None))
        )
        .is_err()
    );
    assert!(
        transition(
            Some(citation(Some("Ada"), Some("Grace"))),
            Some(citation(Some("Ada"), None))
        )
        .is_err()
    );
    assert!(
        transition(
            Some(citation(Some("Ada"), None)),
            Some(citation(None, None))
        )
        .is_err()
    );
    // Filling credit never excuses a different origin identity.
    for tamper in [
        |c: &mut graphforge_discovery::LineageForkOrigin| {
            c.origin_repository = RepositoryIdentity::parse("curate/elsewhere").unwrap();
        },
        |c: &mut graphforge_discovery::LineageForkOrigin| c.origin_project_uuid = uuid(9),
        |c: &mut graphforge_discovery::LineageForkOrigin| c.origin_version_uuid = uuid(9),
        |c: &mut graphforge_discovery::LineageForkOrigin| {
            c.origin_version_identity = Sha256Digest(sha256_digest(b"other"));
        },
    ] {
        let mut changed = citation(Some("Ada"), Some("Grace"));
        tamper(&mut changed);
        assert!(transition(Some(citation(None, None)), Some(changed)).is_err());
    }
    // A citation is neither dropped nor introduced.
    assert!(transition(Some(citation(None, None)), None).is_err());
    assert!(transition(None, Some(citation(None, None))).is_err());
    assert!(transition(None, None).is_ok());
}
#[test]
fn pruning_is_explicit_and_preserves_surviving_parent_and_origin_dependencies() {
    let old = lineage();
    let mut next = old.clone();
    next.versions.pop();
    next.branches[0].head_version_uuid = uuid(3);
    let refs = vec!["main".into()];
    let limits = DiscoveryLimits::default();
    assert!(validate_history_transition(&old, &next, &refs, true, false, limits).is_err());
    assert_eq!(
        validate_history_transition(&old, &next, &refs, true, true, limits).unwrap(),
        vec![uuid(4)]
    );
    let mut child = old.versions[1].clone();
    child.version_uuid = uuid(5);
    child.parents = vec![uuid(4)];
    next.versions.push(child);
    next.branches[0].head_version_uuid = uuid(5);
    assert!(validate_history_transition(&old, &next, &refs, true, true, limits).is_err());
    next = old.clone();
    next.versions.remove(0);
    assert!(validate_history_transition(&old, &next, &refs, true, true, limits).is_err());
}
#[test]
fn omitted_flags_preserve_legacy_bytes_and_each_destructive_flag_changes_commitment() {
    let intent = PublishIntent {
        rewrite: false,
        prune: false,
        repository: RepositoryIdentity::parse("curate/claims").unwrap(),
        intent_digest: sha256_digest(b"intent"),
        objects: vec![],
        manifest_validator: sha256_digest(b"manifest"),
        refs: vec!["main".into()],
        expected_revision: None,
    };
    let original = canonical_json(
        &json!({"format":crate::HUB_PUBLISH_FORMAT,"repository":{"owner":"curate","repository":"claims"},"intent_digest":intent.intent_digest,"objects":[],"manifest_validator":intent.manifest_validator,"refs":["main"],"expected_revision":null}),
    );
    assert_eq!(intent.canonical_bytes(), original);
    let mut changed = intent.clone();
    changed.rewrite = true;
    assert_ne!(intent.request_commitment(), changed.request_commitment());
    changed.rewrite = false;
    changed.prune = true;
    assert_ne!(intent.request_commitment(), changed.request_commitment());
    let request = CommitRequest {
        rewrite: false,
        prune: false,
        manifest: json!({}),
        refs: vec!["main".into()],
        expected_revision: None,
    };
    assert_eq!(
        serde_json::to_value(request).unwrap(),
        json!({"manifest":{},"refs":["main"],"expected_revision":null})
    );
    for value in [json!(null), json!(1), json!("true")] {
        assert!(
            CommitRequest::from_json(
                &serde_json::to_vec(
                    &json!({"manifest":{},"refs":["main"],"expected_revision":null,"prune":value})
                )
                .unwrap()
            )
            .is_err()
        );
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn destructive_negotiation_requires_the_exact_offered_capability() {
    use crate::{HubMethod, HubRequest, ReferenceHub};
    let hub = ReferenceHub::new();
    let response = hub.handle(&HubRequest::new(
        HubMethod::Get,
        "https://hub.example/curate/claims/.gf/publish",
    ));
    let mut capabilities = PublishCapabilities::from_json(&response.body).unwrap();
    capabilities
        .capabilities
        .retain(|c| c.capability != HISTORY_MUTATION_CAPABILITY);
    assert!(require_history_capability(&capabilities, false, false).is_ok());
    assert_eq!(
        require_history_capability(&capabilities, true, false)
            .unwrap_err()
            .code,
        HubPublishErrorCode::UnsupportedFuture
    );
    capabilities
        .capabilities
        .push(Capability::new(HISTORY_MUTATION_CAPABILITY, 2));
    assert!(require_history_capability(&capabilities, false, true).is_err());
    capabilities
        .capabilities
        .push(Capability::new(HISTORY_MUTATION_CAPABILITY, 1));
    assert!(require_history_capability(&capabilities, true, true).is_ok());
}

#[test]
fn proposal_projection_and_branch_dependencies_are_never_erased() {
    use graphforge_discovery::{LineageProposal, PORTABLE_FORMAT, PortablePackageReference};
    let mut next = lineage();
    let package = PortablePackageReference {
        format: PORTABLE_FORMAT.into(),
        package_digest: Sha256Digest(sha256_digest(b"package")),
        object_digest: Sha256Digest(sha256_digest(b"object")),
        producing_core_version: "0.6.0".into(),
    };
    next.proposals.push(LineageProposal {
        proposal_uuid: uuid(8),
        source_branch_uuid: uuid(2),
        source_version_uuid: uuid(4),
        payload_version_uuid: uuid(5),
        package,
    });
    for removed in [uuid(3), uuid(4), uuid(5)] {
        assert!(validate_dependencies(&next, &[removed]).is_err());
    }
    next.proposals.clear();
    next.versions[1].source_version_uuid = Some(uuid(6));
    assert!(validate_dependencies(&next, &[uuid(6)]).is_err());
    assert!(validate_dependencies(&next, &[uuid(9)]).is_ok());
}

#[test]
fn session_requires_destructive_semantics_and_rejects_unknown_major() {
    assert!(require_history_requirement(&[], false, false).is_ok());
    assert_eq!(
        require_history_requirement(&[], true, false)
            .unwrap_err()
            .code,
        HubPublishErrorCode::UnsupportedFuture
    );
    assert!(
        require_history_requirement(
            &[Capability::new(HISTORY_MUTATION_CAPABILITY, 2)],
            true,
            true
        )
        .is_err()
    );
    assert!(
        require_history_requirement(
            &[Capability::new(HISTORY_MUTATION_CAPABILITY, 1)],
            false,
            true
        )
        .is_ok()
    );
}
