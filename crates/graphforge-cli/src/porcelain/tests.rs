//! Argument mapping of the porcelain verbs onto native requests; the end-to-end
//! behaviour is proved by the walkthrough in `hub_publish::tests::porcelain`.
use super::local::commit_request;
use super::remote::push_args;
use super::*;
use clap::Parser as _;
use graphforge_api::{ResearchRemoteBranch, ResearchSignature};

fn registry(labels: &[&str]) -> (ResearchRegistry, Vec<Uuid>) {
    let mut registry = ResearchRegistry::default();
    let mut ids = Vec::new();
    for (index, label) in labels.iter().enumerate() {
        let branch = Uuid::from_u128(0x100 + index as u128);
        registry.branches.insert(
            branch,
            graphforge_storage::research_versions::ResearchBranchRecord {
                branch_uuid: branch,
                project_uuid: Uuid::from_u128(1),
                parent_branch_uuid: None,
                origin_version_uuid: Uuid::from_u128(2),
                base_version_uuid: Uuid::from_u128(3),
                creator_uuid: Uuid::from_u128(4),
                created_at: 0,
                label: (*label).to_owned(),
                selection_sha256: [0; 32],
            },
        );
        registry
            .heads
            .insert(branch, Uuid::from_u128(0x200 + index as u128));
        ids.push(branch);
    }
    (registry, ids)
}

fn parse(arguments: &[&str]) -> crate::Command {
    let mut argv = vec!["gf"];
    argv.extend_from_slice(arguments);
    crate::Cli::try_parse_from(argv)
        .unwrap()
        .command
        .expect("a subcommand")
}

fn signature(name: &str) -> ResearchSignature {
    ResearchSignature {
        name: name.into(),
        email: None,
        orcid: None,
    }
}

#[test]
fn branch_resolution_order_is_flag_current_then_only() {
    let (mut registry, ids) = registry(&["main", "feature/score"]);
    // Two Branches and no selection: the resolver stops and says how to select one.
    let unselected = resolve_branch(&registry, None).unwrap_err().to_string();
    assert!(
        unselected.contains("GF_RESEARCH_BRANCH_UNSELECTED"),
        "{unselected}"
    );
    assert!(unselected.contains("gf switch"));
    // The current Branch.
    registry.current_branch = Some(ids[1]);
    assert_eq!(resolve_branch(&registry, None).unwrap(), ids[1]);
    // The flag beats the current Branch.
    assert_eq!(resolve_branch(&registry, Some("main")).unwrap(), ids[0]);
    resolve_branch(&registry, Some("missing")).unwrap_err();
    // A current Branch that no longer resolves is ignored, not trusted.
    registry.current_branch = Some(Uuid::from_u128(0xdead));
    resolve_branch(&registry, None).unwrap_err();
    // The only Branch needs no selection.
    let (only, only_ids) = self::registry(&["main"]);
    assert_eq!(resolve_branch(&only, None).unwrap(), only_ids[0]);
}

#[test]
fn commit_builds_an_execute_request_on_the_resolved_branch() {
    let (mut registry, ids) = registry(&["main", "other"]);
    registry.current_branch = Some(ids[1]);
    let crate::Command::Commit(args) = parse(&[
        "commit",
        "-m",
        "Raise scores\n\nBecause the source changed",
        "--query",
        "MATCH (n:Item) SET n.score = 2",
    ]) else {
        panic!("not a commit");
    };
    let branch = resolve_branch(&registry, args.select.branch.as_deref()).unwrap();
    let operation = Uuid::now_v7();
    let generation = Uuid::from_u128(0x77);
    let request = commit_request(
        branch,
        &args,
        operation,
        generation,
        (signature("Ada"), signature("Grace")),
        "MATCH (n:Item) SET n.score = 2".into(),
    );
    assert_eq!(request.branch_uuid, ids[1]);
    assert_eq!(request.operation_uuid, operation);
    assert_eq!(request.expected_generation_uuid, generation);
    assert_eq!(request.query, "MATCH (n:Item) SET n.score = 2");
    assert_eq!(
        request.description.as_deref(),
        Some("Raise scores\n\nBecause the source changed")
    );
    assert_eq!(request.label, None);
    assert_eq!(request.author, Some(signature("Ada")));
    assert_eq!(request.committer, Some(signature("Grace")));
    // A rerun with the same operation UUID builds the same request bytes.
    let again = commit_request(
        branch,
        &args,
        operation,
        generation,
        (signature("Ada"), signature("Grace")),
        request.query.clone(),
    );
    assert_eq!(again, request);
    assert_eq!(request.version_uuid, derived_uuid(operation, "version"));
    assert_eq!(request.created_at, created_at(operation));
}

#[test]
fn commit_requires_a_message_and_exactly_one_change() {
    for arguments in [
        &["commit", "--query", "RETURN 1"][..],
        &["commit", "-m", "x"],
        &[
            "commit", "-m", "x", "--query", "RETURN 1", "--file", "q.cypher",
        ],
    ] {
        let mut argv = vec!["gf"];
        argv.extend_from_slice(arguments);
        assert!(crate::Cli::try_parse_from(argv).is_err(), "{arguments:?}");
    }
}

#[test]
fn push_defaults_repository_and_ref_from_the_remote_record() {
    let (mut registry, ids) = registry(&["main"]);
    let remote = ResearchRemoteBranch {
        repository_url: "https://hub.test/curate/claims".into(),
        project_uuid: Uuid::from_u128(1),
        ref_name: "main".into(),
        branch_uuid: ids[0],
        head_version_uuid: Uuid::from_u128(0x200),
        identity_sha256: [0; 32],
    };
    registry
        .remote_branches
        .insert(remote.key().unwrap(), remote);
    let crate::Command::Push(args) = parse(&["push", "--rewrite"]) else {
        panic!("not a push");
    };
    let publish = push_args(&registry, &args).unwrap();
    assert_eq!(publish.repository, "https://hub.test/curate/claims");
    assert_eq!(publish.git_ref.as_deref(), Some("main"));
    assert!(publish.rewrite && !publish.prune);
    assert!(publish.version_uuid.is_none() && publish.fork_of.is_none());

    // A Branch without an upstream needs the repository named.
    let (bare, _) = self::registry(&["main"]);
    let error = push_args(&bare, &args)
        .err()
        .expect("no upstream")
        .to_string();
    assert!(error.contains("GF_RESEARCH_UPSTREAM_UNSET"), "{error}");
    let crate::Command::Push(named) = parse(&["push", "curate/claims"]) else {
        panic!("not a push");
    };
    let publish = push_args(&bare, &named).unwrap();
    assert_eq!(publish.repository, "curate/claims");
    assert_eq!(publish.git_ref.as_deref(), Some("main"));

    // `--ref` names a Branch by its label, so it cannot disagree with `--branch`.
    let crate::Command::Push(conflicting) =
        parse(&["push", "curate/claims", "--ref", "a", "--branch", "b"])
    else {
        panic!("not a push");
    };
    assert!(push_args(&bare, &conflicting).is_err());
}

#[test]
fn operation_identity_must_be_a_uuid_v7_and_derives_stable_values() {
    let operation = Uuid::now_v7();
    assert_eq!(
        operation_uuid(Some(&operation.to_string())).unwrap(),
        operation
    );
    operation_uuid(Some(&Uuid::new_v4().to_string())).unwrap_err();
    operation_uuid(Some("not-a-uuid")).unwrap_err();
    assert_ne!(
        derived_uuid(operation, "version"),
        derived_uuid(operation, "branch")
    );
    assert_eq!(created_at(operation), created_at(operation));
    assert!(created_at(operation) > 1_700_000_000_000_000);
}
