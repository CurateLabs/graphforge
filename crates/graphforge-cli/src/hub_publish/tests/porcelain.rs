//! The research porcelain end to end (ADR 0061): real `gf` argv through the CLI's
//! own parser and dispatcher, against the reference Hub over the product HTTP
//! transport. No request is written by hand: every operation is a verb.
//!
//! The product transport only speaks HTTPS to public addresses, so the Hub is
//! reached through the loopback transport every publish and clone test uses, and
//! the verbs run in-process; `tests/research_porcelain_walkthrough.rs` drives the
//! built `gf` binary through the verbs that need no Hub.
use super::*;
use crate::porcelain::remote::run_remote_with;
use crate::{Cli, Command};
use clap::Parser as _;
use serde_json::Value;

const REPOSITORY: &str = "https://hub.test/curate/claims";
const ALICE: &str = "Alice <alice@example.org>";
const BOB: &str = "Bob <bob@example.org>";

/// Run one `gf` invocation for the Project at `project` and parse its JSON.
fn gf(hub: &LoopbackHub, project: &Path, arguments: &[&str]) -> Result<Value, GfError> {
    let project_text = project.to_str().unwrap();
    let mut argv = vec!["gf", "--project", project_text, "--json"];
    argv.extend_from_slice(arguments);
    let command = Cli::try_parse_from(argv)
        .unwrap_or_else(|error| panic!("gf {arguments:?} does not parse: {error}"))
        .command
        .expect("a subcommand");
    let mut output = Vec::new();
    match command {
        Command::Clone(args) => {
            run_clone_with(&hub.transport(), args, true, &mut output)?;
        }
        Command::Publish(args) => {
            let mut graph = GraphForge::new(Some(project_text))?;
            crate::hub_publish::publish_with_credentials(
                &hub.transport(),
                &mut graph,
                &args,
                Some(token(hub, "curate/claims")),
                None,
                true,
                &mut output,
            )?;
        }
        command @ (Command::Push(_) | Command::Pull(_)) => {
            let mut graph = GraphForge::new(Some(project_text))?;
            run_remote_with(
                &hub.transport(),
                &|_| Ok(Some(token(hub, "curate/claims"))),
                &mut graph,
                command,
                true,
                &mut output,
            )?;
        }
        command @ Command::Log(_) => {
            let mut graph = GraphForge::new(Some(project_text))?;
            if matches!(&command, Command::Log(args) if args.remote.is_some()) {
                run_remote_with(
                    &hub.transport(),
                    &|_| Ok(None),
                    &mut graph,
                    command,
                    true,
                    &mut output,
                )?;
            } else {
                crate::porcelain::run_local(&mut graph, command, true, &mut output)?;
            }
        }
        command => {
            let mut graph = GraphForge::new(Some(project_text))?;
            crate::porcelain::run_local(&mut graph, command, true, &mut output)?;
        }
    }
    Ok(serde_json::from_slice(&output).unwrap_or(Value::Null))
}

fn ok(hub: &LoopbackHub, project: &Path, arguments: &[&str]) -> Value {
    gf(hub, project, arguments).unwrap_or_else(|error| panic!("gf {arguments:?}: {error}"))
}

fn score(project: &Path) -> i64 {
    let graph = GraphForge::new(project.to_str()).unwrap();
    let registry = graph.research_version_retention().unwrap();
    let branch = *registry.branches.keys().next().unwrap();
    sum(graph
        .open_research_branch(branch)
        .unwrap()
        .graph()
        .execute("MATCH (n:Item) RETURN sum(n.score)")
        .unwrap())
}

fn sum(result: graphforge_api::ExecutionResult) -> i64 {
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

/// A Project with one Item, as a researcher would have before the first Branch.
fn seeded(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    let graph = GraphForge::new(path.to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    path
}

fn commit(hub: &LoopbackHub, project: &Path, who: &str, message: &str, value: i64) -> Value {
    ok(
        hub,
        project,
        &[
            "commit",
            "-m",
            message,
            "--query",
            &format!("MATCH (n:Item) SET n.score = {value}"),
            "--author",
            who,
        ],
    )
}

fn names(entries: &Value) -> Vec<String> {
    entries
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["description"].as_str().unwrap_or("").to_owned())
        .collect()
}

#[test]
fn porcelain_walkthrough_clones_commits_tags_pushes_pulls_and_logs() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");

    // Alice starts a Branch and publishes it; the ref is the Branch label. Until
    // it is published the Branch has no upstream and a push must name one.
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    let unpublished = gf(&hub, &alice, &["push"]).unwrap_err().to_string();
    assert!(
        unpublished.contains("GF_RESEARCH_UPSTREAM_UNSET"),
        "{unpublished}"
    );
    ok(&hub, &alice, &["push", REPOSITORY]);
    // The confirmed publication is her upstream, and she is not ahead of it.
    let tracked = ok(&hub, &alice, &["status"]);
    assert_eq!(tracked["upstream"]["repository_url"], REPOSITORY);
    assert_eq!(tracked["upstream"]["ref"], "main");
    assert_eq!(
        tracked["upstream"]["head_version_uuid"],
        tracked["head_version_uuid"]
    );
    assert_eq!(tracked["ahead"], 0);

    // Bob clones that ref and sees it as his current Branch with an upstream.
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    let status = ok(&hub, &bob, &["status"]);
    assert_eq!(status["label"], "main");
    assert_eq!(status["ahead"], 0);
    assert_eq!(status["upstream"]["repository_url"], REPOSITORY);
    assert_eq!(status["upstream"]["ref"], "main");
    let base = status["head_version_uuid"].as_str().unwrap().to_owned();

    // Bob commits twice, tags the second, and pushes with no arguments.
    let first = commit(&hub, &bob, BOB, "first", 1);
    let second = commit(&hub, &bob, BOB, "second", 2);
    assert_eq!(first["parents"][0], base);
    assert_eq!(second["parents"][0], first["version_uuid"]);
    assert_eq!(ok(&hub, &bob, &["status"])["ahead"], 2);
    ok(&hub, &bob, &["tag", "v1"]);
    assert_eq!(ok(&hub, &bob, &["tag"])[0]["name"], "v1");
    ok(&hub, &bob, &["push"]);
    // The push moved Bob's recorded upstream head, so nothing is left unpublished.
    let pushed = ok(&hub, &bob, &["status"]);
    assert_eq!(pushed["ahead"], 0);
    assert_eq!(
        pushed["upstream"]["head_version_uuid"],
        second["version_uuid"]
    );

    // Alice pulls Bob's two commits: her head had not moved, and the pull still
    // records a two-parent Version (there is no pointer fast-forward).
    let alice_head = ok(&hub, &alice, &["status"])["head_version_uuid"].clone();
    let pulled = ok(&hub, &alice, &["pull", REPOSITORY, "--author", ALICE]);
    assert_eq!(pulled["parents"][0], alice_head);
    assert_eq!(pulled["parents"][1], second["version_uuid"]);
    assert_eq!(score(&alice), 2);
    assert_eq!(ok(&hub, &alice, &["tag"])[0]["name"], "v1");
    let again = ok(&hub, &alice, &["pull", "--author", ALICE]);
    assert_eq!(again["up_to_date"], true);

    // Alice commits on top and pushes: a fast-forward of the Hub head.
    let alices = commit(&hub, &alice, ALICE, "alice's change", 3);
    ok(&hub, &alice, &["push"]);

    // Bob pulls her change: his head (the second commit) is not her ancestor.
    let merged = ok(&hub, &bob, &["pull", "--author", BOB]);
    assert_eq!(merged["parents"][0], second["version_uuid"]);
    assert_eq!(merged["parents"][1], alices["version_uuid"]);
    assert_eq!(score(&bob), 3);

    // His log lists both of his commits, Alice's, and the merge Versions.
    let log = ok(&hub, &bob, &["log", "--all"]);
    let entries = log.as_array().unwrap();
    let by_message = |message: &str| {
        entries
            .iter()
            .find(|entry| entry["description"] == message)
            .unwrap_or_else(|| panic!("no '{message}' in {log}"))
    };
    assert_eq!(entries[0]["version_uuid"], merged["version_uuid"]);
    assert_eq!(by_message("first")["author"]["name"], "Bob");
    assert_eq!(by_message("first")["committer"]["name"], "Bob");
    assert_eq!(by_message("alice's change")["author"]["name"], "Alice");
    assert_eq!(
        by_message("second")["parents"],
        serde_json::json!([first["version_uuid"]])
    );
    assert!(
        entries
            .iter()
            .any(|entry| entry["parents"].as_array().is_some_and(|p| p.len() == 2)),
        "a merge Version has two parents: {log}"
    );
    assert_eq!(
        names(&ok(&hub, &bob, &["log"]))[..3],
        [
            "Merge main of https://hub.test/curate/claims".to_owned(),
            "second".to_owned(),
            "first".to_owned()
        ],
        "the default log follows first parents"
    );

    // The Hub's published history agrees with the served lineage.
    let remote = ok(&hub, &bob, &["log", "--remote", "--all"]);
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    let walked: Vec<&str> = remote
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["version_uuid"].as_str().unwrap())
        .collect();
    let head = lineage.branches[0].head_version_uuid.as_str();
    assert_eq!(walked[0], head);
    assert_eq!(head, alices["version_uuid"].as_str().unwrap());
    for version in &walked {
        assert!(lineage.version(version).is_some());
    }

    // A tag names its Version for log, and a third clone materializes it.
    let tagged = ok(&hub, &bob, &["log", "v1", "-n", "1"]);
    assert_eq!(tagged[0]["version_uuid"], second["version_uuid"]);
    assert_eq!(tagged[0]["tags"][0], "v1");
    let third = root.path().join("third");
    ok(
        &hub,
        &third,
        &["clone", REPOSITORY, third.to_str().unwrap(), "--tag", "v1"],
    );
    let cloned = GraphForge::new(third.to_str()).unwrap();
    assert_eq!(
        sum(cloned
            .execute("MATCH (n:Item) RETURN sum(n.score)")
            .unwrap()),
        2
    );
}

#[test]
fn porcelain_walkthrough_preserves_replay_and_fast_forward() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );

    // A commit rerun with its operation UUID returns the original receipt.
    let operation = Uuid::now_v7().to_string();
    let arguments = [
        "commit",
        "-m",
        "once",
        "--query",
        "MATCH (n:Item) SET n.score = 1",
        "--author",
        BOB,
        "--operation-uuid",
        operation.as_str(),
    ];
    let original = ok(&hub, &bob, &arguments);
    let versions = |project: &Path| {
        GraphForge::new(project.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap()
            .versions
            .len()
    };
    let count = versions(&bob);
    let replay = ok(&hub, &bob, &arguments);
    assert_eq!(replay["version_uuid"], original["version_uuid"]);
    assert_eq!(replay["replayed"], true);
    assert_eq!(versions(&bob), count);
    // The same identity with another message is a conflict, not a replay.
    let mut other = arguments;
    other[2] = "different";
    gf(&hub, &bob, &other).unwrap_err();

    // Alice advances the Hub; Bob's push from a stale clone is refused exactly as
    // `gf publish` refuses it, and `--rewrite` replaces the head under the lease.
    commit(&hub, &alice, ALICE, "alice's change", 5);
    ok(&hub, &alice, &["push"]);
    let recorded = |project: &Path| {
        GraphForge::new(project.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap()
            .remote_branches
    };
    let before = recorded(&bob);
    let refused = gf(&hub, &bob, &["push"]).unwrap_err();
    assert_eq!(error_code(&refused), "hub.publish.non_fast_forward");
    assert_eq!(
        recorded(&bob),
        before,
        "a refused push leaves the upstream alone"
    );
    assert_eq!(ok(&hub, &bob, &["status"])["ahead"], 1);
    ok(&hub, &bob, &["push", "--rewrite"]);
    assert_eq!(ok(&hub, &bob, &["status"])["ahead"], 0);
    let (_, lineage) = served_lineage(&hub, "curate/claims");
    assert_eq!(
        lineage.branches[0].head_version_uuid,
        original["version_uuid"].as_str().unwrap()
    );
}

#[test]
fn pull_refuses_before_update_when_the_preview_has_incompatible_changes() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    // Both sides change the same field from the same base.
    commit(&hub, &alice, ALICE, "alice", 1);
    ok(&hub, &alice, &["push"]);
    commit(&hub, &bob, BOB, "bob", 2);
    let before = GraphForge::new(bob.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    let error = gf(&hub, &bob, &["pull", "--author", BOB])
        .unwrap_err()
        .to_string();
    assert!(error.contains("property:score"), "{error}");
    assert!(error.contains("gf research upstream update"), "{error}");
    let after = GraphForge::new(bob.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    // The fetch is durable (it retains the remote head) but nothing was incorporated.
    assert_eq!(after.versions.len(), before.versions.len() + 1);
    assert_eq!(after.heads, before.heads);
    assert_eq!(after.upstream.reviews.len(), before.upstream.reviews.len());
}

#[test]
fn pull_is_a_no_op_when_the_remote_head_is_an_ancestor() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    commit(&hub, &alice, ALICE, "ahead", 1);
    let before = GraphForge::new(alice.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    let pulled = ok(&hub, &alice, &["pull", REPOSITORY, "--author", ALICE]);
    assert_eq!(pulled["up_to_date"], true);
    let after = GraphForge::new(alice.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap();
    assert_eq!(after.versions.len(), before.versions.len());
    assert_eq!(after.receipts.len(), before.receipts.len());
}

#[test]
fn served_tags_are_installed_with_the_fetch_even_when_the_update_is_refused() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    // Alice tags a new commit and pushes; Bob changes the same field.
    let theirs = commit(&hub, &alice, ALICE, "alice", 1)["version_uuid"].clone();
    ok(&hub, &alice, &["tag", "a1"]);
    ok(&hub, &alice, &["push"]);
    commit(&hub, &bob, BOB, "bob", 2);
    assert!(ok(&hub, &bob, &["tag"]).as_array().unwrap().is_empty());

    // The update is refused between the durable fetch and the incorporation; the
    // tag came with the fetch, in the same publication as the admitted Version.
    gf(&hub, &bob, &["pull", "--author", BOB]).unwrap_err();
    let tags = ok(&hub, &bob, &["tag"]);
    assert_eq!(tags[0]["name"], "a1");
    assert_eq!(tags[0]["version_uuid"], theirs);
}

#[test]
fn pull_refuses_a_served_tag_that_conflicts_with_a_local_tag() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["tag", "v1"]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    commit(&hub, &bob, BOB, "bob", 1);
    ok(&hub, &bob, &["tag", "-d", "v1"]);
    ok(&hub, &bob, &["tag", "v1"]);
    let error = gf(&hub, &bob, &["pull", "--author", BOB])
        .unwrap_err()
        .to_string();
    assert!(error.contains("GF_RESEARCH_TAG_CONFLICT"), "{error}");
}

#[test]
fn publish_records_the_upstream_a_later_push_uses() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["publish", REPOSITORY, "--ref", "main"]);
    assert_eq!(ok(&hub, &alice, &["status"])["upstream"]["ref"], "main");
    commit(&hub, &alice, ALICE, "more", 1);
    assert_eq!(ok(&hub, &alice, &["status"])["ahead"], 1);
    ok(&hub, &alice, &["push"]);
    assert_eq!(ok(&hub, &alice, &["status"])["ahead"], 0);
}

fn assert_conflict(result: Result<Value, GfError>) {
    let error = result.expect_err("a different request under a used identity must be refused");
    assert_eq!(error_code(&error), "GF_IDEMPOTENCY_CONFLICT", "{error}");
}

#[test]
fn every_local_verb_replays_only_the_complete_original_request() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    let operation = || Uuid::now_v7().to_string();
    let versions = |project: &Path| {
        GraphForge::new(project.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap()
            .versions
            .len()
    };

    // branch: the first Branch, then the same request again.
    let o = operation();
    let branch = ["branch", "main", "--author", ALICE, "--operation-uuid", &o];
    let created = ok(&hub, &alice, &branch);
    let repeated = ok(&hub, &alice, &branch);
    assert_eq!(repeated["replayed"], true);
    assert_eq!(repeated["version_uuid"], created["version_uuid"]);
    assert_eq!(repeated["generation_uuid"], created["generation_uuid"]);
    assert_conflict(gf(
        &hub,
        &alice,
        &["branch", "other", "--author", ALICE, "--operation-uuid", &o],
    ));
    assert_conflict(gf(
        &hub,
        &alice,
        &["branch", "main", "--author", BOB, "--operation-uuid", &o],
    ));
    assert_conflict(gf(
        &hub,
        &alice,
        &[
            "branch",
            "main",
            "--from",
            "v0",
            "--author",
            ALICE,
            "--operation-uuid",
            &o,
        ],
    ));

    // commit: query, message and credit are all part of the request.
    let o = operation();
    let commit = |message: &str, query: &str, who: &str| {
        gf(
            &hub,
            &alice,
            &[
                "commit",
                "-m",
                message,
                "--query",
                query,
                "--author",
                who,
                "--operation-uuid",
                &o,
            ],
        )
    };
    let first = commit("one", "MATCH (n:Item) SET n.score = 1", ALICE).unwrap();
    let count = versions(&alice);
    let again = commit("one", "MATCH (n:Item) SET n.score = 1", ALICE).unwrap();
    assert_eq!(again["replayed"], true);
    assert_eq!(again["version_uuid"], first["version_uuid"]);
    assert_eq!(again["generation_uuid"], first["generation_uuid"]);
    assert_eq!(versions(&alice), count);
    assert_conflict(commit("one", "MATCH (n:Item) SET n.score = 2", ALICE));
    assert_conflict(commit("two", "MATCH (n:Item) SET n.score = 1", ALICE));
    assert_conflict(commit("one", "MATCH (n:Item) SET n.score = 1", BOB));
    assert_eq!(versions(&alice), count, "a refused repeat records nothing");
    // The same identity for another kind of operation is a conflict too.
    assert_conflict(gf(&hub, &alice, &["tag", "v1", "--operation-uuid", &o]));
    assert_conflict(gf(
        &hub,
        &alice,
        &["branch", "x", "--author", ALICE, "--operation-uuid", &o],
    ));

    // tag: name and target.
    let o = operation();
    let created = ok(&hub, &alice, &["tag", "v1", "--operation-uuid", &o]);
    let head = created["version_uuid"].as_str().unwrap().to_owned();
    // CURRENT has moved since; the rerun is still a replay of the same receipt.
    ok(
        &hub,
        &alice,
        &["tag", "other", "--operation-uuid", &operation()],
    );
    let again = ok(&hub, &alice, &["tag", "v1", &head, "--operation-uuid", &o]);
    assert_eq!(again["replayed"], true);
    assert_eq!(again["generation_uuid"], created["generation_uuid"]);
    assert_conflict(gf(&hub, &alice, &["tag", "v2", "--operation-uuid", &o]));
    assert_conflict(gf(
        &hub,
        &alice,
        &["tag", "-d", "v1", "--operation-uuid", &o],
    ));

    // switch: the target Branch.
    ok(
        &hub,
        &alice,
        &["branch", "feature", "--branch", "main", "--author", ALICE],
    );
    let o = operation();
    let switched = ok(&hub, &alice, &["switch", "feature", "--operation-uuid", &o]);
    assert_eq!(switched["label"], "feature");
    ok(&hub, &alice, &["switch", "feature", "--operation-uuid", &o]);
    assert_conflict(gf(
        &hub,
        &alice,
        &["switch", "main", "--operation-uuid", &o],
    ));
    assert_eq!(ok(&hub, &alice, &["status"])["label"], "feature");
    // Selecting the Branch that is already current still records the identity, so
    // the identity cannot later be reused for another selection.
    let o = operation();
    ok(&hub, &alice, &["switch", "feature", "--operation-uuid", &o]);
    let recorded = GraphForge::new(alice.to_str())
        .unwrap()
        .research_version_retention()
        .unwrap()
        .receipts
        .contains_key(&Uuid::parse_str(&o).unwrap());
    assert!(
        recorded,
        "a no-op selection under an identity leaves its receipt"
    );
    assert_conflict(gf(
        &hub,
        &alice,
        &["switch", "main", "--operation-uuid", &o],
    ));
    assert_eq!(ok(&hub, &alice, &["status"])["label"], "feature");
}

#[test]
fn pull_replays_a_completed_receipt_without_the_network_and_refuses_another_request() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    commit(&hub, &alice, ALICE, "alice", 3);
    ok(&hub, &alice, &["push"]);

    let o = Uuid::now_v7().to_string();
    let pull = ["pull", "--author", BOB, "--operation-uuid", o.as_str()];
    let merged = ok(&hub, &bob, &pull);
    assert_eq!(merged["parents"].as_array().unwrap().len(), 2);
    // A Hub that has nothing at all: a replay must not need the network.
    let nowhere = LoopbackHub::start();
    let replay = ok(&nowhere, &bob, &pull);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["version_uuid"], merged["version_uuid"]);
    assert_eq!(replay["generation_uuid"], merged["generation_uuid"]);
    assert_eq!(replay["parents"], merged["parents"]);
    assert_eq!(replay["incorporated"], merged["incorporated"]);
    assert_eq!(nowhere.log().len(), 0, "no request reached the Hub");
    // Another request under the identity is refused, offline as well.
    assert_conflict(gf(
        &nowhere,
        &bob,
        &["pull", "--author", ALICE, "--operation-uuid", &o],
    ));
    assert_conflict(gf(
        &nowhere,
        &bob,
        &[
            "pull",
            "https://hub.test/curate/other",
            "--author",
            BOB,
            "--operation-uuid",
            &o,
        ],
    ));
}

#[test]
fn push_replays_a_completed_publication_and_refuses_another_request() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    let o = Uuid::now_v7().to_string();
    let push = ["push", REPOSITORY, "--operation-uuid", o.as_str()];
    let first = ok(&hub, &alice, &push);
    // Recording the confirmed publication moved CURRENT; the rerun is a replay.
    let writes = || {
        hub.log()
            .iter()
            .filter(|entry| matches!(entry.method.as_str(), "POST" | "PUT"))
            .count()
    };
    let before = writes();
    let receipts = |project: &Path| {
        GraphForge::new(project.to_str())
            .unwrap()
            .research_version_retention()
            .unwrap()
            .receipts
            .len()
    };
    let recorded = receipts(&alice);
    let again = ok(&hub, &alice, &["push", "--operation-uuid", &o]);
    assert_eq!(again, first);
    assert_eq!(writes(), before, "a replay uploads and commits nothing");
    // Replays are free: the permanent receipt ledger does not grow with them.
    for _ in 0..5 {
        assert_eq!(ok(&hub, &alice, &["push", "--operation-uuid", &o]), first);
    }
    assert_eq!(receipts(&alice), recorded);
    // The same identity for another ref or another flag is refused.
    ok(
        &hub,
        &alice,
        &["branch", "feature", "--branch", "main", "--author", ALICE],
    );
    assert_conflict(gf(
        &hub,
        &alice,
        &[
            "push",
            REPOSITORY,
            "--ref",
            "feature",
            "--operation-uuid",
            &o,
        ],
    ));
    assert_conflict(gf(
        &hub,
        &alice,
        &["push", "--ref", "main", "--rewrite", "--operation-uuid", &o],
    ));
    assert_eq!(writes(), before);
}

#[test]
fn publication_records_only_what_the_receipt_confirmed() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    let receipt: graphforge_hub_publish::PublishReceipt =
        serde_json::from_value(ok(&hub, &alice, &["push", REPOSITORY])).unwrap();
    let published = ok(&hub, &alice, &["status"])["head_version_uuid"].clone();
    // An unpublished descendant: the Branch head moves past what the Hub holds.
    let unpublished = commit(&hub, &alice, ALICE, "not pushed", 9)["version_uuid"].clone();
    let (manifest, lineage) = served_lineage(&hub, "curate/claims");
    let mut graph = GraphForge::new(alice.to_str()).unwrap();
    let branch = *graph
        .research_version_retention()
        .unwrap()
        .branches
        .keys()
        .next()
        .unwrap();
    let mut record = |receipt: graphforge_hub_publish::PublishReceipt,
                      manifest: DiscoveryManifest,
                      lineage: ResearchLineage| {
        let request = graphforge_api::RecordResearchPublicationRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: generation(&graph),
            branch_uuid: branch,
            repository_url: REPOSITORY.into(),
            receipt,
            manifest,
            lineage,
        };
        graph.record_research_publication(&request, &CancellationToken::new())
    };

    // What the receipt confirms is the published head, not the Branch head, and it
    // is already recorded: recording again writes nothing.
    assert!(
        record(receipt.clone(), manifest.clone(), lineage.clone())
            .unwrap()
            .is_none()
    );
    drop(record);
    drop(graph);
    let upstream = ok(&hub, &alice, &["status"]);
    assert_eq!(upstream["upstream"]["head_version_uuid"], published);
    assert_ne!(upstream["upstream"]["head_version_uuid"], unpublished);

    let mut graph = GraphForge::new(alice.to_str()).unwrap();
    let mut record = |receipt: graphforge_hub_publish::PublishReceipt,
                      manifest: DiscoveryManifest,
                      lineage: ResearchLineage| {
        let request = graphforge_api::RecordResearchPublicationRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: generation(&graph),
            branch_uuid: branch,
            repository_url: REPOSITORY.into(),
            receipt,
            manifest,
            lineage,
        };
        graph.record_research_publication(&request, &CancellationToken::new())
    };
    // A lineage that claims the unpublished head is not the one the receipt commits to.
    let mut forged = lineage.clone();
    forged.branches[0].head_version_uuid = unpublished.as_str().unwrap().to_owned();
    record(receipt.clone(), manifest.clone(), forged).unwrap_err();
    // Nor is a receipt for other documents, another repository, or other refs.
    let mut other_documents = receipt.clone();
    other_documents.manifest_validator = format!("sha256:{}", "0".repeat(64));
    record(other_documents, manifest.clone(), lineage.clone()).unwrap_err();
    let mut elsewhere = receipt.clone();
    elsewhere.repository =
        graphforge_discovery::RepositoryIdentity::parse("curate/elsewhere").unwrap();
    record(elsewhere, manifest.clone(), lineage.clone()).unwrap_err();
    let mut other_refs = receipt;
    other_refs.refs = vec!["feature".into()];
    record(other_refs, manifest, lineage).unwrap_err();
}

#[test]
fn an_interrupted_publication_is_recorded_when_it_is_replayed() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    // The Hub committed the publication, but the process died before recording it.
    let graph = GraphForge::new(alice.to_str()).unwrap();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    drop(graph);
    assert!(
        ok(&hub, &alice, &["status"])
            .get("upstream")
            .is_none_or(Value::is_null)
    );
    // Rerunning the command replays the committed operation and records it.
    ok(&hub, &alice, &["push", REPOSITORY]);
    let status = ok(&hub, &alice, &["status"]);
    assert_eq!(status["upstream"]["ref"], "main");
    assert_eq!(status["ahead"], 0);
}

#[test]
fn a_clone_selects_its_branch_for_the_verbs() {
    let hub = LoopbackHub::start();
    let root = tempfile::tempdir().unwrap();
    let alice = seeded(root.path(), "alice");
    ok(&hub, &alice, &["branch", "main", "--author", ALICE]);
    ok(&hub, &alice, &["push", REPOSITORY]);
    let bob = root.path().join("bob");
    ok(
        &hub,
        &bob,
        &["clone", REPOSITORY, bob.to_str().unwrap(), "--ref", "main"],
    );
    // A second Branch does not unselect the cloned one.
    ok(&hub, &bob, &["branch", "feature", "--author", BOB]);
    assert_eq!(ok(&hub, &bob, &["status"])["label"], "main");
    let committed = commit(&hub, &bob, BOB, "on main", 4);
    assert_eq!(committed["parents"].as_array().unwrap().len(), 1);
    assert_eq!(
        ok(&hub, &bob, &["log", "-n", "1"])[0]["description"],
        "on main"
    );
}

#[test]
fn porcelain_walkthrough_uses_no_handwritten_json() {
    // No argument list in this file passes a native request file.
    let source = include_str!("porcelain.rs");
    let flag = ["\"--", "file\""].concat();
    assert_eq!(
        source.matches(&flag).count(),
        0,
        "the walkthrough must not hand a request file to any verb"
    );
}
