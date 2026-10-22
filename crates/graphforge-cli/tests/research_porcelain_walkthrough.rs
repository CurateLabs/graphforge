//! The built `gf` binary driving the research porcelain that needs no Hub:
//! identity, Branches, commits, tags and history, with no hand-written request.
//! The Hub-facing verbs (`clone`, `push`, `pull`, `log --remote`) run against the
//! reference Hub in `hub_publish::tests::porcelain`, because the product
//! transport only speaks HTTPS to public addresses.
use graphforge_api::GraphForge;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Gf {
    project: PathBuf,
    config: PathBuf,
}

impl Gf {
    fn new(root: &Path) -> Self {
        let project = root.join("project");
        let graph = GraphForge::new(project.to_str()).unwrap();
        graph.execute("CREATE (:Item {score:0})").unwrap();
        Self {
            project,
            config: root.join("config"),
        }
    }

    fn command(&self, arguments: &[&str]) -> Command {
        self.formatted_command(arguments, true)
    }

    fn formatted_command(&self, arguments: &[&str], json: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gf"));
        command
            .arg("--project")
            .arg(&self.project)
            .args(arguments)
            .env("XDG_CONFIG_HOME", &self.config)
            .env("APPDATA", &self.config)
            .env_remove("GF_AUTHOR")
            .env_remove("GF_COMMITTER");
        if json {
            command.arg("--json");
        }
        command
    }

    fn raw(&self, arguments: &[&str]) -> Output {
        self.command(arguments).output().unwrap()
    }

    fn json(&self, arguments: &[&str]) -> Value {
        let output = self.raw(arguments);
        assert!(
            output.status.success(),
            "gf {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap_or(Value::Null)
    }

    fn refusal(&self, arguments: &[&str]) -> String {
        let output = self.raw(arguments);
        assert!(!output.status.success(), "gf {arguments:?} should fail");
        String::from_utf8_lossy(&output.stderr).into_owned()
    }
}

#[test]
fn local_porcelain_verbs_run_through_the_built_binary() {
    let root = tempfile::tempdir().unwrap();
    let gf = Gf::new(root.path());

    // Nothing credits a Version until an identity is configured.
    let missing = gf.refusal(&["branch", "main"]);
    assert!(
        missing.contains("GF_RESEARCH_SIGNATURE_MISSING"),
        "{missing}"
    );
    gf.json(&[
        "config",
        "user",
        "--name",
        "Ada Lovelace",
        "--email",
        "ada@example.org",
    ]);
    let who = gf.json(&["config", "user"]);
    assert_eq!(who["author"]["name"], "Ada Lovelace");
    assert_eq!(who["committer"], who["author"]);
    assert!(gf.config.join("graphforge").join("config.toml").is_file());

    // A Branch label is a ref.
    let bad = gf.refusal(&["branch", "not a ref"]);
    assert!(bad.contains("GF_RESEARCH_BRANCH_LABEL_INVALID"), "{bad}");
    let main = gf.json(&["branch", "main"]);
    gf.json(&["branch", "feature/score"]);
    let duplicate = gf.refusal(&["branch", "main", "--branch", "feature/score"]);
    assert!(
        duplicate.contains("GF_RESEARCH_BRANCH_LABEL_TAKEN"),
        "{duplicate}"
    );

    // Two Branches and no selection: the verbs say how to select one.
    let unselected = gf.refusal(&["status"]);
    assert!(
        unselected.contains("GF_RESEARCH_BRANCH_UNSELECTED"),
        "{unselected}"
    );
    assert!(unselected.contains("gf switch"), "{unselected}");
    // An explicit Version needs no selected Branch, including a tag on it.
    let head = main["version_uuid"].as_str().unwrap();
    gf.json(&["tag", "base", head]);
    for version in [head, "base"] {
        assert_eq!(
            gf.json(&["log", version, "-n", "1"])[0]["version_uuid"],
            head
        );
    }
    gf.json(&["tag", "-d", "base"]);
    gf.json(&["switch", "feature/score"]);
    let status = gf.json(&["status"]);
    assert_eq!(status["label"], "feature/score");
    assert_eq!(status["ahead"], 0);
    let branches = gf.json(&["branch"]);
    let current: Vec<&Value> = branches
        .as_array()
        .unwrap()
        .iter()
        .filter(|branch| branch["current"] == true)
        .collect();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0]["label"], "feature/score");

    // Two commits, credited from the configuration; a tag names the second.
    let first = gf.json(&[
        "commit",
        "-m",
        "Raise the score\n\nThe source changed.",
        "--query",
        "MATCH (n:Item) SET n.score = 1",
    ]);
    // An environment identity overrides the file for one invocation.
    let mut second_command = gf.command(&[
        "commit",
        "-m",
        "Raise it again",
        "--query",
        "MATCH (n:Item) SET n.score = 2",
    ]);
    second_command.env("GF_AUTHOR", "Grace Hopper <grace@example.org>");
    let second_output = second_command.output().unwrap();
    assert!(second_output.status.success());
    let second: Value = serde_json::from_slice(&second_output.stdout).unwrap();
    assert_eq!(second["parents"][0], first["version_uuid"]);
    gf.json(&["tag", "v1"]);
    assert_eq!(gf.json(&["tag"])[0]["version_uuid"], second["version_uuid"]);

    let log = gf.json(&["log", "-n", "2"]);
    let entries = log.as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["version_uuid"], second["version_uuid"]);
    assert_eq!(entries[0]["author"]["name"], "Grace Hopper");
    assert_eq!(entries[0]["committer"]["name"], "Grace Hopper");
    assert_eq!(entries[0]["tags"][0], "v1");
    assert_eq!(
        entries[1]["description"],
        "Raise the score\n\nThe source changed."
    );
    assert_eq!(entries[1]["author"]["name"], "Ada Lovelace");
    let from_tag = gf.json(&["log", "v1", "-n", "1"]);
    assert_eq!(from_tag[0]["version_uuid"], second["version_uuid"]);

    // A tag never moves.
    let moved = gf.refusal(&[
        "tag",
        "v1",
        &first["version_uuid"].as_str().unwrap().to_owned(),
    ]);
    assert!(moved.contains("GF_RESEARCH_TAG_EXISTS"), "{moved}");
    gf.json(&["tag", "-d", "v1"]);
    assert_eq!(gf.json(&["tag"]), Value::Array(vec![]));

    // The human output is one record per line.
    let output = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&gf.project)
        .args(["log"])
        .env("XDG_CONFIG_HOME", &gf.config)
        .env("APPDATA", &gf.config)
        .output()
        .unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(
        text.lines().next().unwrap().contains("Raise it again"),
        "{text}"
    );
}

#[test]
fn every_porcelain_verb_documents_an_example() {
    let config = tempfile::tempdir().unwrap();
    for verb in [
        "status", "log", "commit", "branch", "switch", "tag", "push", "pull",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_gf"))
            .args([verb, "--help"])
            .env("XDG_CONFIG_HOME", config.path())
            .env("APPDATA", config.path())
            .output()
            .unwrap();
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(
            help.contains("Example"),
            "gf {verb} --help has no example:\n{help}"
        );
    }
}

#[test]
fn config_user_changes_only_the_edited_keys() {
    let root = tempfile::tempdir().unwrap();
    let gf = Gf::new(root.path());
    let directory = gf.config.join("graphforge");
    std::fs::create_dir_all(&directory).unwrap();
    let before = "# Credit for my Versions.\n\n[user]\nname = \"Ada \\\"The Countess\\\" # not a comment\" # my name\nemail = \"ada@example.org\" # old address\n\n# Other tools read this table.\n[editor]\ncommand = \"vi # -p\"   # keep\nargs = [\"-c\", \"set nu\"]\n";
    std::fs::write(directory.join("config.toml"), before).unwrap();

    let shown = gf.json(&["config", "user", "--email", "countess@example.org"]);
    assert_eq!(
        shown["author"]["name"],
        "Ada \"The Countess\" # not a comment"
    );
    assert_eq!(shown["author"]["email"], "countess@example.org");
    let after = std::fs::read_to_string(directory.join("config.toml")).unwrap();
    assert_eq!(
        after,
        before.replace("ada@example.org", "countess@example.org")
    );

    // A file that is not TOML is refused, never overwritten.
    std::fs::write(directory.join("config.toml"), "[user\nname = ").unwrap();
    let refused = gf.refusal(&["config", "user", "--name", "Ada"]);
    assert!(refused.contains("config.toml"), "{refused}");
    assert_eq!(
        std::fs::read_to_string(directory.join("config.toml")).unwrap(),
        "[user\nname = "
    );
}

#[test]
fn human_mutations_print_retry_identity_before_success_or_refusal() {
    let root = tempfile::tempdir().unwrap();
    let gf = Gf::new(root.path());
    gf.json(&[
        "config",
        "user",
        "--name",
        "Ada",
        "--email",
        "ada@example.org",
    ]);
    let run = |arguments: &[&str], success: bool| {
        let output = gf.formatted_command(arguments, false).output().unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let identity = text
            .lines()
            .next()
            .unwrap()
            .strip_prefix("Operation UUID: ")
            .expect(&text);
        assert_eq!(
            uuid::Uuid::parse_str(identity).unwrap().get_version_num(),
            7
        );
        identity.to_owned()
    };
    run(&["branch", "main"], true);
    run(&["switch", "main"], true);
    let commit = [
        "commit",
        "-m",
        "one",
        "--query",
        "MATCH (n:Item) SET n.score = 1",
    ];
    let operation = run(&commit, true);
    let mut retry = commit.to_vec();
    retry.extend(["--operation-uuid", &operation]);
    assert_eq!(run(&retry, true), operation);
    run(&["tag", "v1"], true);
    run(&["tag", "-d", "v1"], true);
    run(&["commit", "-m", "bad", "--query", "NOT CYPHER"], false);
    run(&["branch", "bad name"], false);
    run(&["tag", "bad name"], false);
    // Read-only listings never claim an operation was started.
    let output = gf.formatted_command(&["tag"], false).output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"\n");
}
