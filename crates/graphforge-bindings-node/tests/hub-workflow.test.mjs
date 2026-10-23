// Real native Hub publish/1 + discovery/1 via ReferenceHub transport injection.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import {
  GraphForge,
  _testHubStart,
  _testHubStop,
  runCli,
  version,
} from "../index.js";
import { HubProject, HubWorkflowError } from "../lib/hub.mjs";

const REPOSITORY = "https://hub.test/curate/claims";
const AUTHOR = "Alice <alice@example.org>";
let sequence = 0;
const operation = () =>
  `018f0f4e-7b8c-7000-8000-${(++sequence).toString(16).padStart(12, "0")}`;

function fixture(t) {
  process.env.GF_WORKFLOW_TEST_TOKEN = _testHubStart();
  const root = mkdtempSync(join(tmpdir(), "gf-hub-workflow-"));
  t.after(() => {
    _testHubStop();
    delete process.env.GF_WORKFLOW_TEST_TOKEN;
    rmSync(root, { recursive: true, force: true });
  });
  return (name) =>
    new HubProject(join(root, name), { tokenEnv: "GF_WORKFLOW_TEST_TOKEN" });
}
function seed(project) {
  const source = project("source");
  const graph = new GraphForge(source.path);
  try {
    graph.execute("CREATE (:Item {score:0})");
  } finally {
    graph.close();
  }
  source.branch("main", { author: AUTHOR });
  const receipt = source.publish(REPOSITORY);
  assert.deepEqual(receipt.repository, {
    owner: "curate",
    repository: "claims",
  });
  assert.deepEqual(receipt.refs, ["main"]);
  return source;
}

test("typed Hub verbs clone, commit twice, tag, push, pull, log, and reopen Arrow data", async (t) => {
  const project = fixture(t);
  seed(project);
  const alice = project("alice");
  const cloned = alice.clone(REPOSITORY, { ref: "main" });
  assert.equal(cloned.contract, "graphforge-hub-clone/1");
  assert.equal(cloned.destination, alice.path);
  assert.equal(cloned.resolved_ref, "main");
  assert.equal(
    cloned.research_version_uuid,
    alice.log(undefined, { remote: true })[0].version_uuid,
  );
  const operationUuid = operation();
  const first = alice.commit("First score", "MATCH (n:Item) SET n.score=1", {
    author: AUTHOR,
    committer: AUTHOR,
    operationUuid,
  });
  const replay = alice.commit("First score", "MATCH (n:Item) SET n.score=1", {
    author: AUTHOR,
    committer: AUTHOR,
    operationUuid,
  });
  assert.equal(replay.version_uuid, first.version_uuid);
  assert.equal(replay.generation_uuid, first.generation_uuid);
  assert.equal(replay.replayed, true);
  const second = alice.commit("Second score", "MATCH (n:Item) SET n.score=2", {
    author: "Alice",
    committer: "Alice",
  });
  assert.deepEqual(second.parents, [first.version_uuid]);
  assert.equal(alice.tag("v1").version_uuid, second.version_uuid);
  const publishId = operation();
  const pushed = alice.push(undefined, { operationUuid: publishId });
  assert.deepEqual(alice.push(undefined, { operationUuid: publishId }), pushed);
  assert.equal(pushed.operation_uuid, publishId);
  assert.equal(alice.log("v1")[0].version_uuid, second.version_uuid);
  const bob = project("bob");
  bob.clone(REPOSITORY);
  const collaborator = bob.commit(
    "Collaborator score",
    "MATCH (n:Item) SET n.score=3",
    { author: "Bob <bob@example.org>" },
  );
  bob.push();
  const pullId = operation();
  const pulled = alice.pull(undefined, {
    author: AUTHOR,
    operationUuid: pullId,
  });
  assert.ok(pulled.parents.includes(collaborator.version_uuid));
  assert.deepEqual(pulled.conflicts, []);
  assert.equal(typeof pulled.incorporated, "number");
  assert.equal(
    alice.pull(undefined, { author: AUTHOR, operationUuid: pullId }).replayed,
    true,
  );
  assert.equal(alice.pull(undefined, { author: AUTHOR }).up_to_date, true);
  const history = alice.log(undefined, { all: true });
  for (const message of ["First score", "Second score", "Collaborator score"]) {
    assert.ok(history.some((entry) => entry.description === message));
  }
  assert.equal(history[0].version_uuid, pulled.version_uuid);
  const localFirst = history.find(
    (entry) => entry.version_uuid === first.version_uuid,
  );
  assert.equal(localFirst.author.name, "Alice");
  assert.equal(localFirst.committer.email, "alice@example.org");
  const localSecond = history.find(
    (entry) => entry.version_uuid === second.version_uuid,
  );
  assert.deepEqual(localSecond.author, { name: "Alice" });
  assert.deepEqual(localSecond.committer, { name: "Alice" });
  const remote = alice.log(undefined, { remote: true, all: true, maxCount: 2 });
  assert.equal(remote[0].version_uuid, collaborator.version_uuid);
  assert.equal(remote.length, 2);
  const reopened = new GraphForge(alice.path);
  try {
    const current = reopened.researchVersionRetention().current_branch;
    const table = tableFromIPC(
      await reopened.queryResearchBranch(
        current,
        "MATCH (n:Item) RETURN n.score AS score",
      ),
    );
    assert.equal(table.getChild("score").get(0), 3n);
    const old = tableFromIPC(
      reopened.queryResearchVersion(
        first.version_uuid,
        "MATCH (n:Item) RETURN n.score AS score",
      ),
    );
    assert.equal(old.getChild("score").get(0), 1n);
  } finally {
    reopened.close();
  }
  assert.equal(
    project("tagged").clone(REPOSITORY, { tag: "v1" }).research_version_uuid,
    second.version_uuid,
  );
});

test("HubWorkflowError retains the native semantic code, receipt refusal and envelope", (t) => {
  const project = fixture(t);
  const source = seed(project);
  const operationUuid = operation();
  source.commit("one", "MATCH (n:Item) SET n.score=1", {
    author: AUTHOR,
    operationUuid,
  });
  assert.throws(
    () =>
      source.commit("different", "MATCH (n:Item) SET n.score=2", {
        author: AUTHOR,
        operationUuid,
      }),
    (error) => {
      assert.ok(error instanceof HubWorkflowError);
      assert.equal(error.nativeCode, "GF_IDEMPOTENCY_CONFLICT");
      assert.deepEqual(
        error.envelope,
        JSON.parse(error.stderr.toString("utf8")),
      );
      assert.equal(error.code, error.details.semantic_code ?? error.nativeCode);
      const native = runCli([
        "--json",
        `--project=${source.path}`,
        "commit",
        "--message=different",
        "--query=MATCH (n:Item) SET n.score=2",
        `--author=${AUTHOR}`,
        `--operation-uuid=${operationUuid}`,
      ]);
      assert.equal(error.exitCode, native.exitCode);
      assert.deepEqual(error.stdout, native.stdout);
      assert.deepEqual(error.stderr, native.stderr);
      return true;
    },
  );
  for (const [repository, code] of [
    ["http://127.0.0.1/private", "hub.unsafe_location"],
    ["https://hub.test/curate/missing", "hub.not_found"],
  ]) {
    assert.throws(
      () => project("refused").clone(repository),
      (error) => error instanceof HubWorkflowError && error.code === code,
    );
  }
});

test("native version warnings preserve the typed history array", (t) => {
  const project = fixture(t);
  const source = seed(project);
  // Relabel the real writer's generation and recompute its CURRENT commitment.
  const currentPath = join(source.path, "CURRENT");
  const currentText = readFileSync(currentPath, "utf8");
  const current = JSON.parse(currentText);
  const manifestPath = join(
    source.path,
    "generations",
    current.generation_uuid,
    "manifest.json",
  );
  const manifest = readFileSync(manifestPath, "utf8");
  const stamp = `,"producing_core_version":"${version()}"}`;
  assert.ok(manifest.trimEnd().endsWith(stamp));
  // Preserve every other byte, including integer identities beyond JS precision.
  const relabelled = manifest.replace(
    stamp,
    ',"producing_core_version":"0.0.1"}',
  );
  writeFileSync(manifestPath, relabelled);
  const digest = createHash("sha256").update(relabelled).digest("hex");
  writeFileSync(
    currentPath,
    currentText.replace(current.generation_manifest_sha256, digest),
  );
  const history = source.log();
  assert.ok(Array.isArray(history));
  assert.ok(history[0].version_uuid);
  assert.equal(source.warnings[0].code, "GF_CORE_VERSION_MISMATCH");
  assert.equal(source.warnings[0].producing_core_version, "0.0.1");
});
