import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { CheckpointView, GraphForge, GraphImportSession, PlanHandle } from "../index.js";

const here = dirname(fileURLToPath(import.meta.url));
const policy = JSON.parse(
  readFileSync(join(here, "non-cypher-parity-policy.json"), "utf8"),
);
const rustManifest = JSON.parse(
  readFileSync(join(here, policy.rustManifest), "utf8"),
);

const camelCase = (value) =>
  value.replace(/_([a-z])/g, (_match, letter) => letter.toUpperCase());

function releaseSurface() {
  return Object.values(rustManifest.method_evidence_groups)
    .flatMap(({ ids }) => ids)
    .sort();
}

test("the Node classification is total, frozen, and backed by non-skipped native tests", () => {
  const release = releaseSurface();
  assert.equal(
    new Set(release).size,
    release.length,
    "duplicate Rust method ID",
  );
  assert.equal(release.length, policy.releaseSurfaceCount);
  assert.equal(
    createHash("sha256")
      .update(`${release.join("\n")}\n`)
      .digest("hex"),
    policy.releaseSurfaceDigest,
    "Rust release surface changed without an explicit Node parity decision",
  );
  assert.deepEqual(
    new Set(Object.values(policy.rustEvidenceGroupMap)),
    new Set(Object.keys(policy.evidence)),
    "every Rust evidence group must map to one exact Node evidence group",
  );

  const equivalent = new Set(policy.classification.equivalent);
  const languageSpecific = new Set(
    Object.keys(policy.classification.languageSpecific),
  );
  const receivers = { CheckpointView, GraphForge, GraphImportSession, PlanHandle };
  for (const id of equivalent) {
    assert.ok(release.includes(id), `stale equivalent classification: ${id}`);
    assert.equal(
      languageSpecific.has(id),
      false,
      `duplicate classification: ${id}`,
    );
  }
  for (const id of languageSpecific) {
    assert.ok(
      release.includes(id),
      `stale language-specific classification: ${id}`,
    );
  }

  const counts = { equivalent: 0, languageSpecific: 0, notExposed: 0 };
  for (const id of release) {
    const [receiver, method] = id.split(".");
    if (equivalent.has(id)) {
      counts.equivalent += 1;
      const prototype = receivers[receiver]?.prototype;
      if (prototype) {
        const descriptor = Object.getOwnPropertyDescriptor(
          prototype,
          camelCase(method),
        );
        assert.ok(descriptor, `missing Node member for ${id}`);
        assert.ok(
          typeof descriptor.value === "function" ||
            typeof descriptor.get === "function",
          `Node member for ${id} is neither a native method nor getter`,
        );
      }
      continue;
    }
    if (languageSpecific.has(id)) {
      counts.languageSpecific += 1;
      continue;
    }
    assert.ok(
      policy.classification.notExposedDefaults[receiver],
      `unclassified Rust release entry: ${id}`,
    );
    counts.notExposed += 1;
  }
  assert.deepEqual(counts, {
    equivalent: policy.classification.equivalent.length,
    languageSpecific: Object.keys(policy.classification.languageSpecific)
      .length,
    notExposed:
      policy.releaseSurfaceCount -
      policy.classification.equivalent.length -
      Object.keys(policy.classification.languageSpecific).length,
  });

  for (const [id, adapter] of Object.entries(
    policy.classification.languageSpecific,
  )) {
    assert.ok(adapter.reason, `missing Node adapter rationale: ${id}`);
    assert.ok(
      adapter.nodeMembers.length > 0,
      `missing Node adapter member: ${id}`,
    );
    for (const member of adapter.nodeMembers) {
      const [receiver, method] = member.split(".");
      assert.ok(
        receivers[receiver],
        `unknown Node adapter receiver: ${member}`,
      );
      assert.ok(
        Object.getOwnPropertyDescriptor(receivers[receiver].prototype, method),
        `missing Node adapter member: ${member}`,
      );
    }
  }

  for (const [receiver, constructor] of Object.entries({
    CheckpointView,
    GraphForge,
    GraphImportSession,
    PlanHandle,
  })) {
    const projected = new Set(
      policy.classification.equivalent
        .filter((id) => id.startsWith(`${receiver}.`))
        .map((id) => camelCase(id.split(".")[1])),
    );
    const nodeOnly = new Set(
      Object.keys(policy.classification.nodeOnly).filter((id) =>
        id.startsWith(`${receiver}.`),
      ),
    );
    for (const method of Object.getOwnPropertyNames(constructor.prototype)) {
      if (method === "constructor" || projected.has(method)) continue;
      assert.ok(
        nodeOnly.has(`${receiver}.${method}`),
        `unclassified shipped Node member: ${receiver}.${method}`,
      );
    }
  }

  for (const [group, files] of Object.entries(policy.evidence)) {
    assert.ok(
      Object.keys(files).length > 0,
      `${group} has no exact Node evidence`,
    );
    for (const [file, titles] of Object.entries(files)) {
      const path = join(here, file);
      assert.ok(existsSync(path), `missing Node parity evidence: ${file}`);
      const source = readFileSync(path, "utf8");
      assert.ok(titles.length > 0, `${group}/${file} has no exact test title`);
      for (const title of titles) {
        assert.ok(
          source.includes(`test("${title}"`),
          `stale Node evidence ${file}: ${title}`,
        );
      }
      assert.doesNotMatch(
        source,
        /\b(?:test|describe)\.skip\s*\(/,
        `${file} is skipped`,
      );
      assert.match(
        source,
        /\.\.\/index\.js/,
        `${file} does not load the shipped addon`,
      );
    }
  }

  assert.match(
    Function.prototype.toString.call(GraphForge.prototype.rank),
    /\[native code\]/,
    "rank must come from the native addon, not a JavaScript fallback",
  );
  assert.match(
    Function.prototype.toString.call(GraphForge.prototype.find),
    /\[native code\]/,
    "find must come from the native addon, not a JavaScript fallback",
  );
});

test("the native Node facade preserves deterministic IPC through persistence and reopen", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-release-parity-"));
  try {
    const forge = new GraphForge(project);
    forge.execute(
      "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
        "(a)-[:KNOWS]->(b)",
    );
    forge.index("Person", { properties: ["name"] });

    const rank = tableFromIPC(forge.rank("Person", "degree", "KNOWS"));
    assert.deepEqual(
      rank.schema.fields.map(({ name }) => name),
      ["node_uuid", "score", "name"],
    );
    assert.deepEqual([...rank.getChild("name").toArray()], ["Alice", "Bob"]);
    assert.deepEqual([...rank.getChild("score").toArray()], [1, 0]);

    const find = tableFromIPC(forge.find("alice", "Person"));
    assert.deepEqual(
      find.schema.fields.map(({ name }) => name),
      ["node_uuid", "name", "score", "matched_on"],
    );
    assert.deepEqual([...find.getChild("name").toArray()], ["Alice"]);
    assert.deepEqual([...find.getChild("matched_on").toArray()], ["text"]);
    assert.deepEqual(forge.labels(), ["Person"]);
    assert.deepEqual(forge.relationshipTypes(), ["KNOWS"]);
    assert.equal(forge.nodeCount(), 2);
    const inspection = tableFromIPC(forge.schema());
    assert.deepEqual(
      inspection.schema.fields.map(({ name }) => name),
      ["label", "node_count", "rel_type", "rel_count"],
    );
    forge.close();

    const reopened = new GraphForge(project);
    assert.deepEqual(
      tableFromIPC(reopened.rank("Person", "degree", "KNOWS")).toArray(),
      rank.toArray(),
    );
    assert.deepEqual(
      tableFromIPC(reopened.find("alice", "Person")).toArray(),
      find.toArray(),
    );
    assert.deepEqual(reopened.labels(), ["Person"]);
    assert.deepEqual(reopened.relationshipTypes(), ["KNOWS"]);
    assert.equal(reopened.nodeCount(), 2);
    assert.deepEqual(
      tableFromIPC(reopened.schema()).toArray(),
      inspection.toArray(),
    );
    reopened.close();
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});

test("a future project format fails structurally without mutating the project", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-future-format-"));
  try {
    const marker = join(project, "FORMAT");
    writeFileSync(marker, "graphforge-project/v2\n");
    assert.throws(
      () => new GraphForge(project),
      (error) => error.code === "GF_UNSUPPORTED_PROJECT_FORMAT",
    );
    assert.equal(readFileSync(marker, "utf8"), "graphforge-project/v2\n");
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
