// Fresh-addon acceptance for embedding-space inspection and alias controls.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GraphForge } from "../index.js";

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

function assertContentFree(space) {
  assert.deepEqual(Object.keys(space).sort(), [
    "active",
    "aliases",
    "chunking",
    "compatibilityId",
    "defaultAlias",
    "dimensions",
    "producer",
    "tokenizer",
  ]);
  for (const forbidden of [
    "embedding",
    "sourceText",
    "credentials",
    "confidence",
    "provenanceId",
    "assertionUuid",
    "beliefStatus",
    "validTime",
  ]) {
    assert.equal(Object.hasOwn(space, forbidden), false);
  }
}

test("embedding spaces inspect, alias, default, and reopen deterministically", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-embedding-spaces-"));
  const forge = new GraphForge(project);
  try {
    const node = forge.addNode("Person", { name: "Alice" });
    const alpha = forge.publishCallerEmbeddings("alpha", {
      rows: [{ node, vector: [1, 0] }],
      dimensions: 2,
      sourceProjection: { label: "Person", recipe: "alpha_v1" },
    });
    const beta = forge.publishCallerEmbeddings("beta", {
      rows: [{ node, vector: [0, 1] }],
      dimensions: 2,
      sourceProjection: { label: "Person", recipe: "beta_v1" },
    });

    const spaces = forge.embeddingSpaces();
    assert.equal(spaces.length, 2);
    assert.deepEqual(
      spaces.map((space) => space.compatibilityId),
      [alpha, beta].sort(),
    );
    assert.deepEqual(
      new Set(spaces.flatMap((space) => space.aliases)),
      new Set(["alpha", "beta"]),
    );
    for (const space of spaces) {
      assert.equal(space.dimensions, 2);
      assert.deepEqual(space.producer, {
        kind: "callerSupplied",
        contractVersion: "graphforge_binding_caller_v1",
      });
      assert.equal(space.tokenizer, null);
      assert.equal(space.chunking, null);
      assertContentFree(space);
    }

    const bound = forge.bindEmbeddingSpaceAlias("also-alpha", alpha);
    assert.deepEqual(bound.aliases, ["alpha", "also-alpha"]);
    const selected = forge.setDefaultEmbeddingSpace("also-alpha");
    assert.notEqual(selected, null);
    assert.deepEqual(forge.embeddingSpace(), forge.embeddingSpace("alpha"));
    assert.equal(forge.embeddingSpace().defaultAlias, "also-alpha");

    expectValidation("already bound", () =>
      forge.bindEmbeddingSpaceAlias("also-alpha", beta),
    );
    const rebound = forge.bindEmbeddingSpaceAlias("also-alpha", beta, true);
    assert.equal(rebound.compatibilityId, beta);
    forge.setDefaultEmbeddingSpace("beta");
    assert.equal(forge.removeEmbeddingSpaceAlias("also-alpha"), true);
    assert.equal(forge.removeEmbeddingSpaceAlias("also-alpha"), false);
    assert.equal(forge.setDefaultEmbeddingSpace(), null);
    expectValidation("default embedding space", () => forge.embeddingSpace());
    expectValidation("not configured", () => forge.embeddingSpace("missing"));
    forge.setDefaultEmbeddingSpace("alpha");
    forge.close();

    const reopened = new GraphForge(project);
    try {
      assert.equal(reopened.embeddingSpace().compatibilityId, alpha);
      assert.equal(reopened.embeddingSpace().defaultAlias, "alpha");
      assertContentFree(reopened.embeddingSpace());
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
