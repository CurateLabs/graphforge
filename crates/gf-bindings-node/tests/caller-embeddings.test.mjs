// Fresh-addon acceptance for complete caller embedding publication.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

test("complete caller embedding batches publish, search, and reopen", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-caller-embeddings-"));
  const forge = new GraphForge(project);
  try {
    const alice = forge.addNode("Person", { name: "Alice" });
    const bob = forge.addNode("Person", { name: "Bob" });
    const rows = [
      { node: alice, vector: [1, 0] },
      {
        node: { label: "Person", property: "name", value: "Bob" },
        vector: [0, 1],
      },
    ];
    const input = {
      rows,
      dimensions: 2,
      sourceProjection: { label: "Person", recipe: "all_people_v1" },
    };
    const identity = forge.publishCallerEmbeddings("semantic", input);
    assert.equal(identity.length, 64);
    assert.equal(
      forge.publishCallerEmbeddings("semantic", {
        ...input,
        sourceProjection: { recipe: "all_people_v1", label: "Person" },
      }),
      identity,
    );

    const l2Identity = forge.publishCallerEmbeddings("l2", {
      rows: [{ node: alice.uuid, vector: [3, 4] }],
      dimensions: 2,
      normalization: "l2",
      sourceProjection: { label: "Person", recipe: "alice_l2_v1" },
    });
    assert.notEqual(l2Identity, identity);

    const replacementInput = {
      rows: [{ node: alice, vector: [1, 0] }],
      dimensions: 2,
      contractVersion: "replacement_v1",
      sourceProjection: { label: "Person", recipe: "replacement_v1" },
    };
    const originalReplacement = forge.publishCallerEmbeddings(
      "replaceable",
      replacementInput,
    );
    expectValidation("already targets", () =>
      forge.publishCallerEmbeddings("replaceable", {
        ...replacementInput,
        contractVersion: "replacement_v2",
      }),
    );
    const replaced = forge.publishCallerEmbeddings("replaceable", {
      ...replacementInput,
      contractVersion: "replacement_v2",
      replace: true,
    });
    assert.notEqual(replaced, originalReplacement);

    const result = tableFromIPC(
      forge.find(
        undefined,
        "Person",
        [1, 0],
        undefined,
        undefined,
        2,
        "semantic",
      ),
    );
    assert.deepEqual(Array.from(result.getChild("node_uuid"), uuidHex), [
      handleHex(alice),
      handleHex(bob),
    ]);
    assert.deepEqual([...result.getChild("score").toArray()], [1, 0]);
    for (const knowledge of [
      "confidence",
      "provenance_id",
      "assertion_uuid",
      "belief_status",
      "valid_time",
    ]) {
      assert.equal(result.getChild(knowledge), null);
    }

    expectValidation("duplicate", () =>
      forge.publishCallerEmbeddings("duplicate", {
        rows: [
          { node: alice, vector: [1, 0] },
          { node: alice.uuid, vector: [0, 1] },
        ],
        dimensions: 2,
        sourceProjection: { label: "Person" },
      }),
    );
    expectValidation("finite", () =>
      forge.publishCallerEmbeddings("nonfinite", {
        rows: [{ node: alice, vector: [Number.NaN, 1] }],
        dimensions: 2,
        sourceProjection: { label: "Person" },
      }),
    );
    expectValidation("zero", () =>
      forge.publishCallerEmbeddings("zero", {
        rows: [{ node: alice, vector: [0, 0] }],
        dimensions: 2,
        sourceProjection: { label: "Person" },
      }),
    );
    expectValidation("normalization", () =>
      forge.publishCallerEmbeddings("normalization", {
        rows: [{ node: alice, vector: [1, 0] }],
        dimensions: 2,
        normalization: "unit-ish",
        sourceProjection: { label: "Person" },
      }),
    );
    expectValidation("dimension", () =>
      forge.publishCallerEmbeddings("width", {
        rows: [{ node: alice, vector: [1] }],
        dimensions: 2,
        sourceProjection: { label: "Person" },
      }),
    );

    const foreignProject = mkdtempSync(
      join(tmpdir(), "gf-node-caller-embeddings-foreign-"),
    );
    const foreignForge = new GraphForge(foreignProject);
    try {
      const foreign = foreignForge.addNode("Person", { name: "Mallory" });
      expectValidation("another graph instance", () =>
        forge.publishCallerEmbeddings("foreign", {
          rows: [{ node: foreign, vector: [1, 0] }],
          dimensions: 2,
          sourceProjection: { label: "Person" },
        }),
      );
    } finally {
      foreignForge.close();
      rmSync(foreignProject, { recursive: true, force: true });
    }
    expectValidation("exceeds the finite f32 range", () =>
      forge.publishCallerEmbeddings("range", {
        rows: [{ node: alice, vector: [Number.MAX_VALUE, 1] }],
        dimensions: 2,
        sourceProjection: { label: "Person" },
      }),
    );

    const empty = forge.publishCallerEmbeddings("empty", {
      rows: [],
      dimensions: 3,
      sourceProjection: { label: "Nobody" },
    });
    assert.equal(empty.length, 64);
    forge.close();

    const reopened = new GraphForge(project);
    try {
      const persisted = tableFromIPC(
        reopened.find(
          undefined,
          "Person",
          [0, 1],
          undefined,
          undefined,
          2,
          "semantic",
        ),
      );
      assert.equal(
        uuidHex(persisted.getChild("node_uuid").get(0)),
        handleHex(bob),
      );
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
