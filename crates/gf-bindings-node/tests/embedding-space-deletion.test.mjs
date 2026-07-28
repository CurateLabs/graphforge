// Fresh-addon acceptance for explicit embedding-space deletion.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

function publish(forge, node, name, vector) {
  return forge.publishCallerEmbeddings(name, {
    rows: [{ node, vector }],
    dimensions: 2,
    sourceProjection: { label: "Person", recipe: `${name}_v1` },
  });
}

test("embedding spaces delete by name and default without affecting peers", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-embedding-delete-"));
  const forge = new GraphForge(project);
  try {
    const node = forge.addNode("Person", { name: "Alice" });
    const obsolete = publish(forge, node, "obsolete", [1, 0]);
    const retained = publish(forge, node, "retained", [0, 1]);
    forge.bindEmbeddingSpaceAlias("obsolete-copy", obsolete);
    forge.setDefaultEmbeddingSpace("obsolete-copy");

    assert.equal(forge.deleteEmbeddingSpace("obsolete"), true);
    assert.equal(forge.deleteEmbeddingSpace("obsolete"), false);
    expectValidation("default embedding space", () => forge.embeddingSpace());
    assert.equal(forge.embeddingSpace("retained").compatibilityId, retained);
    expectValidation("not configured", () =>
      forge.embeddingSpace("obsolete-copy"),
    );
    expectValidation("display name", () => forge.deleteEmbeddingSpace("\n"));
    assert.deepEqual(
      forge.embeddingSpaces().map((space) => space.compatibilityId),
      [retained],
    );
    const result = tableFromIPC(
      forge.find(
        undefined,
        "Person",
        [0, 1],
        undefined,
        undefined,
        1,
        "retained",
      ),
    );
    assert.deepEqual(Array.from(result.getChild("node_uuid"), uuidHex), [
      node.uuid.replaceAll("-", ""),
    ]);

    forge.setDefaultEmbeddingSpace("retained");
    assert.equal(forge.deleteEmbeddingSpace(), true);
    assert.equal(forge.deleteEmbeddingSpace(), false);
    assert.deepEqual(forge.embeddingSpaces(), []);
    forge.close();

    const reopened = new GraphForge(project);
    try {
      assert.deepEqual(reopened.embeddingSpaces(), []);
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
