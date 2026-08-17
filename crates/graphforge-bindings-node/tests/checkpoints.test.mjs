// Thin native checkpoint surface acceptance (#2480).

import assert from "node:assert/strict";
import {
  mkdtempSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  symlinkSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";

const require = createRequire(import.meta.url);
let { GraphForge } = require("../index.js");
// A non-platform local `napi build` emits `graphforge.node` without regenerating
// the platform loader. CI/package builds take the normal first branch.
if (typeof GraphForge.prototype.checkpoint !== "function") {
  ({ GraphForge } = require("../graphforge.node"));
}

const operation = (suffix) =>
  `018f0f4e-7b8c-7000-8000-${suffix.toString().padStart(12, "0")}`;

async function settlesOrCancels(promise, cancellationCode, assertCompletion) {
  let result;
  try {
    result = await promise;
  } catch (error) {
    assert.equal(error.code, cancellationCode);
    assert.notEqual(error.name, "AbortError");
    return;
  }
  assertCompletion(tableFromIPC(result));
}

test("checkpoint surfaces delegate to Rust and views stay read-only", async () => {
  const forge = new GraphForge();
  forge.execute("CREATE (:State {value: 'checkpoint'})");

  const created = tableFromIPC(
    await forge.checkpoint({
      name: "Before",
      description: "native Node checkpoint",
      idempotencyKey: operation(1),
    }),
  );
  assert.equal(created.numRows, 1);
  assert.equal(created.getChild("operation").get(0), "checkpoint");

  const view = forge.openCheckpoint("Before");
  const uuidPattern =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  assert.match(view.checkpointUuid, uuidPattern);
  assert.match(view.generationUuid, uuidPattern);
  assert.equal(
    tableFromIPC(view.execute("MATCH (n:State) RETURN n.value AS value"))
      .getChild("value")
      .get(0),
    "checkpoint",
  );
  assert.ok(tableFromIPC(view.projectCapabilities()).numRows >= 2);
  assert.equal(view.inspectAdjacency().state, "missing");
  assert.throws(
    () => view.execute("CREATE (:State {value: 'forbidden'})"),
    (error) => error.code === "GF_READ_ONLY_VIEW",
  );

  forge.execute("CREATE (:State {value: 'transient'})");
  await forge.checkpoint({ name: "After", idempotencyKey: operation(2) });

  const firstPage = tableFromIPC(await forge.listCheckpoints({ limit: 1 }));
  assert.equal(firstPage.numRows, 1);
  const cursor = firstPage.schema.metadata.get("graphforge.next_page_token");
  assert.ok(cursor);
  const secondPage = tableFromIPC(
    await forge.listCheckpoints({ limit: 1, after: cursor }),
  );
  assert.equal(secondPage.numRows, 1);

  const diff = tableFromIPC(
    await forge.diffCheckpoints({
      from: "Before",
      to: "current",
      scope: "summary",
      detail: "summary",
    }),
  );
  assert.ok(diff.numRows >= 1);

  const reverted = tableFromIPC(
    await forge.revertToCheckpoint({
      name: "Before",
      reason: "verify same-instance Node visibility",
      idempotencyKey: operation(3),
    }),
  );
  assert.equal(reverted.getChild("operation").get(0), "revert_to_checkpoint");
  assert.ok(reverted.getChild("result_generation_uuid").get(0));

  const values = tableFromIPC(
    forge.execute("MATCH (n:State) RETURN n.value AS value ORDER BY value"),
  );
  assert.deepEqual([...values.getChild("value").toArray()], ["checkpoint"]);
  assert.equal(
    tableFromIPC(view.execute("MATCH (n:State) RETURN n.value AS value"))
      .getChild("value")
      .get(0),
    "checkpoint",
  );

  const deleted = tableFromIPC(
    await forge.deleteCheckpoint({
      name: "Before",
      idempotencyKey: operation(4),
    }),
  );
  assert.equal(deleted.getChild("operation").get(0), "delete_checkpoint");
  assert.throws(
    () => forge.openCheckpoint("Before"),
    (error) => error.code === "GF_CHECKPOINT_NOT_FOUND",
  );
});

test("checkpoint pagination and diff cancellation use the shared adapter", async () => {
  const forge = new GraphForge();
  await forge.checkpoint({ name: "One", idempotencyKey: operation(10) });

  const controller = new AbortController();
  const listing = forge.listCheckpoints({ signal: controller.signal });
  controller.abort();
  await settlesOrCancels(listing, "GF_CANCELLED", (table) => {
    assert.equal(table.numRows, 1);
    assert.equal(table.getChild("name").get(0), "One");
  });

  const diffController = new AbortController();
  const diff = forge.diffCheckpoints({
    from: "One",
    to: "current",
    scope: "all",
    detail: "records",
    signal: diffController.signal,
  });
  diffController.abort();
  await settlesOrCancels(diff, "GF_CANCELLED", (table) => {
    assert.equal(table.numRows, 0);
    assert.deepEqual(
      table.schema.fields.map((field) => field.name),
      [
        "from_checkpoint_uuid",
        "to_checkpoint_uuid",
        "scope",
        "record_family_id",
        "record_uuid",
        "record_identity_fingerprint",
        "change_kind",
        "from_record_fingerprint",
        "to_record_fingerprint",
      ],
    );
  });
});

test(
  "async checkpoint preserves filesystem admission code after root substitution",
  { skip: process.platform === "win32" },
  async () => {
    const fixture = mkdtempSync(
      join(tmpdir(), "gf-node-checkpoint-admission-"),
    );
    const parent = realpathSync(fixture);
    const project = join(parent, "project");
    const moved = join(parent, "project-moved");
    const forge = new GraphForge(project);
    try {
      await forge.checkpoint({ name: "Before", idempotencyKey: operation(20) });
      const currentBefore = readFileSync(join(project, "CURRENT"));
      renameSync(project, moved);
      symlinkSync(moved, project, "dir");

      await assert.rejects(
        forge.checkpoint({ name: "Rejected", idempotencyKey: operation(21) }),
        (error) => error.code === "GF_UNSUPPORTED_FILESYSTEM",
      );
      assert.deepEqual(readFileSync(join(moved, "CURRENT")), currentBefore);
    } finally {
      forge.close();
      rmSync(fixture, { recursive: true, force: true });
    }
  },
);
