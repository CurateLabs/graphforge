import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { randomUUID } from "node:crypto";
import { tableToIPC, tableFromArrays } from "apache-arrow";
import { GraphForge } from "../index.js";

test("import session begin checkpoint resume abort lifecycle", () => {
  const root = mkdtempSync(join(tmpdir(), "gf-import-"));
  try {
    const forge = new GraphForge(join(root, "project"));
    const operation = randomUUID();
    const session = forge.beginImportSession(operation);
    assert.equal(session.status().phase, "open");
    const nodeUuid = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const table = tableFromArrays({
      node_uuid: [nodeUuid],
      label: ["Person"],
    });
    session.appendArrow("node", Buffer.from(tableToIPC(table)));
    const progress = session.checkpoint();
    assert.ok(progress.filesPending >= 1n);
    assert.equal(typeof progress.bytesAccepted, "bigint");
    const sessionUuid = session.sessionUuid;
    const resumed = forge.resumeImportSession(sessionUuid);
    assert.equal(resumed.sessionUuid, sessionUuid);
    const aborted = resumed.abort();
    assert.ok(aborted.filesAccepted >= 1n);
    const cleaned = forge.cleanupStaleImportSessions(0n);
    assert.equal(typeof cleaned, "bigint");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
