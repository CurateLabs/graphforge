import assert from "node:assert/strict";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { randomUUID } from "node:crypto";
import {
  FixedSizeBinary,
  Table,
  Utf8,
  tableToIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "../index.js";

test("import session begin checkpoint resume abort lifecycle", () => {
  const root = mkdtempSync(join(tmpdir(), "gf-import-"));
  try {
    const forge = new GraphForge(join(root, "project"));
    const operation = randomUUID();
    const session = forge.beginImportSession(operation);
    assert.equal(session.status().phase, "open");
    const nodeUuid = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const table = new Table({
      node_uuid: vectorFromArray([nodeUuid], new FixedSizeBinary(16)),
      label: vectorFromArray(["Person"], new Utf8()),
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

test("aborting an import by uuid removes one recorded in a retired format", () => {
  const root = mkdtempSync(join(tmpdir(), "gf-import-"));
  try {
    const project = join(root, "project");
    const forge = new GraphForge(project);
    const session = forge.beginImportSession(randomUUID());
    const nodeUuid = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const table = new Table({
      node_uuid: vectorFromArray([nodeUuid], new FixedSizeBinary(16)),
      label: vectorFromArray(["Person"], new Utf8()),
    });
    session.appendArrow("node", Buffer.from(tableToIPC(table)));
    session.checkpoint();
    const sessionUuid = session.sessionUuid;

    const sessionRoot = join(project, "import-sessions", sessionUuid);
    const manifestPath = join(sessionRoot, "manifest.json");
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    manifest.format_version = 2;
    writeFileSync(manifestPath, JSON.stringify(manifest));

    assert.throws(
      () => forge.resumeImportSession(sessionUuid),
      /restart the import/,
    );
    const progress = forge.abortImportSession(sessionUuid);
    assert.equal(progress.filesAccepted, 0n);
    assert.equal(existsSync(sessionRoot), false);
    assert.throws(() => forge.abortImportSession(sessionUuid));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
