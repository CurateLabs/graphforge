import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { randomUUID } from "node:crypto";
import { GraphForge } from "../index.js";

test("portable-v2 export verify and import preserve package digest", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-portable-"));
  try {
    const source = join(root, "source");
    const forge = new GraphForge(source);
    const preview = forge.previewPortableV2Selection({ profile: "complete" });
    assert.equal(preview.packageClass, "complete");
    const expanded = join(root, "expanded");
    const bundle = join(root, "complete.gfpb");
    const expandedExport = await forge.exportPortableV2({
      outputPath: expanded,
      representation: "expanded",
      profile: "complete",
    });
    const bundleExport = await forge.exportPortableV2({
      outputPath: bundle,
      representation: "bundle",
      profile: "complete",
    });
    assert.equal(expandedExport.packageDigest, bundleExport.packageDigest);
    assert.equal(
      expandedExport.selectionFingerprint,
      preview.selectionFingerprint,
    );
    assert.equal(typeof bundleExport.payloadBytes, "bigint");
    const verified = await GraphForge.verifyPortableV2({
      input: bundle,
      mode: "full",
    });
    assert.equal(verified.packageDigest, bundleExport.packageDigest);
    const imported = await GraphForge.importPortableV2({
      projectRoot: join(root, "target"),
      input: bundle,
      operationId: randomUUID(),
    });
    assert.equal(imported.packageDigest, bundleExport.packageDigest);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
