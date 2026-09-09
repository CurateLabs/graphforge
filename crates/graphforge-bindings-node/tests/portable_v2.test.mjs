import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("portable-v2 export verify and import preserve package digest", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-portable-"));
  try {
    const source = join(root, "source");
    const forge = new GraphForge(source);
    forge.execute("CREATE (:Person {name: 'Ada'})");
    const preview = forge.previewPortableV2Selection({ profile: "complete" });
    assert.equal(preview.packageClass, "complete");
    assert.equal(preview.includeGraphTree, true);
    assert.deepEqual(preview.projected, []);
    const nodeBytes = tableFromIPC(
      forge.execute("MATCH (n:Person) RETURN n.node_uuid AS id"),
    )
      .getChild("id")
      .get(0);
    const nodeId = Buffer.from(nodeBytes)
      .toString("hex")
      .replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, "$1-$2-$3-$4-$5");
    const subset = forge.previewPortableV2GraphSubset({
      subset: { selector: { nodeUuids: [nodeId] }, closure: "induced_edges" },
    });
    assert.equal(subset.selectedNodeCount, 1);
    assert.equal(subset.selection.includeGraphTree, true);
    assert.deepEqual(subset.selection.projected, []);
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
    const operationId = randomUUID();
    const imported = await GraphForge.importPortableV2({
      projectRoot: join(root, "target"),
      input: bundle,
      operationId,
    });
    assert.equal(imported.packageDigest, bundleExport.packageDigest);
    const replay = await GraphForge.importPortableV2({
      projectRoot: join(root, "target"),
      input: bundle,
      operationId,
    });
    assert.equal(replay.idempotentReplay, true);
    assert.equal(replay.generationUuid, imported.generationUuid);
    assert.equal(replay.packageDigest, imported.packageDigest);
    assert.equal(
      JSON.parse(readFileSync(join(root, "target", "CURRENT"), "utf8"))
        .generation_uuid,
      imported.generationUuid,
    );
    const reopened = new GraphForge(join(root, "target"));
    assert.deepEqual(
      tableFromIPC(reopened.execute("MATCH (n:Person) RETURN n.name AS name"))
        .getChild("name")
        .toArray(),
      ["Ada"],
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// Explicit complete mapping: no fields/identities are dropped during comparison.
function exactNumber(value) {
  assert.equal(typeof value, "bigint");
  const number = Number(value);
  assert.ok(Number.isSafeInteger(number));
  assert.equal(BigInt(number), value);
  return number;
}

export function canonicalVerification(report) {
  return {
    contract: report.contract,
    representation: report.representation,
    package_digest: report.packageDigest,
    package_class: report.packageClass.replaceAll("_", "-"),
    component_count: exactNumber(report.componentCount),
    entry_count: exactNumber(report.entryCount),
    payload_bytes: exactNumber(report.payloadBytes),
    integrity: report.integrity,
    compatibility: report.compatibility,
    authenticity: report.authenticity,
    transport_digest: report.transportDigest ?? null,
    ontology_composition: report.ontologyComposition,
    ontology_composition_entries: report.ontologyCompositionEntries.map(
      (entry) => ({
        kind: entry.kind,
        identity: entry.identity,
        path: entry.path,
        media_type: entry.mediaType,
        length: exactNumber(entry.length),
        sha256: entry.sha256,
        required_dependencies: entry.requiredDependencies,
      }),
    ),
  };
}

test("shared immutable package has the complete Rust facade verification receipt", async () => {
  const root = fileURLToPath(new URL("../../..", import.meta.url));
  const input = join(
    root,
    "tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb",
  );
  const expected = JSON.parse(
    readFileSync(
      join(
        root,
        "tests/fixtures/portable-v2/facade-verification-receipts.json",
      ),
      "utf8",
    ),
  );
  for (const mode of ["full", "structure_only"]) {
    const report = await GraphForge.verifyPortableV2({ input, mode });
    assert.equal(typeof report.componentCount, "bigint");
    assert.equal(typeof report.entryCount, "bigint");
    assert.equal(typeof report.payloadBytes, "bigint");
    assert.deepEqual(canonicalVerification(report), expected[mode]);
    assert.equal(
      Object.keys(report).length,
      Object.keys(expected[mode]).length,
    );
  }
});
