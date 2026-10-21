import assert from "node:assert/strict";
import {
  mkdtempSync,
  rmSync,
  readFileSync,
  writeFileSync,
  existsSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("portable export verify and import preserve package digest", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-portable-"));
  try {
    const source = join(root, "source");
    const forge = new GraphForge(source);
    forge.execute("CREATE (:Person {name: 'Ada'})");
    const preview = forge.previewPortableSelection({ profile: "complete" });
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
    const subset = forge.previewPortableGraphSubset({
      subset: { selector: { nodeUuids: [nodeId] }, closure: "induced_edges" },
    });
    assert.equal(subset.selectedNodeCount, 1);
    assert.equal(subset.selection.includeGraphTree, true);
    assert.deepEqual(subset.selection.projected, []);
    const expanded = join(root, "expanded");
    const bundle = join(root, "complete.gfpb");
    const expandedExport = await forge.exportPortable({
      outputPath: expanded,
      representation: "expanded",
      profile: "complete",
    });
    const bundleExport = await forge.exportPortable({
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
    const verified = await GraphForge.verifyPortable({
      input: bundle,
      mode: "full",
    });
    assert.equal(verified.packageDigest, bundleExport.packageDigest);
    const operationId = randomUUID();
    const imported = await GraphForge.importPortable({
      projectRoot: join(root, "target"),
      input: bundle,
      operationId,
    });
    assert.equal(imported.packageDigest, bundleExport.packageDigest);
    const replay = await GraphForge.importPortable({
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
    research_interchange: report.researchInterchange,
    research_entries: report.researchEntries.map((entry) => ({
      component_id: entry.componentId,
      path: entry.path,
      length: exactNumber(entry.length),
      sha256: entry.sha256,
    })),
    contract: report.contract,
    representation: report.representation,
    source: report.source,
    package_digest: report.packageDigest,
    package_class: report.packageClass.replaceAll("_", "-"),
    producing_core_version: report.producingCoreVersion,
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
      join(root, "tests/fixtures/portable/facade-verification-receipts.json"),
      "utf8",
    ),
  );
  for (const mode of ["full", "structure_only"]) {
    const report = await GraphForge.verifyPortable({ input, mode });
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

test("retired project format is rejected before a binding import touches its destination", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-retired-format-"));
  try {
    const input = join(root, "unsupported.bin");
    const target = join(root, "target");
    writeFileSync(input, Buffer.from("graphforge-exp\0\n"));
    await assert.rejects(
      GraphForge.importPortable({
        input,
        projectRoot: target,
        operationId: randomUUID(),
      }),
      (error) => error.code === "GF_UNSUPPORTED_PROJECT_FORMAT",
    );
    assert.equal(existsSync(target), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

function canonicalJson(value) {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

// An expanded package whose resealed manifest declares eight tebibytes, so its
// controls are admitted and only the disk can refuse it.
async function packageTooLargeForAnyDisk(root) {
  const source = join(root, "source");
  const package_ = join(root, "too-large.gfproject");
  await new GraphForge(source).exportPortable({
    outputPath: package_,
    representation: "expanded",
    profile: "complete",
  });
  const manifestPath = join(package_, "data", "graphforge-project.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  delete manifest.package_digest;
  manifest.components[0].files[0].length += 2 ** 43;
  const preimage = Buffer.concat([
    Buffer.from("graphforge-project/2\0"),
    Buffer.from(canonicalJson(manifest)),
  ]);
  manifest.package_digest = `sha256:${createHash("sha256").update(preimage).digest("hex")}`;
  writeFileSync(manifestPath, canonicalJson(manifest));
  return package_;
}

async function refusedImport(projectRoot, input) {
  try {
    await GraphForge.importPortable({
      input,
      projectRoot,
      operationId: randomUUID(),
    });
  } catch (error) {
    return error;
  }
  throw new Error("the import was admitted");
}

test("import refusals before extraction keep their code and diagnostic", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-refused-import-"));
  try {
    const input = await packageTooLargeForAnyDisk(root);
    const target = join(root, "destination");
    let error = await refusedImport(target, input);
    assert.equal(error.code, "GF_STORAGE");
    let envelope = JSON.parse(error.message);
    assert.equal(envelope.diagnostics[0].code, "storage.insufficient_space");
    assert.match(
      envelope.diagnostics[0].message,
      /bytes available beside the target/,
    );
    assert.equal(existsSync(target), false);
    if (process.platform === "linux") {
      // `/dev/shm` is tmpfs, which the durable-publication contract does not admit.
      const shm = mkdtempSync("/dev/shm/gf-refused-import-");
      try {
        const shmTarget = join(shm, "destination");
        error = await refusedImport(shmTarget, input);
        assert.equal(error.code, "GF_UNSUPPORTED_FILESYSTEM");
        envelope = JSON.parse(error.message);
        assert.equal(
          envelope.diagnostics[0].code,
          "storage.unsupported_filesystem",
        );
        assert.match(
          envelope.diagnostics[0].message,
          /GF_UNSUPPORTED_FILESYSTEM/,
        );
        assert.equal(existsSync(shmTarget), false);
      } finally {
        rmSync(shm, { recursive: true, force: true });
      }
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
