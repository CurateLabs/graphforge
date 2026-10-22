// Data written by another Core version is reported on the Node surface (#1936).
//
// "Core X" data is made by the real writer and then relabelled: a generation by
// changing its one serialized `producing_core_version` field and recomputing the
// manifest digest CURRENT records; a package by changing the declared version
// and recomputing `package_digest` and the BagIt checksums. No digest is typed in.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GraphForge, version } from "../index.js";

const CORE_X = "0.0.1";
const RUNNING = version();

const sha = (bytes) => createHash("sha256").update(bytes).digest("hex");
const stamp = (value) => `,"producing_core_version":"${value}"}`;

function sortedKeys(value) {
  if (Array.isArray(value)) return value.map(sortedKeys);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sortedKeys(value[key])]),
    );
  }
  return value;
}

async function withTempDir(body) {
  const root = mkdtempSync(join(tmpdir(), "gf-node-core-version-"));
  try {
    await body(root);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function writtenProject(root) {
  const project = join(root, "project");
  const forge = new GraphForge(project);
  forge.execute("CREATE (:Item {score: 1})");
  forge.close();
  return project;
}

// Relabel (or, for null, remove) the CURRENT generation's declared Core.
function relabelGeneration(project, to) {
  const currentPath = join(project, "CURRENT");
  const currentText = readFileSync(currentPath, "utf8");
  const current = JSON.parse(currentText);
  const manifestPath = join(
    project,
    "generations",
    current.generation_uuid,
    "manifest.json",
  );
  const manifest = readFileSync(manifestPath, "utf8");
  const written = stamp(RUNNING);
  assert.ok(manifest.trimEnd().endsWith(written), `not stamped: ${manifest}`);
  const relabelled = manifest.replace(written, to === null ? "}" : stamp(to));
  writeFileSync(manifestPath, relabelled);
  writeFileSync(
    currentPath,
    currentText.replace(current.generation_manifest_sha256, sha(relabelled)),
  );
}

function relabelPackage(pkg, to) {
  const manifestPath = join(pkg, "data", "graphforge-project.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  assert.equal(manifest.producing_core_version, RUNNING);
  manifest.producing_core_version = to;
  delete manifest.package_digest;
  const semantic = JSON.stringify(sortedKeys(manifest));
  manifest.package_digest = `sha256:${sha(Buffer.concat([Buffer.from("graphforge-project/2\0"), Buffer.from(semantic)]))}`;
  writeFileSync(manifestPath, JSON.stringify(sortedKeys(manifest)));
  for (const name of ["manifest-sha256.txt", "tagmanifest-sha256.txt"]) {
    const resealed = readFileSync(join(pkg, name), "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        const path = line.split("  ")[1];
        return `${sha(readFileSync(join(pkg, path)))}  ${path}\n`;
      })
      .join("");
    writeFileSync(join(pkg, name), resealed);
  }
}

async function exportedPackage(root, project) {
  const forge = new GraphForge(project);
  const pkg = join(root, "package");
  await forge.exportPortable({ outputPath: pkg, representation: "expanded" });
  forge.close();
  return pkg;
}

function assertNamesBoth(warning) {
  assert.equal(warning.code, "GF_CORE_VERSION_MISMATCH");
  assert.equal(warning.producingCoreVersion, CORE_X);
  assert.equal(warning.runningCoreVersion, RUNNING);
  assert.ok(warning.message.includes(CORE_X));
  assert.ok(warning.message.includes(RUNNING));
  assert.ok(warning.message.includes(`@curatelabs/graphforge@${CORE_X}`));
}

test("opening another Core's data warns naming both versions", () =>
  withTempDir((root) => {
    const project = writtenProject(root);
    relabelGeneration(project, CORE_X);

    const forge = new GraphForge(project);
    assert.equal(forge.openWarnings.length, 1);
    assertNamesBoth(forge.openWarnings[0]);
    // A mismatch alone never refuses: the project is still readable.
    assert.equal(forge.nodeCount("Item"), 1);
    forge.close();
  }));

test("opening data from the running Core does not warn", () =>
  withTempDir((root) => {
    const project = writtenProject(root);
    const forge = new GraphForge(project);
    assert.deepEqual(forge.openWarnings, []);
    forge.close();
    assert.deepEqual(new GraphForge().openWarnings, []);
  }));

test("opening a generation without a stamp does not warn", () =>
  withTempDir((root) => {
    const project = writtenProject(root);
    relabelGeneration(project, null);

    const forge = new GraphForge(project);
    assert.deepEqual(forge.openWarnings, []);
    assert.equal(forge.nodeCount("Item"), 1);
    forge.close();
  }));

test("importing another Core's package warns and imports", () =>
  withTempDir(async (root) => {
    const pkg = await exportedPackage(root, writtenProject(root));
    relabelPackage(pkg, CORE_X);

    const target = join(root, "imported");
    const result = await GraphForge.importPortable({
      projectRoot: target,
      input: pkg,
      operationId: randomUUID(),
    });

    assert.equal(result.warnings.length, 1);
    assertNamesBoth(result.warnings[0]);
    const imported = new GraphForge(target);
    assert.equal(imported.nodeCount("Item"), 1);
    // The imported generation was written by the running Core.
    assert.deepEqual(imported.openWarnings, []);
    imported.close();
  }));

test("importing the running Core's package does not warn", () =>
  withTempDir(async (root) => {
    const pkg = await exportedPackage(root, writtenProject(root));

    const result = await GraphForge.importPortable({
      projectRoot: join(root, "imported"),
      input: pkg,
      operationId: randomUUID(),
    });

    assert.deepEqual(result.warnings, []);
  }));
