import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const checkoutRoot = fileURLToPath(new URL("../../..", import.meta.url));
const nativeRoot = join(checkoutRoot, "crates", "graphforge-bindings-node");
const lifecycleContract = JSON.parse(
  readFileSync(
    join(checkoutRoot, "tests", "contracts", "repository-cli-lifecycle.json"),
    "utf8",
  ),
);
const packageMetadata = JSON.parse(
  readFileSync(join(packageRoot, "package.json"), "utf8"),
);
const fixture = mkdtempSync(join(tmpdir(), "graphforge-native-lifecycle-"));
const installRoot = join(fixture, "consumer");
const repository = join(fixture, "repository");
const destination = join(fixture, "destination");
mkdirSync(installRoot);
mkdirSync(repository);
mkdirSync(destination);
const npmrc = join(fixture, "empty.npmrc");
writeFileSync(npmrc, "");
const childEnvironment = Object.fromEntries(
  Object.entries(process.env).filter(
    ([name]) => !name.toLowerCase().startsWith("npm_config_"),
  ),
);
childEnvironment.NPM_CONFIG_USERCONFIG = npmrc;

assert.equal(lifecycleContract.contract, "graphforge-packed-cli-lifecycle/1");
assert.equal(
  lifecycleContract.scope.portableInterchange,
  "complete_project_generation",
);
assert.deepEqual(lifecycleContract.scope.ontologyLifecycle, {
  owner: "#236",
  operations: [
    "inspect_runtime_catalog",
    "suggest_ontology",
    "validate_ontology",
    "export_ontology",
  ],
});
assert.deepEqual(lifecycleContract.scope.ontologyBindingParity, {
  owner: "#237",
  operations: [
    "inspect_runtime_catalog",
    "suggest_ontology",
    "validate_ontology",
    "export_ontology",
    "adopt_ontology",
    "clear_ontology",
  ],
});

const requiredScenarios = new Set(
  lifecycleContract.requiredScenarios.map(({ name }) => name),
);
const coveredScenarios = new Set();
const cover = (name) => {
  assert.equal(requiredScenarios.has(name), true, `unknown scenario ${name}`);
  coveredScenarios.add(name);
};
const {
  syncOperation,
  syncActor,
  checkpointCreateOperation,
  checkpointDeleteOperation,
  revertOperation,
  revertActor,
  importOperation,
} = lifecycleContract.identities;

const pack = (directory, pnpm = false) => {
  const command = pnpm ? "pnpm" : "npm";
  const args = pnpm
    ? ["pack", "--json", "--pack-destination", fixture]
    : ["pack", "--ignore-scripts", "--json", "--pack-destination", fixture];
  const payload = JSON.parse(
    execFileSync(command, args, { cwd: directory, encoding: "utf8" }),
  );
  const result = Array.isArray(payload) ? payload[0] : payload;
  return isAbsolute(result.filename)
    ? result.filename
    : join(fixture, result.filename);
};

const parseJson = (bytes, context) => {
  try {
    return JSON.parse(bytes);
  } catch (error) {
    assert.fail(`${context} did not emit JSON: ${error.message}\n${bytes}`);
  }
};

const currentBytes = (root = repository) =>
  readFileSync(join(root, ".graphforge", "state", "CURRENT"));

const resultField = (result, field, row = 0) => {
  assert.equal(result.contract, "graphforge-cli-result/1");
  const index = result.columns.findIndex((column) => column.name === field);
  assert.notEqual(index, -1, `missing result column ${field}`);
  assert.ok(result.rows[row], `missing result row ${row}`);
  return result.rows[row][index];
};

try {
  assert.equal(
    readdirSync(nativeRoot).some((name) => name.endsWith(".node")),
    true,
    "build @curatelabs/graphforge before running the native lifecycle acceptance",
  );
  const nativeTarball = pack(nativeRoot);
  const cliTarball = pack(packageRoot, true);
  writeFileSync(
    join(installRoot, "package.json"),
    `${JSON.stringify({ name: "graphforge-lifecycle-consumer", private: true })}\n`,
  );
  execFileSync(
    "npm",
    [
      "install",
      "--offline",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      nativeTarball,
      cliTarball,
    ],
    { cwd: installRoot, stdio: "pipe" },
  );

  // The public CLI must close over the same-version public native package after
  // packing. A workspace protocol or an undeclared checkout dependency would
  // make a clean, offline consumer unusable.
  const installedCliRoot = join(
    installRoot,
    "node_modules",
    "@curatelabs",
    "graphforge-cli",
  );
  const installedNativeRoot = join(
    installRoot,
    "node_modules",
    "@curatelabs",
    "graphforge",
  );
  const installedCli = JSON.parse(
    readFileSync(join(installedCliRoot, "package.json"), "utf8"),
  );
  const installedNative = JSON.parse(
    readFileSync(join(installedNativeRoot, "package.json"), "utf8"),
  );
  assert.equal(installedCli.version, packageMetadata.version);
  assert.equal(installedNative.version, packageMetadata.version);
  assert.equal(
    installedCli.dependencies["@curatelabs/graphforge"],
    packageMetadata.version,
  );
  assert.equal(
    installedCli.dependencies["@curatelabs/graphforge"].includes("workspace:"),
    false,
  );

  execFileSync("git", ["init", "-q", repository]);
  execFileSync("git", ["init", "-q", destination]);

  const invocations = [];
  const runAt = (root, ...args) => {
    invocations.push(args);
    // Every call is a new npx and Node process against the packed, offline
    // consumer. No module instance or in-memory GraphForge state is reused.
    return spawnSync(
      "npx",
      [
        "--offline",
        "--no-install",
        "graphforge",
        "--project-dir",
        root,
        "--json",
        ...args,
      ],
      { cwd: installRoot, encoding: "utf8", env: childEnvironment },
    );
  };
  const run = (...args) => runAt(repository, ...args);
  const successAt = (root, ...args) => {
    const result = runAt(root, ...args);
    assert.equal(result.status, 0, result.stderr);
    return parseJson(result.stdout, args.join(" "));
  };
  const success = (...args) => successAt(repository, ...args);
  const failure = (expectedStatus, ...args) => {
    const result = run(...args);
    assert.equal(result.status, expectedStatus, result.stderr);
    return parseJson(result.stderr, args.join(" "));
  };

  const initialized = success("init");
  assert.equal(initialized.created_config, true);
  assert.equal(initialized.skills.changed, true);
  const initializedCurrent = currentBytes();
  const reopened = success("init");
  assert.equal(reopened.created_config, false);
  assert.equal(reopened.ignore_changed, false);
  assert.equal(reopened.skills.changed, false);
  assert.deepEqual(currentBytes(), initializedCurrent);
  cover("init_and_reopen");

  writeFileSync(
    join(repository, ".graphforge", "graphforge.yaml"),
    readFileSync(
      join(checkoutRoot, "docs", "contracts", "examples", "graphforge-v1.yaml"),
    ),
  );
  const beforeStaticValidation = currentBytes();
  assert.deepEqual(success("config", "validate"), { valid: true });
  assert.deepEqual(
    success("config", "resolve"),
    JSON.parse(
      readFileSync(
        join(
          checkoutRoot,
          "docs",
          "contracts",
          "examples",
          "graphforge-resolved-v1.json",
        ),
        "utf8",
      ),
    ),
  );
  const infra = success("infra", "validate", "--target", "production");
  assert.deepEqual(
    infra,
    JSON.parse(
      readFileSync(
        join(
          checkoutRoot,
          "docs",
          "contracts",
          "examples",
          "graphforge-infra-validation-production-v1.json",
        ),
        "utf8",
      ),
    ),
  );
  assert.equal(infra.connectivity.status, "not_checked");
  assert.equal(infra.readiness.status, "not_checked");
  assert.deepEqual(currentBytes(), beforeStaticValidation);
  cover("configuration_and_static_infra");

  for (const [path, bytes] of [
    [".graphforge/ontology/keep.yaml", "version: 1\n"],
    [".graphforge/schemas/keep.json", "{}\n"],
    [".graphforge/migrations/001.yaml", "version: 1\n"],
    [".graphforge/seeds/recipe.yaml", "version: 1\n"],
    [".graphforge/state/private.parquet", "graph data\n"],
    [".graphforge/imports/source.arrow", "source data\n"],
    [".graphforge/imports/materialized-seed.db", "seed rows\n"],
    [".graphforge/exports/snapshot.gfportable", "snapshot bytes\n"],
  ]) {
    const target = join(repository, path);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, bytes);
  }
  execFileSync("git", ["-C", repository, "add", "--all"]);
  const staged = execFileSync(
    "git",
    ["-C", repository, "diff", "--cached", "--name-only"],
    { encoding: "utf8" },
  )
    .trim()
    .split("\n");
  for (const expected of [
    ".graphforge/graphforge.yaml",
    ".graphforge/ontology/keep.yaml",
    ".graphforge/schemas/keep.json",
    ".graphforge/migrations/001.yaml",
    ".graphforge/seeds/recipe.yaml",
    ".agents/skills/.graphforge-managed.json",
  ]) {
    assert.equal(
      staged.includes(expected),
      true,
      `expected ${expected} in Git`,
    );
  }
  for (const forbidden of [
    ".graphforge/state/",
    ".graphforge/imports/",
    ".graphforge/exports/",
    ".parquet",
    ".arrow",
    ".db",
    ".sqlite",
    ".duckdb",
    ".gfportable",
  ]) {
    assert.equal(
      staged.some((path) => path.includes(forbidden)),
      false,
      `Git staged data path matching ${forbidden}`,
    );
  }
  cover("git_data_boundary");

  const beforeCheck = currentBytes();
  const drift = run("sync", "--check");
  assert.equal(drift.status, 4, drift.stderr);
  assert.equal(parseJson(drift.stdout, "sync --check").status, "drift");
  assert.deepEqual(currentBytes(), beforeCheck);
  const synced = success(
    "sync",
    "--idempotency-key",
    syncOperation,
    "--actor-uuid",
    syncActor,
  );
  assert.equal(synced.status, "published");
  assert.equal(synced.requested_operation_uuid, syncOperation);
  assert.equal(synced.snapshot_operation_uuid, syncOperation);
  assert.equal(synced.snapshot_actor_uuid, syncActor);
  const publishedCurrent = currentBytes();
  const replayedSync = success(
    "sync",
    "--idempotency-key",
    syncOperation,
    "--actor-uuid",
    syncActor,
  );
  assert.equal(replayedSync.status, "in_sync");
  assert.equal(replayedSync.idempotent_replay, true);
  assert.deepEqual(currentBytes(), publishedCurrent);
  assert.equal(success("sync", "--check").status, "in_sync");

  // Repository files not declared by graphforge.yaml are never scanned into
  // the snapshot. Adding one cannot create sync drift.
  writeFileSync(join(repository, "unrelated.txt"), "not a definition\n");
  assert.equal(success("sync", "--check").status, "in_sync");
  cover("sync_check_apply_and_replay");

  const created = success(
    "checkpoint",
    "create",
    "before-change",
    "--idempotency-key",
    checkpointCreateOperation,
  );
  assert.equal(
    resultField(created, "operation_uuid"),
    checkpointCreateOperation,
  );
  const listed = success("checkpoint", "list");
  assert.equal(resultField(listed, "name"), "before-change");
  const shown = success("checkpoint", "show", "before-change");
  assert.equal(resultField(shown, "name"), "before-change");
  const diff = success(
    "checkpoint",
    "diff",
    "--from",
    "before-change",
    "--to-current",
    "--scope",
    "all",
    "--detail",
    "summary",
  );
  assert.equal(diff.contract, "graphforge-cli-result/1");

  const beforePreview = currentBytes();
  const preview = success("revert", "before-change", "--preview");
  assert.equal(preview.contract, "graphforge-revert-preview/1");
  assert.deepEqual(currentBytes(), beforePreview);
  const refusal = failure(
    2,
    "revert",
    "before-change",
    "--reason",
    "packed npm lifecycle acceptance",
    "--idempotency-key",
    revertOperation,
    "--actor-uuid",
    revertActor,
  );
  assert.equal(refusal.error.code, "GF_VALIDATION");
  assert.deepEqual(currentBytes(), beforePreview);
  const reverted = success(
    "revert",
    "before-change",
    "--reason",
    "packed npm lifecycle acceptance",
    "--idempotency-key",
    revertOperation,
    "--actor-uuid",
    revertActor,
    "--yes",
  );
  const revertedCurrent = currentBytes();
  assert.notDeepEqual(revertedCurrent, beforePreview);
  assert.equal(resultField(reverted, "operation_uuid"), revertOperation);
  const replayedRevert = success(
    "revert",
    "before-change",
    "--reason",
    "packed npm lifecycle acceptance",
    "--idempotency-key",
    revertOperation,
    "--actor-uuid",
    revertActor,
    "--yes",
  );
  assert.deepEqual(currentBytes(), revertedCurrent);
  assert.deepEqual(replayedRevert, reverted);
  assert.equal(
    resultField(success("checkpoint", "list"), "name"),
    "before-change",
  );
  const deleted = success(
    "checkpoint",
    "delete",
    "before-change",
    "--idempotency-key",
    checkpointDeleteOperation,
  );
  assert.equal(
    resultField(deleted, "operation_uuid"),
    checkpointDeleteOperation,
  );
  cover("checkpoint_and_top_level_revert");

  const envelope = join(
    repository,
    ".graphforge",
    "exports",
    "current.gfportable",
  );
  const secondEnvelope = join(
    repository,
    ".graphforge",
    "exports",
    "current-copy.gfportable",
  );
  const exported = success("export", "--current", "--output", envelope);
  assert.equal(exported.contract, "graphforge-portable-export/1");
  assert.equal(exported.source, "current");
  assert.equal(exported.checkpoint, null);
  const secondExport = success(
    "export",
    "--current",
    "--output",
    secondEnvelope,
  );
  assert.equal(secondExport.envelope_sha256, exported.envelope_sha256);
  assert.deepEqual(readFileSync(secondEnvelope), readFileSync(envelope));

  successAt(destination, "init", "--no-skills");
  const imported = successAt(
    destination,
    "import",
    "--input",
    envelope,
    "--idempotency-key",
    importOperation,
  );
  assert.equal(imported.contract, "graphforge-portable-import/1");
  assert.equal(imported.source_generation_uuid, exported.generation_uuid);
  assert.equal(imported.envelope_sha256, exported.envelope_sha256);
  assert.equal(imported.idempotent_replay, false);
  assert.equal(
    successAt(destination, "checkpoint", "list").contract,
    "graphforge-cli-result/1",
  );
  assert.equal(
    existsSync(join(destination, ".graphforge", "state", "CURRENT")),
    true,
  );
  cover("portable_export_import_and_reopen");

  const packaged = join(installedCliRoot, "project-skills");
  const managed = join(repository, ".agents", "skills");
  for (const skill of ["graphforge-bootstrap", "graphforge-build-knowledge"]) {
    assert.deepEqual(
      readFileSync(join(managed, skill, "SKILL.md")),
      readFileSync(join(packaged, skill, "SKILL.md")),
    );
  }
  assert.equal(success("skills", "status").status, "current");
  assert.equal(success("skills", "install").changed, false);

  // Recover an interrupted publication before reporting status.
  const lifecycle = join(
    repository,
    ".graphforge",
    "imports",
    "skills-lifecycle",
  );
  const backup = join(lifecycle, "backup");
  mkdirSync(backup);
  for (const skill of ["graphforge-bootstrap", "graphforge-build-knowledge"]) {
    renameSync(join(managed, skill), join(backup, skill));
    mkdirSync(join(managed, skill), { recursive: true });
    writeFileSync(join(managed, skill, "SKILL.md"), "interrupted\n");
  }
  renameSync(
    join(managed, ".graphforge-managed.json"),
    join(backup, ".graphforge-managed.json"),
  );
  writeFileSync(join(lifecycle, "transaction"), "graphforge-skills/1\n");
  assert.equal(success("skills", "status").status, "current");
  assert.equal(existsSync(join(lifecycle, "transaction")), false);

  const edited = join(managed, "graphforge-bootstrap", "SKILL.md");
  appendFileSync(edited, "\nuser edit\n");
  const beforeConflict = readFileSync(edited);
  assert.equal(success("skills", "status").status, "conflict");
  const updateConflict = run("skills", "update");
  assert.equal(updateConflict.status, 2);
  assert.deepEqual(readFileSync(edited), beforeConflict);
  assert.equal(success("skills", "update", "--force").changed, true);

  // Corrupting the installed npm asset must fail bundle validation. This proves
  // the package copy, rather than a separately embedded checkout copy, is used.
  const packagedManifest = join(packaged, "manifest.json");
  const manifestBytes = readFileSync(packagedManifest);
  writeFileSync(packagedManifest, "{}\n");
  assert.notEqual(run("skills", "status").status, 0);
  writeFileSync(packagedManifest, manifestBytes);

  appendFileSync(edited, "\nsecond user edit\n");
  assert.equal(run("skills", "remove").status, 2);
  assert.equal(existsSync(edited), true);
  assert.equal(success("skills", "remove", "--force").changed, true);
  assert.equal(existsSync(join(managed, "graphforge-bootstrap")), false);
  assert.equal(success("skills", "install").changed, true);

  const beforeRemove = currentBytes();
  const removeRefusal = failure(2, "remove");
  assert.equal(removeRefusal.error.code, "GF_VALIDATION");
  assert.deepEqual(currentBytes(), beforeRemove);
  const removed = success("remove", "--yes");
  assert.equal(removed.target, ".graphforge/state");
  assert.equal(removed.removed, true);
  assert.equal(existsSync(join(repository, ".graphforge", "state")), false);
  for (const preserved of [
    ".graphforge/graphforge.yaml",
    ".graphforge/ontology/keep.yaml",
    ".graphforge/schemas/keep.json",
    ".graphforge/migrations/001.yaml",
    ".graphforge/seeds/recipe.yaml",
    ".graphforge/imports/source.arrow",
    ".graphforge/exports/current.gfportable",
    ".agents/skills/graphforge-bootstrap/SKILL.md",
  ]) {
    assert.equal(
      existsSync(join(repository, preserved)),
      true,
      `remove deleted ${preserved}`,
    );
  }
  cover("remove_refusal_and_confirmation");

  assert.deepEqual(
    [...coveredScenarios].sort(),
    [...requiredScenarios].sort(),
    "packed npm lifecycle did not cover every shared required scenario",
  );
  for (const forbidden of lifecycleContract.scope
    .forbiddenImplicitOntologyOperations) {
    assert.equal(
      invocations.some((args) => args.includes(forbidden)),
      false,
      `repository lifecycle invoked ontology operation ${forbidden}`,
    );
  }

  process.stdout.write(
    `native offline npx lifecycle: ${coveredScenarios.size} scenarios verified\n`,
  );
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
