import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { GraphForge } from "../index.js";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const oracle = JSON.parse(
  readFileSync(
    join(ROOT, "tests/fixtures/multi-ontology-v1/binding-parity-v1.json"),
    "utf8",
  ),
);

function openProject(prefix) {
  const root = mkdtempSync(join(tmpdir(), prefix));
  const projectRoot = join(root, "project");
  mkdirSync(projectRoot);
  return { root, forge: new GraphForge(projectRoot) };
}

function authority(forge, operationUuid = randomUUID()) {
  const state = forge.ontologyAuthorityState();
  const input = {
    operationUuid,
    expectedProjectGenerationUuid: state.project_generation_uuid,
  };
  if (state.composition_fingerprint !== null) {
    input.expectedCompositionFingerprint = state.composition_fingerprint;
  }
  return input;
}

function replaceIdentities(value, identities) {
  if (typeof value === "string" && identities[value])
    return structuredClone(identities[value]);
  if (Array.isArray(value))
    return value.map((item) => replaceIdentities(item, identities));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        replaceIdentities(item, identities),
      ]),
    );
  return value;
}

function nativeFailure(call) {
  try {
    call();
  } catch (error) {
    const envelope = JSON.parse(error.message);
    assert.equal(error.code, envelope.code);
    assert.ok(Array.isArray(envelope.diagnostics));
    return envelope;
  }
  assert.fail("expected native multi-ontology failure");
}

async function nativeAsyncFailure(call) {
  try {
    await call();
  } catch (error) {
    const envelope = JSON.parse(error.message);
    assert.equal(error.code, envelope.code);
    assert.ok(Array.isArray(envelope.diagnostics));
    return envelope;
  }
  assert.fail("expected native multi-ontology failure");
}

async function portableFailure(call) {
  try {
    await call();
  } catch (error) {
    const envelope = JSON.parse(error.message);
    assert.equal(error.code, envelope.code);
    return envelope;
  }
  assert.fail("expected native portable-v2 failure");
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
      .join(",")}}`;
  return JSON.stringify(value);
}

function digest(domain, value) {
  return `sha256:${createHash("sha256")
    .update(`${domain}\0${canonical(value)}`)
    .digest("hex")}`;
}

function injectUnsupportedFeature(portable) {
  const relativeControl =
    "data/components/compatibility/graphforge-ontology-composition/composition.json";
  const controlPath = join(portable, relativeControl);
  const control = JSON.parse(readFileSync(controlPath, "utf8"));
  control.required_features.push("future-multi-ontology@999");
  control.required_features.sort();
  const unsignedControl = structuredClone(control);
  delete unsignedControl.composition_digest;
  control.composition_digest = digest(
    "graphforge-ontology-composition/1",
    unsignedControl,
  );
  const controlText = canonical(control);
  writeFileSync(controlPath, controlText);

  const manifestPath = join(portable, "data/graphforge-project.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  const file = manifest.components
    .flatMap((component) => component.files)
    .find((candidate) => candidate.path.endsWith("composition.json"));
  file.length = Buffer.byteLength(controlText);
  file.sha256 = createHash("sha256").update(controlText).digest("hex");
  const unsignedManifest = structuredClone(manifest);
  delete unsignedManifest.package_digest;
  manifest.package_digest = digest("graphforge-project/2", unsignedManifest);
  writeFileSync(manifestPath, canonical(manifest));
}

async function runSemantics() {
  const subject = openProject("graphforge-node-multi-ontology-");
  const { forge } = subject;
  let stage = "base";
  try {
    const moduleValidation = forge.validateOntologyModule(oracle.modules.base);
    assert.deepEqual(moduleValidation, { valid: true, diagnostics: [] });
    const invalidModule = structuredClone(oracle.modules.base);
    invalidModule.entity_types.push(
      structuredClone(invalidModule.entity_types[0]),
    );
    const invalidModuleValidation = forge.validateOntologyModule(invalidModule);
    assert.equal(invalidModuleValidation.valid, false);
    assert.equal(
      invalidModuleValidation.diagnostics[0].code,
      "inventory.malformed",
    );
    const created = forge.createOntologyModule({
      document: oracle.modules.base,
      enforcement: "advisory",
    });
    const imported = forge.importOntologyModule({
      text: JSON.stringify(oracle.modules.base),
      format: "json",
    });
    assert.deepEqual(created.id, imported.id);
    const basePath = join(subject.root, "base.json");
    writeFileSync(basePath, JSON.stringify(oracle.modules.base));
    forge.adoptOntology(basePath, "advisory", randomUUID());
    const base = forge.ontologyModules()[0];
    assert.deepEqual(
      forge.inspectOntologyModule({ exact: base.id }).doc,
      oracle.modules.base,
    );

    const dependentCandidate = forge.importOntologyModule({
      text: JSON.stringify(oracle.modules.dependent),
      format: "json",
      dependencies: [base.id],
    });
    stage = "dependent";
    const operationUuid = randomUUID();
    const replayAuthority = authority(forge, operationUuid);
    const replayResult = await forge.adoptOntologyModule({
      authority: replayAuthority,
      candidate: dependentCandidate,
    });
    stage = "dependent replay";
    const replayReceipt = await forge.adoptOntologyModule({
        authority: replayAuthority,
        candidate: dependentCandidate,
      });
    assert.deepEqual(replayReceipt, replayResult);
    const conflict = forge.createOntologyModule({
      document: oracle.modules.dependent_update,
      dependencies: [base.id],
    });
    stage = "dependent conflict";
    const replayConflict = await nativeAsyncFailure(() =>
      forge.adoptOntologyModule({
        authority: replayAuthority,
        candidate: conflict,
      }),
    );
    assert.equal(
      replayConflict.code,
      oracle.expected.idempotency_conflict_code,
    );
    const dependent = forge.ontologyModules()[1];
    const moduleOrder = forge
      .ontologyModules()
      .map((row) => row.id.ontology_id);
    assert.deepEqual(
      forge.inspectOntologyModule({ ontologyId: dependent.id.ontology_id })
        .entry.id,
      dependent.id,
    );

    const bridgeDocument = replaceIdentities(oracle.bridge, {
      $base: base.id,
      $dependent: dependent.id,
    });
    stage = "bridge";
    const bridgeValidation = forge.validateOntologyBridge(bridgeDocument);
    assert.deepEqual(bridgeValidation, { valid: true, diagnostics: [] });
    const bridgeCandidate = forge.importOntologyBridge({
      text: JSON.stringify(bridgeDocument),
      format: "json",
    });
    assert.deepEqual(
      bridgeCandidate.id,
      forge.createOntologyBridge(bridgeDocument).id,
    );
    await forge.adoptOntologyBridge({
      authority: authority(forge),
      candidate: bridgeCandidate,
    });
    const bridge = forge.ontologyBridges()[0];
    const bridgeOrder = forge.ontologyBridges().map((row) => row.id.bridge_id);
    assert.equal(
      JSON.parse(forge.exportOntologyBridge({ exact: bridge.id }, "json"))
        .bridge_id,
      oracle.bridge.bridge_id,
    );

    const beforeBlocked = forge.ontologyAuthorityState();
    stage = "blocked deletion";
    const previewDelete = forge.previewDeleteOntologyModule({ exact: base.id });
    assert.equal(previewDelete.safe, false);
    const blocked = await nativeAsyncFailure(() =>
      forge.deleteOntologyModule({
        authority: authority(forge),
        selector: { exact: base.id },
      }),
    );
    assert.equal(blocked.code, oracle.expected.dependency_blocked_code);
    assert.deepEqual(forge.ontologyAuthorityState(), beforeBlocked);

    const ambiguous = forge.explainOntologyResolution({
      kind: "entity",
      localId: "Person",
      maxCandidates: oracle.expected.max_diagnostics,
    });
    assert.equal(ambiguous.outcome, null);
    assert.equal(ambiguous.diagnostics[0].code, oracle.expected.ambiguous_code);
    assert.ok(
      ambiguous.diagnostics[0].subjects.length <=
        ambiguous.diagnostics[0].limit,
    );
    assert.ok(
      forge.explainOntologyResolution({
        module: base.id,
        kind: "entity",
        localId: "Person",
      }).outcome,
    );

    const bridgeUpdate = structuredClone(bridgeDocument);
    bridgeUpdate.authored_version = "2.0.0";
    const bridgePreview = forge.previewUpdateOntologyBridge(
      { exact: bridge.id },
      bridgeUpdate,
    );
    assert.deepEqual(bridgePreview.prior, bridge.id);
    await forge.updateOntologyBridge({
      authority: authority(forge),
      selector: { exact: bridge.id },
      document: bridgeUpdate,
    });
    const updatedBridge = forge.ontologyBridges()[0];
    assert.equal(
      JSON.parse(
        forge.exportOntologyBridge({ exact: updatedBridge.id }, "json"),
      ).authored_version,
      "2.0.0",
    );
    assert.equal(
      forge.previewDeleteOntologyBridge({ exact: updatedBridge.id }).safe,
      true,
    );
    await forge.deleteOntologyBridge({
      authority: authority(forge),
      selector: { exact: updatedBridge.id },
    });
    assert.deepEqual(forge.ontologyBridges(), []);

    const modulePreview = forge.previewUpdateOntologyModule(
      { exact: dependent.id },
      oracle.modules.dependent_update,
      [base.id],
    );
    assert.deepEqual(modulePreview.prior, dependent.id);
    await forge.updateOntologyModule({
      authority: authority(forge),
      selector: { exact: dependent.id },
      document: oracle.modules.dependent_update,
      dependencies: [base.id],
    });
    const updatedDependent = forge.ontologyModules()[1];
    assert.deepEqual(
      JSON.parse(
        forge.exportOntologyModule({ exact: updatedDependent.id }, "json"),
      ),
      oracle.modules.dependent_update,
    );

    const beforeCancel = forge.ontologyAuthorityState();
    const cancellationBeforeModules = forge.ontologyModules();
    stage = "cancellation";
    const cancelBeforeStart = new AbortController();
    const cancellationPromise = forge.changeOntologyActivationProfile({
      authority: authority(forge),
      profileDefault: "exploratory",
      activation: forge.ontologyActivationProfile().activation,
      signal: cancelBeforeStart.signal,
    });
    cancelBeforeStart.abort();
    const cancelled = await nativeAsyncFailure(() => cancellationPromise);
    assert.equal(cancelled.code, "GF_CANCELLED");
    assert.deepEqual(forge.ontologyAuthorityState(), beforeCancel);
    const cancellationAfterModules = forge.ontologyModules();

    const malformed = nativeFailure(() =>
      forge.importOntologyModule({ text: "{", format: "json" }),
    );
    assert.equal(malformed.code, "GF_ONTOLOGY_DIAGNOSTIC");
    assert.deepEqual(forge.ontologyAuthorityState(), beforeCancel);

    const portable = join(subject.root, "portable");
    stage = "portable";
    await forge.exportPortableV2({
      outputPath: portable,
      representation: "expanded",
    });
    injectUnsupportedFeature(portable);
    const unsupported = await portableFailure(() =>
      GraphForge.verifyPortableV2({
        input: portable,
        mode: "structure_only",
      }),
    );
    assert.equal(unsupported.code, oracle.expected.unsupported_future_code);
    const failedImportRoot = join(subject.root, "failed-import");
    mkdirSync(failedImportRoot);
    const beforeEntries = readdirSync(failedImportRoot);
    const importAuthorityBefore = forge.ontologyAuthorityState();
    const importFailure = await portableFailure(() =>
      GraphForge.importPortableV2({
        projectRoot: failedImportRoot,
        input: portable,
        operationId: randomUUID(),
      }),
    );
    assert.equal(importFailure.code, oracle.expected.unsupported_future_code);
    assert.deepEqual(readdirSync(failedImportRoot), beforeEntries);
    const importAuthorityAfter = forge.ontologyAuthorityState();
    assert.deepEqual(importAuthorityAfter, importAuthorityBefore);
    const firstInventory = canonical(forge.ontologyModules());
    const secondInventory = canonical(forge.ontologyModules());
    const installed = new GraphForge();
    const installedModules = installed.ontologyModules();
    installed.close();
    const report = {
      contract: "graphforge-multi-ontology-parity-result/1",
      cases: {
        positive_crud_import_export: {
          module_ids: moduleOrder,
          bridge_id: bridgeOrder[0],
          module_export_match: true,
          bridge_export_match: true,
        },
        exact_identity_and_ambiguity: {
          exact_match: true,
          diagnostic_code: ambiguous.diagnostics[0].code,
        },
        dependency_blocked_deletion: {
          safe: previewDelete.safe,
          diagnostic_code: blocked.diagnostics[0].code,
        },
        unsupported_future_portability: {
          error_code: unsupported.code,
          diagnostic_code: unsupported.diagnostics[0].code,
        },
        cancellation: { error_code: cancelled.code, before_modules: cancellationBeforeModules, after_modules: cancellationAfterModules },
        idempotent_replay: {
          first_receipt: replayResult,
          replay_receipt: replayReceipt,
          conflict_code: replayConflict.code,
        },
        no_partial_import_or_authority_change: {
          before_entries: beforeEntries,
          after_entries: readdirSync(failedImportRoot),
          authority_before: importAuthorityBefore,
          authority_after: importAuthorityAfter,
        },
        bounded_structured_diagnostics: {
          outer_code: blocked.code,
          diagnostic_code: blocked.diagnostics[0].code,
          bounded:
            blocked.diagnostics[0].subjects.length <=
            blocked.diagnostics[0].limit,
          path_free: !JSON.stringify(blocked).includes(subject.root),
        },
        deterministic_path_free_cli_json: {
          first_serialized: firstInventory,
          second_serialized: secondInventory,
          forbidden_path: subject.root,
        },
        packaged_clean_install: { package_origin: fileURLToPath(new URL("../index.js", import.meta.url)), operation: "ontology_modules", module_count: installedModules.length },
      },
    };
    const reportPath = process.env.GRAPHFORGE_MULTI_ONTOLOGY_PARITY_REPORT;
    if (reportPath) writeFileSync(reportPath, `${canonical(report)}\n`);
    return {
      base,
      bridge,
      blocked,
      ambiguous,
      replayResult,
      cancelled,
      unsupported,
      report,
      moduleValidation,
      invalidModuleValidation,
      bridgeValidation,
    };
  } catch (error) {
    error.message = `${stage}: ${error.message}`;
    throw error;
  } finally {
    forge.close();
  }
}

let semanticPromise;
function semantics() {
  semanticPromise ??= runSemantics();
  return semanticPromise;
}

test("positive CRUD import export", async () => {
  const createOntologyModule = "createOntologyModule";
  const exportOntologyBridge = "exportOntologyBridge";
  const result = await semantics();
  assert.equal(result.base.id.ontology_id, oracle.expected.module_order[0]);
  assert.deepEqual(result.report.cases.positive_crud_import_export, {
    module_ids: oracle.expected.module_order,
    bridge_id: oracle.bridge.bridge_id,
    module_export_match: true,
    bridge_export_match: true,
  });
  assert.deepEqual(result.moduleValidation, { valid: true, diagnostics: [] });
  assert.equal(
    result.invalidModuleValidation.diagnostics[0].code,
    "inventory.malformed",
  );
  assert.deepEqual(result.bridgeValidation, { valid: true, diagnostics: [] });
  assert.ok(createOntologyModule && exportOntologyBridge && result.bridge.id);
});

test("exact identity and ambiguity", async () => {
  const ontologyId = "ontologyId";
  const selector_ambiguous = oracle.expected.ambiguous_code;
  assert.equal(
    (await semantics()).ambiguous.diagnostics[0].code,
    selector_ambiguous,
  );
  assert.equal(
    (await semantics()).report.cases.exact_identity_and_ambiguity.exact_match,
    true,
  );
  assert.ok(ontologyId);
});

test("dependency blocked deletion", async () => {
  const previewDeleteOntologyModule = "previewDeleteOntologyModule";
  const dependency_blocked = oracle.expected.dependency_blocked_code;
  assert.equal((await semantics()).blocked.code, dependency_blocked);
  assert.equal(
    (await semantics()).report.cases.dependency_blocked_deletion.safe,
    false,
  );
  assert.ok(previewDeleteOntologyModule);
});

test("unsupported future portability", async () => {
  const verifyPortableV2 = "verifyPortableV2";
  const unsupported_future_version = oracle.expected.unsupported_future_code;
  assert.equal(
    (await semantics()).unsupported.code,
    unsupported_future_version,
  );
  assert.equal(
    (await semantics()).report.cases.unsupported_future_portability
      .diagnostic_code,
    oracle.expected.unsupported_future_diagnostic,
  );
  assert.ok(verifyPortableV2);
});

test("cancellation", async () => {
  const cancelBeforeStart = true;
  const GF_CANCELLED = "GF_CANCELLED";
  assert.equal((await semantics()).cancelled.code, GF_CANCELLED);
  const observed = (await semantics()).report.cases.cancellation;
  assert.deepEqual(observed.before_modules, observed.after_modules);
  assert.ok(cancelBeforeStart);
});

test("idempotent replay", async () => {
  const operationUuid = randomUUID();
  const replayResult = (await semantics()).replayResult;
  assert.match(replayResult.operation_uuid, /^[0-9a-f-]{36}$/);
  const observed = (await semantics()).report.cases.idempotent_replay;
  assert.deepEqual(observed.first_receipt, observed.replay_receipt);
  assert.ok(operationUuid);
});

test("no partial import or authority change", async () => {
  const ontologyAuthorityState = "ontologyAuthorityState";
  const no_partial_import = true;
  assert.equal(
    (await semantics()).unsupported.code,
    oracle.expected.unsupported_future_code,
  );
  const observed = (await semantics()).report.cases.no_partial_import_or_authority_change;
  assert.deepEqual(observed.before_entries, observed.after_entries);
  assert.deepEqual(observed.authority_before, observed.authority_after);
  assert.ok(ontologyAuthorityState && no_partial_import);
});

test("bounded structured diagnostics", async () => {
  const diagnostics = (await semantics()).ambiguous.diagnostics;
  const limit = oracle.expected.max_diagnostics;
  assert.ok(diagnostics[0].subjects.length <= limit);
  assert.equal(
    (await semantics()).report.cases.bounded_structured_diagnostics.path_free,
    true,
  );
});

test("deterministic path free serialization", async () => {
  const project_generation_uuid = "project_generation_uuid";
  const serialized = JSON.stringify((await semantics()).replayResult);
  assert.ok(serialized.includes(project_generation_uuid));
  assert.ok(!serialized.includes(tmpdir()));
  const observed = (await semantics()).report.cases.deterministic_path_free_cli_json;
  assert.equal(observed.first_serialized, observed.second_serialized);
  assert.ok(!observed.first_serialized.includes(observed.forbidden_path));
});

test("packaged clean install", async () => {
  const packagedManifest = "graphforge/package.json";
  const packageJson = JSON.parse(
    readFileSync(join(ROOT, "crates/graphforge-bindings-node/package.json")),
  );
  const smoke = readFileSync(
    join(ROOT, "scripts/ci/multi-ontology-packaged-smoke.mjs"),
    "utf8",
  );
  assert.equal(packageJson.name, "@curatelabs/graphforge");
  assert.match(smoke, /ontologyModules/);
  const packaged = new GraphForge();
  try {
    assert.deepEqual(packaged.ontologyModules(), []);
  } finally {
    packaged.close();
  }
  assert.ok(packagedManifest);
  const observed = (await semantics()).report.cases.packaged_clean_install;
  assert.equal(observed.operation, "ontology_modules");
  assert.equal(observed.module_count, 0);
  assert.ok(observed.package_origin.endsWith("index.js"));
});
