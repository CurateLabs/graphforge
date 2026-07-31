import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { GraphForge } from "../index.js";

const root = fileURLToPath(new URL("../../..", import.meta.url));
const expectedPath = join(
  root,
  "tests",
  "contracts",
  "ontology-lifecycle-draft.json",
);
const expectedYamlPath = join(
  root,
  "tests",
  "contracts",
  "ontology-lifecycle-draft.yaml",
);
const expected = JSON.parse(readFileSync(expectedPath, "utf8"));
const expectedBytes = Buffer.from(
  readFileSync(expectedPath, "utf8").replace(/\n$/, ""),
);
const adoptOperation = "018f5f0d-65dd-7a88-b6ef-0123456789ab";
const clearOperation = "018f5f0d-65dd-7a88-b6ef-0123456789ac";

test("Rust-owned ontology lifecycle is deterministic and durable", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (:Person {name: 'Alice'})");
  const catalog = forge.inspectRuntimeCatalog();
  assert.equal(catalog.contractVersion, 1);
  assert.deepEqual(
    catalog.entries.map(({ kind, name }) => [kind, name]),
    [["entity_type", "Person"]],
  );

  const suggestion = forge.suggestOntology("binding-parity", "1.0.0");
  assert.equal(suggestion.draft, true);
  assert.deepEqual(suggestion.document, expected);
  assert.deepEqual(suggestion.omittedRelationTypes, []);
  assert.deepEqual(forge.validateOntology(expected), {
    valid: true,
    diagnostics: [],
  });

  const directory = mkdtempSync(join(tmpdir(), "gf-ontology-node-"));
  try {
    const exported = join(directory, "suggested.json");
    forge.exportOntology("suggested", exported, "json", expected);
    assert.deepEqual(readFileSync(exported), expectedBytes);
    const exportedYaml = join(directory, "suggested.yaml");
    forge.exportOntology("suggested", exportedYaml, "yaml", expected);
    assert.deepEqual(
      readFileSync(exportedYaml),
      readFileSync(expectedYamlPath),
    );

    assert.throws(
      () => forge.exportOntology("invalid", exported, "json"),
      (error) => error.code === "GF_VALIDATION",
    );
    assert.throws(
      () => forge.exportOntology("suggested", exported, "xml", expected),
      (error) => error.code === "GF_VALIDATION",
    );
    assert.throws(
      () =>
        forge.exportOntology(
          "suggested",
          join(directory, "missing", "out.json"),
          "json",
          expected,
        ),
      (error) => error.code === "GF_IO",
    );

    const project = join(directory, "project");
    mkdirSync(project);
    const session = new GraphForge(project);
    session.loadOntology(expectedPath);
    assert.equal(session.ontologyMode, "advisory");
    session.close();
    const reopened = new GraphForge(project);
    assert.equal(reopened.ontologyMode, "exploratory");

    reopened.adoptOntology(expectedPath, "strict", adoptOperation);
    reopened.adoptOntology(expectedPath, "strict", adoptOperation);
    assert.equal(reopened.workspaceOntology().mode, "strict");
    assert.throws(
      () => reopened.adoptOntology(expectedPath, "advisory", adoptOperation),
      (error) => error.code === "GF_IDEMPOTENCY_CONFLICT",
    );
    assert.throws(
      () =>
        reopened.adoptOntology(
          expectedPath,
          "exploratory",
          "018f5f0d-65dd-7a88-b6ef-0123456789ad",
        ),
      (error) => error.code === "GF_VALIDATION",
    );
    reopened.close();

    const adopted = new GraphForge(project);
    assert.equal(adopted.ontologyMode, "strict");
    const adoptedExport = join(directory, "adopted.json");
    adopted.exportOntology("adopted", adoptedExport, "json");
    assert.deepEqual(readFileSync(adoptedExport), expectedBytes);
    adopted.clearOntology(clearOperation);
    adopted.clearOntology(clearOperation);
    assert.equal(adopted.workspaceOntology().mode, "none");
    adopted.close();

    const cleared = new GraphForge(project);
    assert.equal(cleared.ontologyMode, "exploratory");
    assert.equal(cleared.workspaceOntology().canonicalOntology, undefined);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
