// Fresh-addon acceptance for canonical M18 embedding publication.

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  Field,
  FixedSizeBinary,
  FixedSizeList,
  Float32,
  List,
  RecordBatchStreamWriter,
  Schema,
  Table,
  tableFromIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const vectorType = (dimensions = 2) =>
  new FixedSizeList(dimensions, new Field("item", new Float32(), false));

function schema(
  algorithm = "node2vec",
  algorithmVersion = "node2vec-v1",
  dimensions = 2,
  seed = 0,
) {
  return new Schema(
    [
      new Field("node_uuid", new FixedSizeBinary(16), false),
      new Field("embedding", vectorType(dimensions), false),
    ],
    new Map([
      ["graphforge.algorithm", algorithm],
      ["graphforge.verb", "analyze"],
      ["graphforge.algorithm_version", algorithmVersion],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.dimensions", String(dimensions)],
      ["graphforge.seed", String(seed)],
      ["graphforge.rng_version", "splitmix64-v1"],
      ["graphforge.rng_derivation", "graphforge-embedding-substream-v1"],
    ]),
  );
}

function batch(arrowSchema, rows) {
  if (rows.length === 0) {
    return batch(arrowSchema, [
      [
        "00000000-0000-0000-0000-000000000000",
        Array(arrowSchema.fields[1].type.listSize).fill(0),
      ],
    ]).slice(0, 0);
  }
  return new Table(arrowSchema, {
    node_uuid: vectorFromArray(
      rows.map(([uuid]) => Buffer.from(uuid.replaceAll("-", ""), "hex")),
      new FixedSizeBinary(16),
    ),
    embedding: vectorFromArray(
      rows.map(([, vector]) => vector),
      arrowSchema.fields[1].type,
    ),
  }).batches[0];
}

function ipc(arrowSchema, rowGroups) {
  return Buffer.from(
    RecordBatchStreamWriter.writeAll(
      rowGroups.map((rows) => batch(arrowSchema, rows)),
    ).toUint8Array(true),
  );
}

function options(overrides = {}) {
  return {
    algorithm: "node2vec",
    algorithmVersion: "node2vec-v1",
    dimensions: 2,
    hyperparameters: { walks: 8, nested: [true] },
    inputRecipe: { recipe: "m18_nodes_v1" },
    sourceProjection: { label: "Person", recipe: "all_people_v1" },
    ...overrides,
  };
}

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

test("canonical M18 IPC publishes, replaces, searches, and reopens", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-m18-publication-"));
  const forge = new GraphForge(project);
  try {
    const alice = forge.addNode("Person", { name: "Alice" });
    const bob = forge.addNode("Person", { name: "Bob" });
    const canonical = schema();
    const result = ipc(canonical, [
      [[alice.uuid, [1, 0]]],
      [[bob.uuid, [0, 1]]],
    ]);

    const identity = forge.publishM18Embeddings(
      "structural",
      result,
      options(),
    );
    assert.equal(identity.length, 64);
    assert.equal(
      forge.publishM18Embeddings("structural", result, options()),
      identity,
    );
    assert.deepEqual(forge.embeddingSpace("structural").producer, {
      kind: "m18",
      algorithm: "node2vec",
      algorithmVersion: "node2vec-v1",
    });

    const found = tableFromIPC(
      forge.find(
        undefined,
        "Person",
        [1, 0],
        undefined,
        undefined,
        2,
        "structural",
      ),
    );
    assert.deepEqual(Array.from(found.getChild("node_uuid"), uuidHex), [
      alice.uuid.replaceAll("-", ""),
      bob.uuid.replaceAll("-", ""),
    ]);
    for (const knowledge of [
      "confidence",
      "provenance_id",
      "assertion_uuid",
      "belief_status",
      "valid_time",
    ]) {
      assert.equal(found.getChild(knowledge), null);
    }

    expectValidation("non-canonical algorithm metadata", () =>
      forge.publishM18Embeddings(
        "structural",
        result,
        options({ algorithmVersion: "node2vec-v2" }),
      ),
    );
    const replacementSchema = schema("node2vec", "node2vec-v2");
    const replacementResult = ipc(replacementSchema, [
      [
        [alice.uuid, [1, 0]],
        [bob.uuid, [0, 1]],
      ],
    ]);
    const replaced = forge.publishM18Embeddings(
      "structural",
      replacementResult,
      options({ algorithmVersion: "node2vec-v2", replace: true }),
    );
    assert.notEqual(replaced, identity);

    expectValidation("requires an embedding analysis algorithm", () =>
      forge.publishM18Embeddings(
        "unsupported",
        ipc(schema("is_dag", "not-an-embedding-v1"), [[[alice.uuid, [1, 0]]]]),
        options({
          algorithm: "is_dag",
          algorithmVersion: "not-an-embedding-v1",
        }),
      ),
    );
    expectValidation("duplicate", () =>
      forge.publishM18Embeddings(
        "duplicate",
        ipc(canonical, [
          [
            [alice.uuid, [1, 0]],
            [alice.uuid, [0, 1]],
          ],
        ]),
        options(),
      ),
    );
    const variableSchema = new Schema(
      [
        new Field("node_uuid", new FixedSizeBinary(16), false),
        new Field(
          "embedding",
          new List(new Field("item", new Float32(), false)),
          false,
        ),
      ],
      canonical.metadata,
    );
    expectValidation("exact node_uuid and embedding fields", () =>
      forge.publishM18Embeddings(
        "variable-list",
        ipc(variableSchema, [[[alice.uuid, [1, 0]]]]),
        options(),
      ),
    );
    const missingMetadata = new Map(canonical.metadata);
    missingMetadata.delete("graphforge.rng_derivation");
    expectValidation("non-canonical algorithm metadata", () =>
      forge.publishM18Embeddings(
        "missing-metadata",
        ipc(new Schema(canonical.fields, missingMetadata), [
          [[alice.uuid, [1, 0]]],
        ]),
        options(),
      ),
    );
    const extraMetadata = new Map(canonical.metadata);
    extraMetadata.set("graphforge.extra", "forbidden");
    expectValidation("non-canonical algorithm metadata", () =>
      forge.publishM18Embeddings(
        "extra-metadata",
        ipc(new Schema(canonical.fields, extraMetadata), [
          [[alice.uuid, [1, 0]]],
        ]),
        options(),
      ),
    );
    expectValidation("non-zero", () =>
      forge.publishM18Embeddings(
        "zero",
        ipc(canonical, [[[alice.uuid, [0, 0]]]]),
        options(),
      ),
    );
    expectValidation("finite", () =>
      forge.publishM18Embeddings(
        "non-finite",
        ipc(canonical, [[[alice.uuid, [Number.NaN, 1]]]]),
        options(),
      ),
    );
    expectValidation("exact node_uuid and embedding fields", () =>
      forge.publishM18Embeddings(
        "dimension",
        ipc(canonical, [[[alice.uuid, [1, 0]]]]),
        options({ dimensions: 3 }),
      ),
    );
    expectValidation("input recipe", () =>
      forge.publishM18Embeddings(
        "recipe",
        result,
        options({ inputRecipe: {} }),
      ),
    );
    expectValidation("normalization", () =>
      forge.publishM18Embeddings(
        "normalization",
        result,
        options({ normalization: "unit-ish" }),
      ),
    );
    expectValidation("Arrow IPC", () =>
      forge.publishM18Embeddings(
        "invalid-ipc",
        Buffer.from("not arrow"),
        options(),
      ),
    );

    const foreignProject = mkdtempSync(join(tmpdir(), "gf-node-m18-foreign-"));
    const foreignForge = new GraphForge(foreignProject);
    try {
      const foreign = foreignForge.addNode("Person", { name: "Mallory" });
      expectValidation("matched no nodes", () =>
        forge.publishM18Embeddings(
          "foreign",
          ipc(canonical, [[[foreign.uuid, [1, 0]]]]),
          options(),
        ),
      );
    } finally {
      foreignForge.close();
      rmSync(foreignProject, { recursive: true, force: true });
    }

    const empty = forge.publishM18Embeddings(
      "empty",
      ipc(schema("node2vec", "node2vec-v1", 3), [[]]),
      options({ dimensions: 3, sourceProjection: { label: "Nobody" } }),
    );
    assert.equal(empty.length, 64);
    forge.close();

    const reopened = new GraphForge(project);
    try {
      const persisted = tableFromIPC(
        reopened.find(
          undefined,
          "Person",
          [0, 1],
          undefined,
          undefined,
          2,
          "structural",
        ),
      );
      assert.equal(
        uuidHex(persisted.getChild("node_uuid").get(0)),
        bob.uuid.replaceAll("-", ""),
      );
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
