// Fresh-addon acceptance for thin Node embedding option construction.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function expectError(code, fragment, call) {
  assert.throws(
    call,
    (error) => error.code === code && error.message.includes(fragment),
  );
}

function analyze(forge, by, options = {}, invocation = {}) {
  return forge.analyze(
    by,
    "Person",
    invocation.via,
    invocation.directed,
    invocation.weight,
    invocation.partitionProperty,
    invocation.k,
    options,
  );
}

function uuidHex(value) {
  return Buffer.from(value).toString("hex");
}

test("constructs typed embedding variants and activates available kernels", () => {
  const forge = new GraphForge();
  const node2vec = tableFromIPC(analyze(forge, "node2vec"));
  assert.equal(node2vec.numRows, 0);
  assert.deepEqual(
    node2vec.schema.fields.map((field) => field.name),
    ["node_uuid", "embedding"],
  );

  const fastrp = tableFromIPC(analyze(forge, "fast_random_projection"));
  assert.equal(fastrp.numRows, 0);
  assert.equal(
    fastrp.schema.metadata.get("graphforge.algorithm"),
    "fast_random_projection",
  );
  const hashgnn = tableFromIPC(analyze(forge, "hashgnn"));
  assert.equal(hashgnn.numRows, 0);
  assert.equal(hashgnn.schema.metadata.get("graphforge.algorithm"), "hashgnn");
  const graphsage = tableFromIPC(
    analyze(forge, "graphsage", { feature_properties: ["age"] }),
  );
  assert.equal(graphsage.numRows, 0);
  assert.equal(
    graphsage.schema.metadata.get("graphforge.algorithm"),
    "graphsage",
  );
  assert.equal(
    tableFromIPC(
      analyze(forge, "node2vec", {}, { via: "KNOWS", weight: "strength" }),
    ).numRows,
    0,
  );

  const explicit = {
    node2vec: {
      dimensions: 64,
      walk_length: 12,
      walks_per_node: 4,
      p: 0.5,
      q: 2,
      window_size: 3,
      negative_samples: 2,
      epochs: 2,
      learning_rate: 0.01,
      seed: 7,
    },
    graphsage: {
      dimensions: 96,
      hidden_dimensions: 48,
      layers: 1,
      sample_sizes: [8],
      aggregator: "mean",
      epochs: 2,
      negative_samples: 4,
      learning_rate: 0.001,
      feature_properties: ["age", "score"],
      seed: 8,
    },
    fast_random_projection: {
      dimensions: 80,
      iteration_weights: [0, 0.5, 1],
      normalization_strength: 0,
      feature_weight: 0.25,
      feature_properties: ["age"],
      seed: 9,
    },
    hashgnn: {
      dimensions: 512,
      iterations: 4,
      embedding_density: 0.5,
      heterogeneous: true,
      node_type_property: "node_kind",
      relationship_type_property: "edge_kind",
      seed: 10,
    },
  };
  for (const [algorithm, options] of Object.entries(explicit)) {
    if (
      algorithm === "node2vec" ||
      algorithm === "fast_random_projection" ||
      algorithm === "hashgnn"
    ) {
      const result = tableFromIPC(analyze(forge, algorithm, options));
      assert.equal(result.numRows, 0);
      assert.match(
        result.schema.fields[1].type.toString(),
        {
          node2vec: /64/,
          fast_random_projection: /80/,
          hashgnn: /512/,
        }[algorithm],
      );
      continue;
    }
    if (algorithm === "graphsage") {
      const result = tableFromIPC(analyze(forge, algorithm, options));
      assert.equal(result.numRows, 0);
      assert.match(result.schema.fields[1].type.toString(), /96/);
      continue;
    }
  }
  forge.close();
});

test("executes deterministic non-empty GraphSAGE through the native addon", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'Alice', score:1.0, features:[1.0,0.0]})" +
      "-[:KNOWS]->(:Person {name:'Bob', score:2.0, features:[0.0,1.0]}), " +
      "(:Person {name:'Carol', score:3.0, features:[0.5,0.5]})",
  );
  const options = {
    dimensions: 2,
    hidden_dimensions: 2,
    layers: 1,
    sample_sizes: [1],
    epochs: 1,
    negative_samples: 1,
    learning_rate: 0.001,
    feature_properties: ["score", "features"],
    seed: 13,
  };
  const result = tableFromIPC(
    analyze(forge, "graphsage", options, {
      via: "KNOWS",
      directed: false,
    }),
  );
  const repeated = tableFromIPC(
    analyze(forge, "graphsage", options, {
      via: "KNOWS",
      directed: false,
    }),
  );
  assert.equal(result.numRows, 3);
  assert.deepEqual(
    result.schema.fields.map((field) => [field.name, field.nullable]),
    [
      ["node_uuid", false],
      ["embedding", false],
    ],
  );
  assert.match(result.schema.fields[1].type.toString(), /FixedSizeList.*2/);
  assert.deepEqual(Object.fromEntries(result.schema.metadata), {
    "graphforge.algorithm": "graphsage",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_version": "graphsage-unsupervised-v1",
    "graphforge.algorithm_schema_version": "1",
    "graphforge.dimensions": "2",
    "graphforge.seed": "13",
    "graphforge.rng_version": "splitmix64-v1",
    "graphforge.rng_derivation": "graphforge-embedding-substream-v1",
  });
  const nodeUuids = Array.from(result.getChild("node_uuid"), uuidHex);
  assert.deepEqual(nodeUuids, [...nodeUuids].sort());
  const vectors = Array.from(result.getChild("embedding"), (value) =>
    Array.from(value),
  );
  assert.ok(vectors.every((vector) => vector.length === 2));
  assert.ok(vectors.flat().every(Number.isFinite));
  assert.deepEqual(
    nodeUuids,
    Array.from(repeated.getChild("node_uuid"), uuidHex),
  );
  assert.deepEqual(
    vectors,
    Array.from(repeated.getChild("embedding"), (value) => Array.from(value)),
  );
  forge.close();
});

test("surfaces structured native validation without knowledge fields", () => {
  const forge = new GraphForge();
  const invalid = [
    ["node2vec", { dimensions: 0 }, {}, "embedding dimensions"],
    ["node2vec", { learning_rate: 0 }, {}, "finite and positive"],
    [
      "graphsage",
      { feature_properties: [] },
      { directed: false },
      "non-empty ordered list",
    ],
    [
      "fast_random_projection",
      { feature_properties: ["confidence", "confidence"] },
      {},
      "cannot contain duplicate names",
    ],
    ["hashgnn", { node_type_property: "kind" }, {}, "homogeneous hashgnn"],
    ["hashgnn", { seed: -1 }, {}, "unsigned 64-bit"],
  ];
  for (const [algorithm, options, invocation, fragment] of invalid) {
    expectError("ValidationError", fragment, () =>
      analyze(forge, algorithm, options, invocation),
    );
  }
  expectError("ValidationError", "unknown node2vec option", () =>
    analyze(forge, "node2vec", { provenance: "source" }),
  );
  expectError("ValidationError", "knowledge-layer field", () =>
    analyze(forge, "node2vec", {}, { via: "evidence" }),
  );
  expectError("ValidationError", "partition_property or k", () =>
    analyze(forge, "node2vec", {}, { k: 1 }),
  );
  expectError("ValidationError", "does not accept embedding options", () =>
    forge.analyze(
      "is_dag",
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      {},
    ),
  );
  forge.close();
});

test("executes deterministic non-empty Node2Vec through the native addon", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}), " +
      "(:Person {name:'Carol'})",
  );
  const options = {
    dimensions: 4,
    walk_length: 3,
    walks_per_node: 2,
    window_size: 1,
    negative_samples: 1,
    epochs: 1,
    seed: 7,
  };
  const result = tableFromIPC(
    analyze(forge, "node2vec", options, { via: "KNOWS" }),
  );
  const repeated = tableFromIPC(
    analyze(forge, "node2vec", options, { via: "KNOWS" }),
  );
  assert.equal(result.numRows, 3);
  assert.deepEqual(
    result.schema.fields.map((field) => [field.name, field.nullable]),
    [
      ["node_uuid", false],
      ["embedding", false],
    ],
  );
  assert.match(result.schema.fields[1].type.toString(), /FixedSizeList.*4/);
  assert.deepEqual(Object.fromEntries(result.schema.metadata), {
    "graphforge.algorithm": "node2vec",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_version": "node2vec-v1",
    "graphforge.algorithm_schema_version": "1",
    "graphforge.dimensions": "4",
    "graphforge.seed": "7",
    "graphforge.rng_version": "splitmix64-v1",
    "graphforge.rng_derivation": "graphforge-embedding-substream-v1",
  });
  const nodeUuids = Array.from(result.getChild("node_uuid"), uuidHex);
  assert.deepEqual(nodeUuids, [...nodeUuids].sort());
  const vectors = Array.from(result.getChild("embedding"), (value) =>
    Array.from(value),
  );
  assert.ok(vectors.every((vector) => vector.length === 4));
  assert.ok(vectors.flat().every(Number.isFinite));
  assert.deepEqual(
    nodeUuids,
    Array.from(repeated.getChild("node_uuid"), uuidHex),
  );
  assert.deepEqual(
    vectors,
    Array.from(repeated.getChild("embedding"), (value) => Array.from(value)),
  );
  forge.close();
});

test("executes deterministic non-empty FastRP through the native addon", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'Alice', score:1.0})" +
      "-[:KNOWS {strength:2.0}]->(:Person {name:'Bob', score:2.0}), " +
      "(:Person {name:'Carol', score:3.0})",
  );
  const options = {
    dimensions: 4,
    iteration_weights: [1, 1],
    feature_weight: 1,
    feature_properties: ["score"],
    seed: 11,
  };
  const result = tableFromIPC(
    analyze(forge, "fast_random_projection", options, {
      via: "KNOWS",
      weight: "strength",
    }),
  );
  const repeated = tableFromIPC(
    analyze(forge, "fast_random_projection", options, {
      via: "KNOWS",
      weight: "strength",
    }),
  );
  assert.equal(result.numRows, 3);
  assert.deepEqual(
    result.schema.fields.map((field) => [field.name, field.nullable]),
    [
      ["node_uuid", false],
      ["embedding", false],
    ],
  );
  assert.match(result.schema.fields[1].type.toString(), /FixedSizeList.*4/);
  assert.deepEqual(Object.fromEntries(result.schema.metadata), {
    "graphforge.algorithm": "fast_random_projection",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_version": "fastrp-v1",
    "graphforge.algorithm_schema_version": "1",
    "graphforge.dimensions": "4",
    "graphforge.seed": "11",
    "graphforge.rng_version": "splitmix64-v1",
    "graphforge.rng_derivation": "graphforge-embedding-substream-v1",
  });
  const nodeUuids = Array.from(result.getChild("node_uuid"), uuidHex);
  assert.deepEqual(nodeUuids, [...nodeUuids].sort());
  const vectors = Array.from(result.getChild("embedding"), (value) =>
    Array.from(value),
  );
  assert.ok(vectors.every((vector) => vector.length === 4));
  assert.ok(vectors.flat().every(Number.isFinite));
  assert.deepEqual(
    nodeUuids,
    Array.from(repeated.getChild("node_uuid"), uuidHex),
  );
  assert.deepEqual(
    vectors,
    Array.from(repeated.getChild("embedding"), (value) => Array.from(value)),
  );
  forge.close();
});

test("executes deterministic non-empty HashGNN through the native addon", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'Alice', kind:'human'})" +
      "-[:KNOWS {kind:'friend'}]->(:Person {name:'Bob', kind:'human'}), " +
      "(:Person {name:'Carol', kind:'human'})",
  );
  const options = {
    dimensions: 8,
    iterations: 2,
    embedding_density: 0.25,
    heterogeneous: true,
    node_type_property: "kind",
    relationship_type_property: "kind",
    seed: 19,
  };
  const result = tableFromIPC(
    analyze(forge, "hashgnn", options, {
      via: "KNOWS",
      directed: true,
    }),
  );
  const repeated = tableFromIPC(
    analyze(forge, "hashgnn", options, {
      via: "KNOWS",
      directed: true,
    }),
  );
  assert.equal(result.numRows, 3);
  assert.deepEqual(
    result.schema.fields.map((field) => [field.name, field.nullable]),
    [
      ["node_uuid", false],
      ["embedding", false],
    ],
  );
  assert.match(result.schema.fields[1].type.toString(), /FixedSizeList.*8/);
  assert.deepEqual(Object.fromEntries(result.schema.metadata), {
    "graphforge.algorithm": "hashgnn",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_version": "hashgnn-v1",
    "graphforge.algorithm_schema_version": "1",
    "graphforge.dimensions": "8",
    "graphforge.seed": "19",
    "graphforge.rng_version": "splitmix64-v1",
    "graphforge.rng_derivation": "graphforge-embedding-substream-v1",
  });
  const nodeUuids = Array.from(result.getChild("node_uuid"), uuidHex);
  assert.deepEqual(nodeUuids, [...nodeUuids].sort());
  const vectors = Array.from(result.getChild("embedding"), (value) =>
    Array.from(value),
  );
  assert.ok(vectors.every((vector) => vector.length === 8));
  assert.ok(vectors.flat().every((value) => value === 0 || value === 1));
  assert.deepEqual(
    nodeUuids,
    Array.from(repeated.getChild("node_uuid"), uuidHex),
  );
  assert.deepEqual(
    vectors,
    Array.from(repeated.getChild("embedding"), (value) => Array.from(value)),
  );
  forge.close();
});
