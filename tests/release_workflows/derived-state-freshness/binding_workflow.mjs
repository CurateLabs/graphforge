// Same-SHA native Node evidence for derived-state freshness.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, value, index, values) => {
    if (value.startsWith("--")) pairs.push([value.slice(2), values[index + 1]]);
    return pairs;
  }, []),
);
for (const name of ["project", "evidence", "commit-sha", "module"]) {
  if (!args[name]) throw new Error(`missing --${name}`);
}

const modulePath = resolve(args.module);
const { GraphForge, version } = await import(pathToFileURL(modulePath));
const packageVersion = JSON.parse(
  readFileSync(resolve(dirname(modulePath), "package.json"), "utf8"),
).version;
const forge = new GraphForge(resolve(args.project));
const alice = forge.addNode("Person", {
  name: "Alice",
  summary: "Graph systems",
});
const bob = forge.addNode("Person", {
  name: "Bob",
  summary: "Native bindings",
});
const edge = forge.addEdge(alice, "KNOWS", bob);

const textCurrent = forge.index("Person", {
  properties: ["name"],
  rebuild: true,
});
const adjacencyCurrent = forge.indexAdjacency();
const embeddingV1 = forge.publishCallerEmbeddings("semantic", {
  rows: [
    { node: alice, vector: [1, 0] },
    { node: bob, vector: [0, 1] },
  ],
  dimensions: 2,
  contractVersion: "derived-state-v1",
  sourceProjection: { label: "Person", recipe: "v1" },
});
const embeddingInitial = forge.inspectEmbeddingSpaceFreshness("semantic");

const carol = forge.addNode("Person", {
  name: "Carol",
  summary: "Fresh state",
});
const textStale = forge.inspectTextIndex("Person", ["name"]);
forge.execute(
  "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $edge_uuid DELETE r",
  { edge_uuid: { $uuid: edge.uuid } },
);
const adjacencyStale = forge.inspectAdjacency();
const textRebuilt = forge.index("Person", {
  properties: ["name"],
  rebuild: true,
});
const adjacencyRebuilt = forge.rebuildAdjacency();
const embeddingV2 = forge.publishCallerEmbeddings("semantic", {
  rows: [
    { node: alice, vector: [0, 1] },
    { node: bob, vector: [1, 0] },
    { node: carol, vector: [1, 0] },
  ],
  dimensions: 2,
  contractVersion: "derived-state-v2",
  sourceProjection: { label: "Person", recipe: "v2" },
  replace: true,
});
const embeddingReplaced = forge.inspectEmbeddingSpaceFreshness("semantic");

const states = (receipts) => receipts.map(({ state }) => state);
assert.deepEqual(states([textCurrent, textStale, textRebuilt]), [
  "current",
  "stale",
  "current",
]);
assert.deepEqual(states([adjacencyCurrent, adjacencyStale, adjacencyRebuilt]), [
  "current",
  "stale",
  "current",
]);
assert.equal(embeddingInitial.state, "fresh");
assert.equal(embeddingReplaced.state, "fresh");
assert.equal(embeddingInitial.compatibilityId, embeddingV1);
assert.equal(embeddingReplaced.compatibilityId, embeddingV2);
assert.notEqual(embeddingInitial.generationId, embeddingReplaced.generationId);

const authority = {
  text: forge.inspectTextIndex("Person", ["name"]),
  adjacency: forge.inspectAdjacency(),
  embedding: forge.inspectEmbeddingSpaceFreshness("semantic"),
};
forge.close();
const reopened = new GraphForge(resolve(args.project));
const reopenedAuthority = {
  text: reopened.inspectTextIndex("Person", ["name"]),
  adjacency: reopened.inspectAdjacency(),
  embedding: reopened.inspectEmbeddingSpaceFreshness("semantic"),
};
reopened.close();
const reopenEqual = isDeepStrictEqual(reopenedAuthority, authority);
assert.deepEqual(reopenedAuthority, authority);

const require = createRequire(import.meta.url);
const addonPaths = Object.entries(require.cache)
  .filter(
    ([path, cached]) =>
      path.endsWith(".node") && cached.exports.GraphForge === GraphForge,
  )
  .map(([path]) => path);
assert.equal(addonPaths.length, 1, `loaded addons: ${addonPaths.join(", ")}`);
const addonPath = resolve(addonPaths[0]);
const addonSha256 = createHash("sha256")
  .update(readFileSync(addonPath))
  .digest("hex");
writeFileSync(
  resolve(args.evidence),
  `${JSON.stringify(
    {
      schema_version: 1,
      scenario_id: "derived-state-freshness",
      binding: "node",
      commit_sha: args["commit-sha"],
      package_version: packageVersion,
      text_states: states([textCurrent, textStale, textRebuilt]),
      adjacency_states: states([
        adjacencyCurrent,
        adjacencyStale,
        adjacencyRebuilt,
      ]),
      compatibility_ids: [embeddingV1, embeddingV2],
      generation_ids: [
        embeddingInitial.generationId,
        embeddingReplaced.generationId,
      ],
      embedding_states: [embeddingInitial.state, embeddingReplaced.state],
      reopen_equal: reopenEqual,
      native_version: version(),
      native_module_path: addonPath,
      native_module_sha256: addonSha256,
    },
    null,
    2,
  )}\n`,
);
