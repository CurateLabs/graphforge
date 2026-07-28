// Same-SHA native Node representative evidence for #2469.

import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { tableFromArrays, tableToIPC } from "apache-arrow";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, value, index, values) => {
    if (value.startsWith("--")) pairs.push([value.slice(2), values[index + 1]]);
    return pairs;
  }, []),
);
for (const name of [
  "source-project",
  "target-project",
  "ontology",
  "evidence",
  "commit-sha",
  "module",
]) {
  if (!args[name]) throw new Error(`missing --${name}`);
}

const modulePath = resolve(args.module);
const { GraphForge, version } = await import(pathToFileURL(modulePath));
const packageVersion = JSON.parse(
  readFileSync(resolve(dirname(modulePath), "package.json"), "utf8"),
).version;

const scratch = resolve(dirname(args["source-project"]), "node-source-scratch");
mkdirSync(scratch, { recursive: true });
const source = new GraphForge(scratch);
assert.equal(source.ontologyMode, "exploratory");
const nodeTable = tableFromArrays({
  node_uuid: [null, null],
  label: ["Host", "Host"],
  name: ["edge-gw-01", "edge-gw-02"],
  risk_score: [0.4, 0.55],
});
source.publishBulkNodes(
  "018f0f4e-7b8c-7000-8000-000000029911",
  Buffer.from(tableToIPC(nodeTable)),
);
source.loadOntology(resolve(args.ontology));
assert.equal(source.ontologyMode, "advisory");
source.close();

const reopened = new GraphForge(scratch);
assert.equal(reopened.ontologyMode, "exploratory");
const names = reopened.execute(
  "MATCH (h:Host) RETURN h.name AS name ORDER BY name",
);
assert.deepEqual(
  [...names.getChild("name")],
  ["edge-gw-01", "edge-gw-02"],
);
reopened.close();

const target = new GraphForge(resolve(args["target-project"]));
assert.equal(target.ontologyMode, "strict");
const before = target.execute(
  "MATCH (h:HostAsset) RETURN h.name AS name ORDER BY name",
);
const beforeNames = [...before.getChild("name")];
assert.deepEqual(beforeNames, ["edge-gw-01", "edge-gw-02"]);
let failure = "";
try {
  const bad = tableFromArrays({
    node_uuid: [null],
    label: ["UnmappedLabel"],
    name: ["ghost"],
    source_graph_uuid: [randomUUID()],
    approval_record_uuid: ["018f0f4e-7b8c-7000-8000-00000000a001"],
  });
  target.publishBulkNodes(
    "018f0f4e-7b8c-7000-8000-000000029913",
    Buffer.from(tableToIPC(bad)),
  );
  throw new Error("strict unmapped label must fail");
} catch (error) {
  failure = String(error);
}
const afterNames = [
  ...target
    .execute("MATCH (h:HostAsset) RETURN h.name AS name ORDER BY name")
    .getChild("name"),
];
assert.deepEqual(afterNames, beforeNames);
target.close();

const rustSource = new GraphForge(resolve(args["source-project"]));
assert.equal(rustSource.ontologyMode, "exploratory");
rustSource.close();

const require = createRequire(import.meta.url);
const addonPaths = Object.entries(require.cache)
  .filter(
    ([path, cached]) =>
      path.endsWith(".node") && cached.exports.GraphForge === GraphForge,
  )
  .map(([path]) => path);
if (addonPaths.length !== 1) {
  throw new Error(`expected one native addon, found ${addonPaths.length}`);
}
const nativePath = addonPaths[0];
const evidence = {
  binding: "node",
  commit_sha: args["commit-sha"],
  package_version: packageVersion,
  reported_version: version,
  package_module_path: modulePath,
  native_module_path: nativePath,
  native_module_sha256: createHash("sha256")
    .update(readFileSync(nativePath))
    .digest("hex"),
  source_reopen_exploratory: true,
  strict_reject_before_mutation: true,
  failure,
  uuid_composition: true,
  reopen_equal: true,
};
writeFileSync(resolve(args.evidence), `${JSON.stringify(evidence, null, 2)}\n`);
