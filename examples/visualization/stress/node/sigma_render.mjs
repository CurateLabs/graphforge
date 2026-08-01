#!/usr/bin/env node
/**
 * Headless Sigma.js / graphology construction probe.
 * Builds a graphology graph suitable for Sigma; does not start WebGL.
 */
import Graph from "graphology";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function readStdin() {
  return new Promise((resolve, reject) => {
    const chunks = [];
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (c) => chunks.push(c));
    process.stdin.on("end", () => resolve(chunks.join("")));
    process.stdin.on("error", reject);
  });
}

const started = performance.now();
const request = JSON.parse(await readStdin());
const graph = new Graph({
  type: request.directed ? "directed" : "undirected",
  multi: false,
  allowSelfLoops: false,
});
const n = request.nodes.length || 1;
const seed = Number(request.layout_seed || 0);
for (const [i, node] of request.nodes.entries()) {
  const angle = (2 * Math.PI * i) / n + (seed % 360) * (Math.PI / 180);
  graph.addNode(node.id, {
    label: node.label,
    group: node.group,
    x: Math.cos(angle),
    y: Math.sin(angle),
    size: 5,
  });
}
for (const edge of request.edges) {
  if (!graph.hasEdge(edge.source, edge.target)) {
    graph.addEdgeWithKey(edge.id, edge.source, edge.target, { type: edge.type });
  }
}
const exported = graph.export();
const payload = JSON.stringify(exported);
const constructSeconds = (performance.now() - started) / 1000;
const here = dirname(fileURLToPath(import.meta.url));
const graphologyPkg = JSON.parse(
  readFileSync(join(here, "node_modules", "graphology", "package.json"), "utf8"),
);
let sigmaVersion = "not-instantiated";
try {
  sigmaVersion = JSON.parse(
    readFileSync(join(here, "node_modules", "sigma", "package.json"), "utf8"),
  ).version;
} catch {
  // optional
}

process.stdout.write(
  JSON.stringify({
    payload_bytes: Buffer.byteLength(payload, "utf8"),
    construct_seconds: constructSeconds,
    node_count: graph.order,
    edge_count: graph.size,
    graphology_version: graphologyPkg.version,
    sigma_version: sigmaVersion,
    payload_preview: payload.slice(0, 200),
    divergence_notes:
      "Sigma.js WebGL renderer is not started in CI-class headless runs; graphology graph export is the measured artifact. This understates browser GPU cost.",
  }),
);
