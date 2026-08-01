#!/usr/bin/env node
/**
 * Sigma.js visualization payload over the shared GraphForge projection.
 *
 * Builds a graphology graph export and a browser-ready HTML page that loads
 * Sigma from a CDN. No interactive browser is opened in CI.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { project } from "../shared/projection.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function seededUnit(seed, index) {
  let x = (seed * 1664525 + index * 1013904223) >>> 0;
  x ^= x << 13;
  x ^= x >>> 17;
  x ^= x << 5;
  return (x >>> 0) / 0xffffffff;
}

export function toGraphologyExport(projection) {
  const nodes = projection.nodes.map((node, index) => {
    const angle = (2 * Math.PI * index) / projection.nodes.length;
    const jitter = seededUnit(projection.layout_seed, index) * 0.05;
    return {
      key: String(node.id),
      attributes: {
        label: node.label,
        club_id: node.club_id,
        x: Math.cos(angle) + jitter,
        y: Math.sin(angle) + jitter,
        size: projection.style.node_size / 4,
        color: projection.style.node_color,
      },
    };
  });
  const edges = projection.edges.map((edge, index) => ({
    key: `e${index}`,
    source: String(edge.source),
    target: String(edge.target),
    attributes: {
      color: projection.style.edge_color,
      size: projection.style.edge_width,
    },
  }));
  return {
    attributes: {
      name: projection.projection_id,
      layout_seed: projection.layout_seed,
    },
    options: {
      type: projection.directed ? "directed" : "undirected",
      multi: false,
      allowSelfLoops: false,
    },
    nodes,
    edges,
  };
}

export function buildHtml(projection, graphExport) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Zachary karate club (Sigma.js / GraphForge)</title>
  <style>
    html, body, #container { width: 100%; height: 100%; margin: 0; padding: 0; }
  </style>
</head>
<body>
  <div id="container"></div>
  <script type="importmap">
  {
    "imports": {
      "graphology": "https://cdn.jsdelivr.net/npm/graphology@0.25.4/+esm",
      "sigma": "https://cdn.jsdelivr.net/npm/sigma@3.0.1/+esm"
    }
  }
  </script>
  <script type="module">
    import Graph from "graphology";
    import Sigma from "sigma";
    const exported = ${JSON.stringify(graphExport)};
    const graph = new Graph(exported.options);
    for (const node of exported.nodes) {
      graph.addNode(node.key, node.attributes);
    }
    for (const edge of exported.edges) {
      graph.addEdgeWithKey(edge.key, edge.source, edge.target, edge.attributes);
    }
    new Sigma(graph, document.getElementById("container"));
  </script>
</body>
</html>
`;
}

export async function run(outputDir = join(ROOT, "output")) {
  const projection = await project();
  const graphExport = toGraphologyExport(projection);
  mkdirSync(outputDir, { recursive: true });

  const graphPath = join(outputDir, "sigma_karate_graph.json");
  const htmlPath = join(outputDir, "sigma_karate.html");
  writeFileSync(graphPath, `${JSON.stringify(graphExport, null, 2)}\n`);
  writeFileSync(htmlPath, buildHtml(projection, graphExport));
  return [graphPath, htmlPath];
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const outputDir = process.argv.includes("--output-dir")
    ? process.argv[process.argv.indexOf("--output-dir") + 1]
    : join(ROOT, "output");
  for (const path of await run(outputDir)) {
    console.log(path);
  }
}
