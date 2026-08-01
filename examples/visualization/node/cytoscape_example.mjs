#!/usr/bin/env node
/**
 * Cytoscape.js visualization payload over the shared GraphForge projection.
 *
 * Builds a browser-ready HTML page that loads Cytoscape from a CDN and embeds
 * the shared elements JSON. No interactive browser is opened in CI.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { project } from "../shared/projection.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

export function toCytoscapeElements(projection) {
  const nodes = projection.nodes.map((node) => ({
    data: {
      id: String(node.id),
      label: node.label,
      club_id: node.club_id,
    },
  }));
  const edges = projection.edges.map((edge, index) => ({
    data: {
      id: `e${index}`,
      source: String(edge.source),
      target: String(edge.target),
    },
  }));
  return [...nodes, ...edges];
}

export function buildHtml(projection, elements) {
  const style = projection.style;
  const payload = JSON.stringify(elements);
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Zachary karate club (Cytoscape.js / GraphForge)</title>
  <script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>
  <style>
    html, body, #cy { width: 100%; height: 100%; margin: 0; padding: 0; }
  </style>
</head>
<body>
  <div id="cy"></div>
  <script>
    const elements = ${payload};
    cytoscape({
      container: document.getElementById('cy'),
      elements,
      style: [
        {
          selector: 'node',
          style: {
            'background-color': ${JSON.stringify(style.node_color)},
            'label': 'data(label)',
            'width': ${JSON.stringify(style.node_size)},
            'height': ${JSON.stringify(style.node_size)},
            'font-size': 10
          }
        },
        {
          selector: 'edge',
          style: {
            'line-color': ${JSON.stringify(style.edge_color)},
            'width': ${JSON.stringify(style.edge_width)},
            'curve-style': 'bezier'
          }
        }
      ],
      layout: {
        name: 'cose',
        animate: false,
        randomize: true
      },
      wheelSensitivity: 0.2
    });
  </script>
</body>
</html>
`;
}

export async function run(outputDir = join(ROOT, "output")) {
  const projection = await project();
  const elements = toCytoscapeElements(projection);
  mkdirSync(outputDir, { recursive: true });

  const elementsPath = join(outputDir, "cytoscape_karate_elements.json");
  const htmlPath = join(outputDir, "cytoscape_karate.html");
  writeFileSync(
    elementsPath,
    `${JSON.stringify(
      {
        projection_id: projection.projection_id,
        layout_seed_requested: projection.layout_seed,
        limitation:
          "Cytoscape cose layout does not expose a portable cross-version seed matching GraphForge's contract seed; HTML uses cose with animate:false.",
        elements,
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(htmlPath, buildHtml(projection, elements));
  return [elementsPath, htmlPath];
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const outputDir = process.argv.includes("--output-dir")
    ? process.argv[process.argv.indexOf("--output-dir") + 1]
    : join(ROOT, "output");
  for (const path of await run(outputDir)) {
    console.log(path);
  }
}
