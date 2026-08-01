#!/usr/bin/env node
/**
 * Plotly.js visualization over the shared GraphForge karate projection.
 *
 * Builds a Plotly figure JSON and a browser-ready HTML page that loads Plotly
 * from a CDN. No interactive browser is opened in CI. Layout matches the
 * Python Plotly example: deterministic circular coordinates from layout_seed.
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { project } from "../shared/projection.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

export function seededLayout(nodeIds, seed) {
  const sorted = [...nodeIds].sort((a, b) => a - b);
  const count = sorted.length || 1;
  const positions = new Map();
  sorted.forEach((nodeId, index) => {
    const angle = (2 * Math.PI * index) / count + ((seed % 360) * Math.PI) / 180;
    positions.set(nodeId, [Math.cos(angle), Math.sin(angle)]);
  });
  return positions;
}

export function toPlotlyFigure(projection) {
  const positions = seededLayout(
    projection.nodes.map((node) => node.id),
    projection.layout_seed,
  );

  const edgeX = [];
  const edgeY = [];
  for (const edge of projection.edges) {
    const [x0, y0] = positions.get(edge.source);
    const [x1, y1] = positions.get(edge.target);
    edgeX.push(x0, x1, null);
    edgeY.push(y0, y1, null);
  }

  const nodeX = projection.nodes.map((node) => positions.get(node.id)[0]);
  const nodeY = projection.nodes.map((node) => positions.get(node.id)[1]);
  const labels = projection.nodes.map((node) => node.label);

  return {
    data: [
      {
        type: "scatter",
        x: edgeX,
        y: edgeY,
        mode: "lines",
        line: {
          width: projection.style.edge_width,
          color: projection.style.edge_color,
        },
        hoverinfo: "none",
        name: "edges",
      },
      {
        type: "scatter",
        x: nodeX,
        y: nodeY,
        mode: "markers+text",
        text: labels,
        textposition: "top center",
        marker: {
          size: projection.style.node_size,
          color: projection.style.node_color,
        },
        name: "nodes",
      },
    ],
    layout: {
      title: "Zachary karate club (Plotly.js / GraphForge projection)",
      showlegend: false,
      hovermode: "closest",
      xaxis: { showgrid: false, zeroline: false, showticklabels: false },
      yaxis: { showgrid: false, zeroline: false, showticklabels: false },
    },
  };
}

export function buildHtml(figure) {
  const payload = JSON.stringify(figure);
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Zachary karate club (Plotly.js / GraphForge)</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    html, body, #graph { width: 100%; height: 100%; margin: 0; padding: 0; }
  </style>
</head>
<body>
  <div id="graph"></div>
  <script>
    const figure = ${payload};
    Plotly.newPlot("graph", figure.data, figure.layout, {responsive: true});
  </script>
</body>
</html>
`;
}

export async function run(outputDir = join(ROOT, "output")) {
  const projection = await project();
  const figure = toPlotlyFigure(projection);
  mkdirSync(outputDir, { recursive: true });

  const jsonPath = join(outputDir, "plotly_js_karate.json");
  const htmlPath = join(outputDir, "plotly_js_karate.html");
  writeFileSync(jsonPath, `${JSON.stringify(figure, null, 2)}\n`);
  writeFileSync(htmlPath, buildHtml(figure));
  return [htmlPath, jsonPath];
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const outputDir = process.argv.includes("--output-dir")
    ? process.argv[process.argv.indexOf("--output-dir") + 1]
    : join(ROOT, "output");
  for (const path of await run(outputDir)) {
    console.log(path);
  }
}
