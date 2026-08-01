#!/usr/bin/env node
/**
 * Headless Plotly.js figure-construction probe.
 * Reads JSON {nodes, edges, layout_seed, style?} from stdin; writes measurement JSON to stdout.
 * Builds the same circular-layout figure shape as the #298 Plotly.js example; does not open a browser.
 */
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

function seededLayout(nodeIds, seed) {
  const sorted = [...nodeIds].sort((a, b) => {
    const na = Number(a);
    const nb = Number(b);
    if (Number.isFinite(na) && Number.isFinite(nb) && na !== nb) {
      return na - nb;
    }
    return String(a).localeCompare(String(b));
  });
  const count = sorted.length || 1;
  const positions = new Map();
  sorted.forEach((nodeId, index) => {
    const angle = (2 * Math.PI * index) / count + ((seed % 360) * Math.PI) / 180;
    positions.set(nodeId, [Math.cos(angle), Math.sin(angle)]);
  });
  return positions;
}

const started = performance.now();
const request = JSON.parse(await readStdin());
const style = request.style || {
  node_size: 8,
  node_color: "#2E86AB",
  edge_width: 0.5,
  edge_color: "#888",
};
const positions = seededLayout(
  request.nodes.map((node) => node.id),
  Number(request.layout_seed || 0),
);

const edgeX = [];
const edgeY = [];
for (const edge of request.edges) {
  const [x0, y0] = positions.get(edge.source);
  const [x1, y1] = positions.get(edge.target);
  edgeX.push(x0, x1, null);
  edgeY.push(y0, y1, null);
}

const figure = {
  data: [
    {
      type: "scatter",
      x: edgeX,
      y: edgeY,
      mode: "lines",
      line: { width: style.edge_width, color: style.edge_color },
      hoverinfo: "none",
      name: "edges",
    },
    {
      type: "scatter",
      x: request.nodes.map((node) => positions.get(node.id)[0]),
      y: request.nodes.map((node) => positions.get(node.id)[1]),
      mode: "markers",
      marker: { size: style.node_size, color: style.node_color },
      text: request.nodes.map(
        (node) => `${node.label} (club_id=${node.club_id})`,
      ),
      hoverinfo: "text",
      name: "nodes",
    },
  ],
  layout: {
    title: "GraphForge visualization stress — Plotly.js",
    showlegend: false,
    xaxis: { showgrid: false, zeroline: false, visible: false },
    yaxis: { showgrid: false, zeroline: false, visible: false },
  },
};

const payload = JSON.stringify(figure);
const constructSeconds = (performance.now() - started) / 1000;
const here = dirname(fileURLToPath(import.meta.url));
let plotlyjsVersion = "cdn-plotly-2.35.2";
try {
  // Optional: if a local plotly package is ever installed for experiments.
  plotlyjsVersion = JSON.parse(
    readFileSync(join(here, "node_modules", "plotly.js-dist-min", "package.json"), "utf8"),
  ).version;
} catch {
  // Expected: stress probe constructs figure JSON without a Node Plotly package.
}

process.stdout.write(
  JSON.stringify({
    payload_bytes: Buffer.byteLength(payload, "utf8"),
    construct_seconds: constructSeconds,
    node_count: request.nodes.length,
    edge_count: request.edges.length,
    plotly_js_version: plotlyjsVersion,
    payload_preview: payload.slice(0, 200),
    divergence_notes:
      "Plotly.js figure JSON construction with circular layout from the shared seed; no DOM/Plotly.newPlot (matches Python to_json() headless path).",
  }),
);
