#!/usr/bin/env node
/**
 * Headless Cytoscape.js construction probe.
 * Reads JSON {elements, layout_seed} from stdin; writes measurement JSON to stdout.
 * Does not open a browser or run a visible layout animation.
 */
import cytoscape from "cytoscape";
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
const cy = cytoscape({
  headless: true,
  styleEnabled: true,
  elements: [
    ...request.elements.nodes,
    ...request.elements.edges,
  ],
});
// Seeded circular positions — avoids depending on browser layout engines.
const nodes = cy.nodes();
const n = nodes.length || 1;
const seed = Number(request.layout_seed || 0);
nodes.forEach((node, i) => {
  const angle = (2 * Math.PI * i) / n + (seed % 360) * (Math.PI / 180);
  node.position({ x: Math.cos(angle) * 100, y: Math.sin(angle) * 100 });
});
const exported = cy.json();
const payload = JSON.stringify(exported);
const constructSeconds = (performance.now() - started) / 1000;
const here = dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(
  readFileSync(join(here, "node_modules", "cytoscape", "package.json"), "utf8"),
);

process.stdout.write(
  JSON.stringify({
    payload_bytes: Buffer.byteLength(payload, "utf8"),
    construct_seconds: constructSeconds,
    node_count: cy.nodes().length,
    edge_count: cy.edges().length,
    cytoscape_version: pkg.version,
    payload_preview: payload.slice(0, 200),
    divergence_notes:
      "Cytoscape.js headless element+position construction; no DOM/WebGL first-paint.",
  }),
);
cy.destroy();
