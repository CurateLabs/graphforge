/**
 * Focused checks for Node visualization examples (#298).
 *
 * Constructs Cytoscape.js and Sigma.js payloads without opening a browser.
 */

import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import test from "node:test";
import { project } from "../shared/projection.mjs";
import { toCytoscapeElements, buildHtml as buildCyHtml } from "../node/cytoscape_example.mjs";
import {
  toGraphologyExport,
  buildHtml as buildSigmaHtml,
} from "../node/sigma_example.mjs";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

test("shared projection has karate counts via GraphForge", async () => {
  const projection = await project();
  assert.equal(projection.projection_id, "karate-member-friend-v1");
  assert.equal(projection.nodes.length, 34);
  assert.equal(projection.edges.length, 78);
});

test("cytoscape builds elements and html artifact", async () => {
  const projection = await project();
  const elements = toCytoscapeElements(projection);
  assert.equal(elements.filter((el) => el.data.source == null).length, 34);
  assert.equal(elements.filter((el) => el.data.source != null).length, 78);
  const html = buildCyHtml(projection, elements);
  assert.match(html, /cytoscape/);
  assert.match(html, /M1/);
});

test("sigma builds graphology export and html artifact", async () => {
  const projection = await project();
  const graphExport = toGraphologyExport(projection);
  assert.equal(graphExport.nodes.length, 34);
  assert.equal(graphExport.edges.length, 78);
  assert.equal(graphExport.attributes.layout_seed, 42);
  const html = buildSigmaHtml(projection, graphExport);
  assert.match(html, /sigma/i);
  assert.match(html, /graphology/i);
});

test("example scripts write artifacts", async () => {
  const out = mkdtempSync(join(tmpdir(), "gf-viz-node-"));
  try {
    for (const script of ["cytoscape_example.mjs", "sigma_example.mjs"]) {
      const result = spawnSync(
        process.execPath,
        [join(ROOT, "node", script), "--output-dir", out],
        { encoding: "utf8" },
      );
      assert.equal(result.status, 0, result.stderr || result.stdout);
    }
    assert.match(
      readFileSync(join(out, "cytoscape_karate.html"), "utf8"),
      /cytoscape/,
    );
    assert.match(
      readFileSync(join(out, "sigma_karate_graph.json"), "utf8"),
      /karate-member-friend-v1/,
    );
  } finally {
    rmSync(out, { recursive: true, force: true });
  }
});
