/**
 * Shared GraphForge projection for Node visualization examples.
 *
 * Loads Mark Newman's karate-club GML through GraphForge's public Node API and
 * returns the same deterministic projection consumed by Cytoscape.js / Sigma.js.
 */

import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { execFileSync } from "node:child_process";
import { tableFromIPC } from "apache-arrow";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, "..");
const MANIFEST_PATH = join(ROOT, "dataset", "MANIFEST.json");
const CONTRACT_PATH = join(HERE, "contract.json");
const DEFAULT_CACHE = join(ROOT, ".cache");

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function sha256File(path) {
  const hash = createHash("sha256");
  hash.update(readFileSync(path));
  return hash.digest("hex");
}

async function download(url, dest) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Download failed (${response.status}): ${url}`);
  }
  await pipeline(Readable.fromWeb(response.body), createWriteStream(dest));
}

export async function fetchDataset(cacheDir = DEFAULT_CACHE, { force = false } = {}) {
  const manifest = loadJson(MANIFEST_PATH);
  mkdirSync(cacheDir, { recursive: true });
  const archivePath = join(cacheDir, manifest.archive.filename);
  const extractDir = join(cacheDir, "karate");
  const expected = manifest.archive.sha256;

  if (force || !existsSync(archivePath) || sha256File(archivePath) !== expected) {
    await download(manifest.source_url, archivePath);
  }
  const actual = sha256File(archivePath);
  if (actual !== expected) {
    throw new Error(
      `Archive checksum mismatch: expected ${expected}, got ${actual}`,
    );
  }

  mkdirSync(extractDir, { recursive: true });
  execFileSync("unzip", ["-o", archivePath, "-d", extractDir], {
    stdio: "ignore",
  });

  for (const [relative, memberExpected] of Object.entries(manifest.archive.members)) {
    const memberPath = join(extractDir, relative);
    if (!existsSync(memberPath)) {
      throw new Error(`Missing archive member: ${relative}`);
    }
    const memberActual = sha256File(memberPath);
    if (memberActual !== memberExpected) {
      throw new Error(
        `Member checksum mismatch for ${relative}: expected ${memberExpected}, got ${memberActual}`,
      );
    }
  }
  return extractDir;
}

export function parseGmlUndirectedEdges(gmlText) {
  const edges = new Set();
  const re = /edge\s*\[\s*source\s+(\d+)\s+target\s+(\d+)/g;
  let match;
  while ((match = re.exec(gmlText)) !== null) {
    const left = Number(match[1]);
    const right = Number(match[2]);
    if (left === right) continue;
    const a = Math.min(left, right);
    const b = Math.max(left, right);
    edges.add(`${a},${b}`);
  }
  return [...edges]
    .map((key) => key.split(",").map(Number))
    .sort((x, y) => x[0] - y[0] || x[1] - y[1]);
}

function resolveGraphForge() {
  const envPath = process.env.GRAPHFORGE_NODE_PATH;
  if (envPath) {
    return import(pathToFileURL(envPath).href);
  }
  // Prefer the in-repo binding during development.
  const localBinding = join(
    ROOT,
    "..",
    "..",
    "crates",
    "graphforge-bindings-node",
    "index.js",
  );
  if (existsSync(localBinding)) {
    return import(pathToFileURL(localBinding).href);
  }
  return import("@curatelabs/graphforge");
}

export async function buildGraphForge(datasetDir) {
  const [{ GraphForge }, manifest] = await Promise.all([
    resolveGraphForge(),
    Promise.resolve(loadJson(MANIFEST_PATH)),
  ]);
  const extractDir = datasetDir || (await fetchDataset());
  const gml = readFileSync(join(extractDir, "karate.gml"), "utf8");
  const edges = parseGmlUndirectedEdges(gml);
  if (edges.length !== manifest.graph.edge_count) {
    throw new Error(
      `Expected ${manifest.graph.edge_count} edges, found ${edges.length}`,
    );
  }

  const forge = new GraphForge();
  const handles = new Map();
  const [low, high] = manifest.graph.node_id_range;
  for (let clubId = low; clubId <= high; clubId += 1) {
    handles.set(
      clubId,
      forge.addNode(manifest.graph.node_label, {
        club_id: clubId,
        label: `M${clubId}`,
      }),
    );
  }
  if (handles.size !== manifest.graph.node_count) {
    throw new Error(
      `Expected ${manifest.graph.node_count} nodes, built ${handles.size}`,
    );
  }

  const rel = manifest.graph.relationship_type;
  for (const [source, target] of edges) {
    forge.addEdge(handles.get(source), rel, handles.get(target));
  }
  return forge;
}

function columnValues(table, name) {
  return [...table.getChild(name).toArray()].map((value) =>
    typeof value === "bigint" ? Number(value) : value,
  );
}

export async function project(forge, datasetDir) {
  const contract = loadJson(CONTRACT_PATH);
  const engine = forge || (await buildGraphForge(datasetDir));
  const nodeTable = tableFromIPC(engine.execute(contract.query.nodes));
  const edgeTable = tableFromIPC(engine.execute(contract.query.edges));

  const clubIds = columnValues(nodeTable, "club_id");
  const labels = columnValues(nodeTable, "label");
  const sources = columnValues(edgeTable, "source");
  const targets = columnValues(edgeTable, "target");

  return {
    projection_id: contract.projection_id,
    directed: contract.edge.directed,
    layout_seed: contract.layout.seed,
    style: {
      node_color: contract.node.color,
      node_size: contract.node.size,
      edge_color: contract.edge.color,
      edge_width: contract.edge.width,
    },
    nodes: clubIds.map((clubId, index) => ({
      id: Number(clubId),
      label: String(labels[index]),
      club_id: Number(clubId),
    })),
    edges: sources.map((source, index) => ({
      source: Number(source),
      target: Number(targets[index]),
    })),
  };
}

export function writeProjectionJson(path, projection) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(projection, null, 2)}\n`, "utf8");
  return path;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const out = join(ROOT, "output", "projection.json");
  const payload = await project();
  writeProjectionJson(out, payload);
  console.log(out);
}
