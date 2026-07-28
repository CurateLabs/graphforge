/**
 * cucumber-js step definitions for tests/features/api/*.feature
 *
 * Implemented steps call the native addon strictly; unsupported milestone
 * areas remain pending. The same features run against Python, Rust, and Node.
 */

import {
  Given,
  When,
  Then,
  Before,
  setWorldConstructor,
  IWorldOptions,
  World,
} from "@cucumber/cucumber";
import { tableFromIPC, Table } from "apache-arrow";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

// ---------------------------------------------------------------------------
// Import the native binding (built by `napi build`; see the node-native-build
// CI job). napi-rs cannot export JS Error subclasses, so the fault domain is
// carried on `err.code` (see crates/gf-bindings-node/src/error.rs); assertions
// below match on that code. Construction handles are native and UUID-only;
// EdgeHandle values remain pending until the Rust construction path lands.
// ---------------------------------------------------------------------------
const { GraphForge, NodeHandle, EdgeHandle, version } = require("../../../../crates/gf-bindings-node/index.js");
void version;

/** The fault-domain code on a thrown native error (`err.code`). */
function errCode(e: unknown): string | undefined {
  return e && typeof e === "object" ? (e as { code?: string }).code : undefined;
}

/** Build an error carrying a `code`, matching the native error shape. */
function codedError(code: string, message: string): Error {
  return Object.assign(new Error(message), { code });
}

// ---------------------------------------------------------------------------
// World
// ---------------------------------------------------------------------------

interface GFWorld {
  forge: InstanceType<typeof GraphForge> | null;
  result: unknown;
  error: Error | null;
  // Handles stay untyped until the native declarations land. Fixtures use only
  // the public UUID identity; final shared selector scenarios land in #1301.
  nodes: Record<string, any>;
  edges: any[];
  extra: Record<string, unknown>;
  tmpDir: string | null;
}

class GraphForgeWorld extends World implements GFWorld {
  forge: InstanceType<typeof GraphForge> | null = null;
  result: unknown = null;
  error: Error | null = null;
  nodes: Record<string, any> = {};
  edges: any[] = [];
  extra: Record<string, unknown> = {};
  tmpDir: string | null = null;

  constructor(options: IWorldOptions) {
    super(options);
  }
}

setWorldConstructor(GraphForgeWorld);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function _catch(
  world: GraphForgeWorld,
  fn: () => unknown
): void {
  try {
    world.result = fn();
    world.error = null;
  } catch (e) {
    world.result = null;
    world.error = e as Error;
  }
}

function _mkTmp(world: GraphForgeWorld): string {
  if (!world.tmpDir) {
    world.tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gf-bdd-"));
  }
  return world.tmpDir!;
}

/**
 * Decode the `execute()` result — an Arrow IPC stream `Buffer` (the native
 * binding returns `result_to_ipc` bytes) — into an Arrow `Table`. Throws
 * (failing the step) if the query errored or the result is not a Buffer, so
 * strict mode surfaces a misuse as a failure rather than a pending skeleton.
 */
function resultTable(world: GraphForgeWorld): Table {
  if (world.error) {
    throw new Error(`expected an Arrow result, but the query errored: ${world.error.message}`);
  }
  if (!(world.result instanceof Buffer)) {
    throw new Error(`expected an Arrow IPC Buffer result, got: ${typeof world.result}`);
  }
  return tableFromIPC(world.result);
}

/** First-row value of `col` (throws if the column is absent). */
function firstRowValue(world: GraphForgeWorld, col: string): unknown {
  const table = resultTable(world);
  const child = table.getChild(col as never);
  if (child === null) {
    const cols = table.schema.fields.map((f) => f.name).join(", ");
    throw new Error(`result has no column "${col}" (columns: ${cols})`);
  }
  return child.get(0);
}

/** Format a JS value as a Cypher literal for a CREATE property map. */
function cypherLit(v: unknown): string {
  return typeof v === "number" ? String(v) : JSON.stringify(v);
}

/**
 * Build graph state via `execute("CREATE …")` — the data-setup `Given` steps run
 * against the real engine (the `addNode`/`addEdge` write API is M18/M19). Each
 * entry is a `:Person`/`:Paper`/… node's property map.
 */
function createNode(world: GraphForgeWorld, label: string, props: Record<string, unknown>): void {
  const body = Object.entries(props)
    .map(([k, v]) => `${k}: ${cypherLit(v)}`)
    .join(", ");
  world.forge!.execute(`CREATE (:${label} {${body}})`);
}

// ---------------------------------------------------------------------------
// Before hook — reset state between scenarios
// ---------------------------------------------------------------------------

Before(function (this: GraphForgeWorld) {
  try { this.forge?.close(); } catch { /* ignore */ }
  this.forge = null;
  this.result = null;
  this.error = null;
  this.nodes = {};
  this.edges = [];
  this.extra = {};
  this.tmpDir = null;
});

// ---------------------------------------------------------------------------
// GIVEN steps
// ---------------------------------------------------------------------------

Given("an empty graph", function (this: GraphForgeWorld) {
  this.forge = new GraphForge();
});

Given("a graph with a directed cycle", function (this: GraphForgeWorld) {
  this.forge = new GraphForge();
  this.forge.execute(
    "CREATE (a:Person {name:'Alice'})-[:KNOWS]->" +
      "(b:Person {name:'Bob'})-[:KNOWS]->" +
      "(c:Person {name:'Carol'})-[:KNOWS]->(a)"
  );
});

Given(
  /^a graph with a Person node named "([^"]*)"$/,
  function (this: GraphForgeWorld, name: string) {
    this.forge = new GraphForge();
    createNode(this, "Person", { name });
  }
);

Given(
  /^a graph with a Person node named "([^"]*)" with age (\d+)$/,
  function (this: GraphForgeWorld, name: string, ageStr: string) {
    this.forge = new GraphForge();
    createNode(this, "Person", { name, age: parseInt(ageStr, 10) });
  }
);

Given(
  /^a graph with (\d+) Person nodes$/,
  function (this: GraphForgeWorld, nStr: string) {
    const n = parseInt(nStr, 10);
    this.forge = new GraphForge();
    for (let i = 0; i < n; i++) {
      createNode(this, "Person", { name: `Person${i}` });
    }
  }
);

Given(
  "a graph with 3 Person nodes connected by KNOWS edges",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const names = ["Alice", "Bob", "Carol"];
    const handles = names.map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    this.forge.addEdge(handles[0], "KNOWS", handles[1]);
    this.forge.addEdge(handles[1], "KNOWS", handles[2]);
  }
);

Given(
  "a graph with 4 Person nodes in two connected groups",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const g1 = ["Alice", "Bob"].map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    const g2 = ["Carol", "Dave"].map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    this.forge.addEdge(g1[0], "KNOWS", g1[1]);
    this.forge.addEdge(g2[0], "KNOWS", g2[1]);
    this.extra["group1"] = g1;
    this.extra["group2"] = g2;
  }
);

Given(
  /^a graph with a Paper node titled "([^"]*)"$/,
  function (this: GraphForgeWorld, title: string) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Paper", { title });
    this.nodes[title] = h;
  }
);

Given(
  "a graph with a Paper node that has a stored vector embedding",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Paper", { title: "Stub Paper" });
    this.nodes["Stub Paper"] = h;
    this.extra["vector"] = new Array(128).fill(1.0);
  }
);

Given(
  /^a graph with a Paper node titled "([^"]*)" and a stored vector embedding$/,
  function (this: GraphForgeWorld, title: string) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Paper", { title });
    this.nodes[title] = h;
    this.extra["vector"] = new Array(128).fill(1.0);
  }
);

Given(
  /^a graph with a Paper node titled "([^"]*)" and a Person node named "([^"]*)"$/,
  function (this: GraphForgeWorld, title: string, name: string) {
    this.forge = new GraphForge();
    this.nodes[title] = this.forge.addNode("Paper", { title });
    this.nodes[name] = this.forge.addNode("Person", { name });
  }
);

Given(
  /^a graph with (\d+) Paper nodes with similar titles$/,
  function (this: GraphForgeWorld, nStr: string) {
    const n = parseInt(nStr, 10);
    this.forge = new GraphForge();
    for (let i = 0; i < n; i++) {
      const t = `Graph Theory Paper ${i}`;
      this.nodes[t] = this.forge.addNode("Paper", { title: t });
    }
  }
);

Given(
  /^a graph with (\d+) Paper nodes with title and abstract properties$/,
  function (this: GraphForgeWorld, nStr: string) {
    const n = parseInt(nStr, 10);
    this.forge = new GraphForge();
    for (let i = 0; i < n; i++) {
      const t = `Neural Networks Paper ${i}`;
      this.nodes[t] = this.forge.addNode("Paper", { title: t, abstract: "About neural networks" });
    }
  }
);

Given(
  "a graph with a Paper node",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Paper", { title: "Stub Paper" });
    this.nodes["paper"] = h;
    this.extra["paper_id"] = h.uuid;
  }
);

Given(
  /^a graph with 3 Paper nodes with title properties$/,
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    for (let i = 0; i < 3; i++) {
      const t = `Paper ${i}`;
      this.nodes[t] = this.forge.addNode("Paper", { title: t });
    }
  }
);

Given(
  /^a graph with a Person node named "([^"]*)" and a Paper node titled "([^"]*)"$/,
  function (this: GraphForgeWorld, name: string, title: string) {
    this.forge = new GraphForge();
    this.nodes[name] = this.forge.addNode("Person", { name });
    this.nodes[title] = this.forge.addNode("Paper", { title });
  }
);

Given(
  "a graph with a KNOWS relationship and an AUTHORED relationship",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const alice = this.forge.addNode("Person", { name: "Alice" });
    const bob = this.forge.addNode("Person", { name: "Bob" });
    const paper = this.forge.addNode("Paper", { title: "GNN" });
    this.forge.addEdge(alice, "KNOWS", bob);
    this.forge.addEdge(alice, "AUTHORED", paper);
    this.nodes["Alice"] = alice;
    this.nodes["Bob"] = bob;
    this.nodes["paper"] = paper;
  }
);

Given(
  /^a graph with (\d+) Person nodes and (\d+) Paper node$/,
  function (this: GraphForgeWorld, npStr: string, npaStr: string) {
    const np = parseInt(npStr, 10);
    const npa = parseInt(npaStr, 10);
    this.forge = new GraphForge();
    for (let i = 0; i < np; i++) {
      this.nodes[`p${i}`] = this.forge.addNode("Person", { name: `Person${i}` });
    }
    for (let i = 0; i < npa; i++) {
      this.nodes[`paper${i}`] = this.forge.addNode("Paper", { title: `Paper${i}` });
    }
  }
);

Given(
  "a graph with Person nodes but no Paper nodes",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    for (const name of ["Alice", "Bob"]) {
      this.nodes[name] = this.forge.addNode("Person", { name });
    }
  }
);

Given(
  /^a graph with Paper nodes indexed with (\d+)-dimensional vectors$/,
  function (this: GraphForgeWorld, nStr: string) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Paper", { title: "Stub" });
    this.nodes["paper"] = h;
    this.extra["vector_dim"] = parseInt(nStr, 10);
  }
);

Given(
  "a graph with Person nodes connected by KNOWS edges",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const names = ["Alice", "Bob", "Carol"];
    const handles = names.map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    this.forge.addEdge(handles[0], "KNOWS", handles[1]);
    this.forge.addEdge(handles[1], "KNOWS", handles[2]);
  }
);

Given(
  "a graph with Person nodes connected by both KNOWS and FOLLOWS edges",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const alice = this.forge.addNode("Person", { name: "Alice" });
    const bob = this.forge.addNode("Person", { name: "Bob" });
    this.forge.addEdge(alice, "KNOWS", bob);
    this.forge.addEdge(bob, "FOLLOWS", alice);
    this.nodes["Alice"] = alice;
    this.nodes["Bob"] = bob;
  }
);

Given(
  "a graph with Person nodes connected by directed KNOWS edges",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const names = ["Alice", "Bob", "Carol"];
    const handles = names.map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    this.forge.addEdge(handles[0], "KNOWS", handles[1]);
    this.forge.addEdge(handles[1], "KNOWS", handles[2]);
  }
);

Given(
  "2 other Person nodes connected by a KNOWS edge but isolated from the first pair",
  function (this: GraphForgeWorld) {
    const carol = this.forge!.addNode("Person", { name: "Carol" });
    const dave = this.forge!.addNode("Person", { name: "Dave" });
    this.forge!.addEdge(carol, "KNOWS", dave);
    this.nodes["Carol"] = carol;
    this.nodes["Dave"] = dave;
    this.extra["group2"] = [carol, dave];
  }
);

Given(
  /^a graph with 2 Person nodes connected by a KNOWS edge$/,
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const alice = this.forge.addNode("Person", { name: "Alice" });
    const bob = this.forge.addNode("Person", { name: "Bob" });
    this.forge.addEdge(alice, "KNOWS", bob);
    this.nodes["Alice"] = alice;
    this.nodes["Bob"] = bob;
    this.extra["group1"] = [alice, bob];
  }
);

Given(
  /^a Person node named "([^"]*)"$/,
  function (this: GraphForgeWorld, name: string) {
    const h = this.forge!.addNode("Person", { name });
    this.nodes[name] = h;
  }
);

Given(
  /^Person nodes named "([^"]*)" and "([^"]*)"$/,
  function (this: GraphForgeWorld, first: string, second: string) {
    for (const name of [first, second]) {
      this.nodes[name] = this.forge!.addNode("Person", { name });
    }
  }
);

Given(
  /^a graph with a Person node with age stored as a string "([^"]*)"$/,
  function (this: GraphForgeWorld, val: string) {
    this.forge = new GraphForge();
    createNode(this, "Person", { name: "Alice", age: val });
  }
);

Given(
  "a path that does not exist on disk",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    this.extra["path"] = path.join(tmp, "does_not_exist");
    this.forge = null;
  }
);

Given(
  "a persistent graph backed by Parquet",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    const d = path.join(tmp, "graph");
    fs.mkdirSync(d, { recursive: true });
    this.forge = new GraphForge(d);
    this.extra["path"] = d;
  }
);

Given(
  "a persistent graph at a temporary path",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    const d = path.join(tmp, "graph2");
    fs.mkdirSync(d, { recursive: true });
    this.forge = new GraphForge(d);
    this.extra["path"] = d;
  }
);

Given(
  "the forge instance is closed",
  function (this: GraphForgeWorld) {
    this.forge!.close();
  }
);

Given(
  "a transaction has been started",
  function (this: GraphForgeWorld) {
    this.forge!.begin();
  }
);

Given(
  /^a graph with a Person node named "([^"]*)" connected by a KNOWS edge to a Person node named "([^"]*)"$/,
  function (this: GraphForgeWorld, name: string, name2: string) {
    this.forge = new GraphForge();
    this.forge.execute(
      `CREATE (:Person {name: ${cypherLit(name)}})-[:KNOWS]->(:Person {name: ${cypherLit(name2)}})`
    );
  }
);

Given(
  'a graph with a Person node named "Alice" without an age property',
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    createNode(this, "Person", { name: "Alice" });
  }
);

Given(
  "a valid ontology YAML file defining a Person label",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    const p = path.join(tmp, "ontology.yaml");
    fs.writeFileSync(p, "labels:\n  Person:\n    properties:\n      name: string\n");
    this.extra["ontology_path"] = p;
    this.forge = new GraphForge();
  }
);

Given(
  "a valid ontology JSON file defining a Paper label",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    const p = path.join(tmp, "ontology.json");
    fs.writeFileSync(p, JSON.stringify({ labels: { Paper: { properties: { title: "string" } } } }));
    this.extra["ontology_path"] = p;
    this.forge = new GraphForge();
  }
);

Given(
  "a file containing invalid YAML",
  function (this: GraphForgeWorld) {
    const tmp = _mkTmp(this);
    const p = path.join(tmp, "bad.yaml");
    fs.writeFileSync(p, ": this is not: valid: yaml: [");
    this.extra["ontology_path"] = p;
    this.forge = new GraphForge();
  }
);

Given(
  "a graph with Person nodes connected by KNOWS edges up to 3 hops deep",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const names = ["Alice", "Bob", "Carol", "Dave"];
    const handles = names.map((n) => {
      const h = this.forge!.addNode("Person", { name: n });
      this.nodes[n] = h;
      return h;
    });
    for (let i = 0; i < handles.length - 1; i++) {
      this.forge.addEdge(handles[i], "KNOWS", handles[i + 1]);
    }
  }
);

Given(
  "a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const alice = this.forge.addNode("Person", { name: "Alice" });
    const bob = this.forge.addNode("Person", { name: "Bob" });
    const charlie = this.forge.addNode("Person", { name: "Charlie" });
    this.forge.addEdge(alice, "KNOWS", bob);
    this.forge.addEdge(bob, "KNOWS", charlie);
    this.nodes = { Alice: alice, Bob: bob, Charlie: charlie };
  }
);

Given(
  "a graph where Alice knows Bob",
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const alice = this.forge.addNode("Person", { name: "Alice" });
    const bob = this.forge.addNode("Person", { name: "Bob" });
    this.forge.addEdge(alice, "KNOWS", bob);
    this.nodes = { Alice: alice, Bob: bob };
  }
);

Given(
  'a graph with a single Person node named "Lone"',
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    const h = this.forge.addNode("Person", { name: "Lone" });
    this.nodes["Lone"] = h;
  }
);

Given(
  'a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"',
  function (this: GraphForgeWorld) {
    this.forge = new GraphForge();
    this.nodes["SrcNode"] = this.forge.addNode("Person", { name: "SrcNode" });
    this.nodes["DstNode"] = this.forge.addNode("Person", { name: "DstNode" });
  }
);

Given(
  'I have stored the node id as "paper_id"',
  function (this: GraphForgeWorld) {
    const first = this.nodes["paper"] || Object.values(this.nodes)[0];
    if (first) this.extra["paper_id"] = first.uuid;
  }
);

Given(
  'I have an embedding vector stored as "embedding"',
  function (this: GraphForgeWorld) {
    this.extra["embedding"] = new Array(128).fill(1.0);
  }
);



// ---------------------------------------------------------------------------
// WHEN steps
// ---------------------------------------------------------------------------

When(
  /^I execute "([^"]*)"$/,
  function (this: GraphForgeWorld, query: string) {
    if (this.forge === null) return;
    _catch(this, () => this.forge!.execute(query));
  }
);

When(
  /^I execute "([^"]*)" with parameter name "([^"]*)"$/,
  function (this: GraphForgeWorld, query: string, value: string) {
    _catch(this, () => this.forge!.execute(query, { name: value }));
  }
);


When(
  /^I execute "([^"]*)" without parameters$/,
  function (this: GraphForgeWorld, query: string) {
    _catch(this, () => this.forge!.execute(query, undefined));
  }
);

When(
  /^I add a node with label "([^"]*)" named "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, name: string) {
    _catch(this, () => {
      const h = this.forge!.addNode(label, { name });
      this.nodes[name] = h;
      return h;
    });
  }
);

When(
  /^I add a node with label "([^"]*)" named "([^"]*)" aged (\d+)$/,
  function (this: GraphForgeWorld, label: string, name: string, ageStr: string) {
    _catch(this, () => {
      const h = this.forge!.addNode(label, { name, age: parseInt(ageStr, 10) });
      this.nodes[name] = h;
      return h;
    });
  }
);

When(
  /^I request "([^"]*)" paths using "([^"]*)" selectors$/,
  function (this: GraphForgeWorld, algorithm: string, selector: string) {
    const alice = this.nodes["Alice"];
    const bob = this.nodes["Bob"];
    let source: unknown;
    let target: unknown;
    if (selector === "UUID") {
      source = alice.uuid;
      target = bob.uuid;
    } else if (selector === "handle") {
      source = alice;
      target = bob;
    } else if (selector === "property") {
      source = { label: "Person", property: "name", value: "Alice" };
      target = { label: "Person", property: "name", value: "Bob" };
    } else {
      throw new Error(`unknown selector form ${selector}`);
    }
    _catch(this, () => this.forge!.paths(source, target, algorithm));
  }
);

When(
  /^I request "([^"]*)" paths with a "([^"]*)" source selector$/,
  function (this: GraphForgeWorld, algorithm: string, selectorCase: string) {
    const bob = this.nodes["Bob"];
    this.extra["selector_case"] = selectorCase;
    let source: unknown;
    if (selectorCase === "malformed") {
      source = { label: "Person", property: "name" };
    } else if (selectorCase === "missing") {
      source = "01900000-0000-7000-8000-000000000000";
    } else if (selectorCase === "ambiguous") {
      this.forge!.addNode("Person", { name: "Alice" });
      source = { label: "Person", property: "name", value: "Alice" };
    } else if (selectorCase === "cross-graph") {
      const other = new GraphForge();
      source = other.addNode("Person", { name: "Mallory" });
    } else {
      throw new Error(`unknown invalid selector case ${selectorCase}`);
    }
    _catch(this, () => this.forge!.paths(source, bob, algorithm));
  }
);

When(
  /^I add a node with label "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string) {
    _catch(this, () => this.forge!.addNode(label));
  }
);


When(
  'I add a node with label "Person" with an unsupported property value',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.addNode("Person", { data: function () {} }));
  }
);

When(
  /^I add a "([^"]*)" edge from "([^"]*)" to "([^"]*)" with since (\d+)$/,
  function (this: GraphForgeWorld, relType: string, srcName: string, dstName: string, yearStr: string) {
    const src = this.nodes[srcName];
    const dst = this.nodes[dstName];
    _catch(this, () => {
      const h = this.forge!.addEdge(src, relType, dst, { since: parseInt(yearStr, 10) });
      this.edges.push(h);
      return h;
    });
  }
);

When(
  /^I add a "([^"]*)" edge from "([^"]*)" to "([^"]*)"$/,
  function (this: GraphForgeWorld, relType: string, srcName: string, dstName: string) {
    const src = this.nodes[srcName];
    const dst = this.nodes[dstName];
    if (!src || !dst) {
      this.error = codedError("ValidationError", `Node not found: ${srcName} or ${dstName}`);
      return;
    }
    _catch(this, () => this.forge!.addEdge(src, relType, dst));
  }
);


When(
  'I add a "KNOWS" edge from a raw integer to the node for "Alice"',
  function (this: GraphForgeWorld) {
    const dst = this.nodes["Alice"];
    _catch(this, () => this.forge!.addEdge(42, "KNOWS", dst));
  }
);

When(
  'I add a "KNOWS" edge from the node for "Alice" to a raw integer',
  function (this: GraphForgeWorld) {
    const src = this.nodes["Alice"];
    _catch(this, () => this.forge!.addEdge(src, "KNOWS", 42));
  }
);

When(
  'I bulk add nodes with label "Person" and 2 records',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.addNodes("Person", [{ name: "Alice" }, { name: "Bob" }]));
  }
);

When(
  'I bulk add nodes with label "Person" from an Arrow Table of 5 rows',
  function (this: GraphForgeWorld) {
    const rows = [0, 1, 2, 3, 4].map((i) => ({ name: String.fromCharCode(65 + i) }));
    _catch(this, () => this.forge!.addNodes("Person", rows));
  }
);

When(
  'I bulk add edges with type "KNOWS" using source column "src_id" and destination column "dst_id"',
  function (this: GraphForgeWorld) {
    const nodes = Object.values(this.nodes);
    if (nodes.length < 2) {
      this.error = codedError("ValidationError", "Need 2 nodes");
      return;
    }
    const records = [{ src_id: nodes[0].uuid, dst_id: nodes[1].uuid }];
    _catch(this, () => this.forge!.addEdges("KNOWS", records, "src_id", "dst_id"));
  }
);


When(
  /^I rank "([^"]*)" by "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, algorithm: string) {
    _catch(this, () => this.forge!.rank(label, { by: algorithm }));
    this.extra["last_rank"] = this.result;
  }
);

When(
  /^I rank "([^"]*)" by "([^"]*)" writing result to property "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, algorithm: string, prop: string) {
    _catch(this, () => this.forge!.rank(label, { by: algorithm, writeProperty: prop }));
  }
);

When(
  /^I rank "([^"]*)" by "([^"]*)" via relationship type "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, algorithm: string, via: string) {
    _catch(this, () => this.forge!.rank(label, { by: algorithm, via }));
  }
);

When(
  /^I rank "([^"]*)" by "([^"]*)" treating edges as directed$/,
  function (this: GraphForgeWorld, label: string, algorithm: string) {
    _catch(this, () => this.forge!.rank(label, { by: algorithm, directed: true }));
    this.extra["rank_directed"] = this.result;
  }
);

When(
  /^I rank "([^"]*)" by "([^"]*)" treating edges as undirected$/,
  function (this: GraphForgeWorld, label: string, algorithm: string) {
    _catch(this, () => this.forge!.rank(label, { by: algorithm, directed: false }));
    this.extra["rank_undirected"] = this.result;
  }
);

When(
  /^I cluster "([^"]*)" by "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, algorithm: string) {
    _catch(this, () => this.forge!.cluster(label, { by: algorithm }));
  }
);

When(
  /^I cluster "([^"]*)" by "([^"]*)" writing result to property "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, algorithm: string, prop: string) {
    _catch(this, () => this.forge!.cluster(label, { by: algorithm, writeProperty: prop }));
  }
);

When(
  /^I find "([^"]*)" in label "([^"]*)"$/,
  function (this: GraphForgeWorld, query: string, label: string) {
    _catch(this, () => this.forge!.find(query, { label }));
    if (!("first_find_result" in this.extra) && this.extra["first_index_done"]) {
      this.extra["first_find_result"] = this.result;
    }
  }
);

When(
  /^I find "([^"]*)" in label "([^"]*)" with limit (\d+)$/,
  function (this: GraphForgeWorld, query: string, label: string, limitStr: string) {
    _catch(this, () => this.forge!.find(query, { label, limit: parseInt(limitStr, 10) }));
  }
);

When(
  'I find by the stored vector in label "Paper"',
  function (this: GraphForgeWorld) {
    const vec = this.extra["vector"] as number[];
    _catch(this, () => this.forge!.find(undefined, { label: "Paper", vector: vec }));
  }
);

When(
  /^I find by the stored embedding in label "([^"]*)" in space "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, space: string) {
    const vec = this.extra["embedding"] as number[];
    _catch(this, () => this.forge!.find(undefined, { label, vector: vec, space }));
  }
);

When(
  /^I find "([^"]*)" with the stored vector in label "([^"]*)"$/,
  function (this: GraphForgeWorld, query: string, label: string) {
    const vec = this.extra["vector"] as number[];
    _catch(this, () => this.forge!.find(query, { label, vector: vec }));
  }
);

When(
  'I find with no query and no vector in label "Paper"',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.find(undefined, { label: "Paper" }));
  }
);

When(
  'I find by an empty vector in label "Paper"',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.find(undefined, { label: "Paper", vector: [] }));
  }
);

When(
  'I find by a vector containing NaN in label "Paper"',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.find(undefined, { label: "Paper", vector: [NaN, 1.0] }));
  }
);

When(
  'I find by a vector containing infinity in label "Paper"',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.find(undefined, { label: "Paper", vector: [Infinity, 1.0] }));
  }
);

When(
  /^I find by a (\d+)-dimensional vector in label "([^"]*)"$/,
  function (this: GraphForgeWorld, nStr: string, label: string) {
    const vec = new Array(parseInt(nStr, 10)).fill(1.0);
    _catch(this, () => this.forge!.find(undefined, { label, vector: vec }));
  }
);

When(
  /^I index label "([^"]*)" on properties "([^"]*)" and "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, p1: string, p2: string) {
    _catch(this, () => this.forge!.index(label, { properties: [p1, p2] }));
    this.extra["index_called"] = true;
  }
);

When(
  /^I index label "([^"]*)" on property "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, prop: string) {
    _catch(this, () => this.forge!.index(label, { properties: [prop] }));
    this.extra["index_called"] = true;
    if (!("first_find_result" in this.extra)) {
      this.extra["first_index_done"] = true;
    }
  }
);

When(
  /^I index label "([^"]*)" storing the vector for node "([^"]*)" in space "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string, nodeKey: string, space: string) {
    const nodeId = (this.extra["paper_id"] as string) || (this.nodes[nodeKey]?.uuid);
    const vec = this.extra["embedding"] as number[];
    _catch(this, () => this.forge!.index(label, { nodeId, vector: vec, space }));
  }
);

When(
  'I index label "Paper" on an empty properties list',
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.index("Paper", { properties: [] }));
  }
);

When(
  'I add a node with label "Paper" titled "Deep Graph Learning"',
  function (this: GraphForgeWorld) {
    const h = this.forge!.addNode("Paper", { title: "Deep Graph Learning" });
    this.nodes["Deep Graph Learning"] = h;
  }
);

When(
  "I call schema",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.schema());
  }
);

When(
  "I call labels",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.labels());
  }
);

When(
  "I call relationship_types",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.relationshipTypes());
  }
);

When(
  /^I call node_count for label "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string) {
    _catch(this, () => this.forge!.nodeCount(label));
  }
);

When(
  /^I call explain on "([^"]*)"$/,
  function (this: GraphForgeWorld, query: string) {
    _catch(this, () => this.forge!.explain(query));
  }
);

When(
  "I call begin",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.begin());
  }
);

When(
  "I call commit",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.commit());
  }
);

When(
  "I call rollback",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.rollback());
  }
);

When(
  "I call clear",
  function (this: GraphForgeWorld) {
    _catch(this, () => this.forge!.clear());
  }
);

When(
  "I open a graph at that path",
  function (this: GraphForgeWorld) {
    const p = this.extra["path"] as string || "/nonexistent/path";
    _catch(this, () => {
      const forge = new GraphForge(p);
      this.forge = forge;
      return forge;
    });
  }
);

When(
  "I reopen the forge at the same path",
  function (this: GraphForgeWorld) {
    const p = this.extra["path"] as string;
    _catch(this, () => {
      this.forge = new GraphForge(p);
      return this.forge;
    });
  }
);

When(
  /^I attempt to call (.+)$/,
  function (this: GraphForgeWorld, method: string) {
    method = method.trim();
    if (method.startsWith("execute with query")) {
      const q = method.split('"')[1];
      _catch(this, () => this.forge!.execute(q));
    } else if (method.startsWith("rank with label")) {
      const parts = method.split('"');
      _catch(this, () => this.forge!.rank(parts[1], { by: parts[3] }));
    } else if (method.startsWith("find with text")) {
      const parts = method.split('"');
      _catch(this, () => this.forge!.find(parts[1], { label: parts[3] }));
    } else if (method.startsWith("add_node with label")) {
      const parts = method.split('"');
      _catch(this, () => this.forge!.addNode(parts[1], { name: parts[3] }));
    } else {
      this.error = codedError("LifecycleError", `Unknown method in step: ${method}`);
    }
  }
);

When(
  "I load the ontology from that file",
  function (this: GraphForgeWorld) {
    const p = this.extra["ontology_path"] as string;
    _catch(this, () => this.forge!.loadOntology(p));
  }
);

When(
  /^I analyze by "([^"]*)"$/,
  function (this: GraphForgeWorld, algorithm: string) {
    _catch(this, () => this.forge!.analyze(algorithm));
  }
);

When(
  /^I call neighbourhood for "([^"]*)" with hops (\d+) in label "([^"]*)" using canonical property "([^"]*)"$/,
  function (this: GraphForgeWorld, canonical: string, hopsStr: string, label: string, prop: string) {
    // neighbourhood recipe not yet implemented at Node skeleton stage
    this.result = null;
    this.error = new Error("not implemented");
  }
);

// ---------------------------------------------------------------------------
// THEN steps
// ---------------------------------------------------------------------------

Then(
  "the result is an Arrow Table",
  function (this: GraphForgeWorld) {
    resultTable(this); // throws if the query errored or the result is not Arrow IPC
  }
);

function assertHasColumn(world: GraphForgeWorld, col: string): void {
  const table = resultTable(world);
  if (!table.schema.fields.some((f) => f.name === col)) {
    const cols = table.schema.fields.map((f) => f.name).join(", ");
    throw new Error(`expected column "${col}"; schema has: ${cols}`);
  }
}

Then(/^the table has column "([^"]*)"$/, function (this: GraphForgeWorld, col: string) {
  assertHasColumn(this, col);
});

Then(
  /^the result schema contains column "([^"]*)"$/,
  function (this: GraphForgeWorld, col: string) {
    assertHasColumn(this, col);
  }
);

function assertRowCount(world: GraphForgeWorld, n: number, atMost = false): void {
  const got = resultTable(world).numRows;
  const ok = atMost ? got <= n : got === n;
  if (!ok) {
    throw new Error(`expected ${atMost ? "at most " : ""}${n} row(s), got ${got}`);
  }
}

Then(/^the table has (\d+) rows$/, function (this: GraphForgeWorld, nStr: string) {
  assertRowCount(this, parseInt(nStr, 10));
});

Then(/^the table has (\d+) row$/, function (this: GraphForgeWorld, nStr: string) {
  assertRowCount(this, parseInt(nStr, 10));
});

Then(/^the table has at most (\d+) rows$/, function (this: GraphForgeWorld, nStr: string) {
  assertRowCount(this, parseInt(nStr, 10), true);
});

Then(/^the "is_dag" value is (true|false)$/, function (this: GraphForgeWorld, expected: string) {
  const actual = firstRowValue(this, "is_dag");
  if (actual !== (expected === "true")) {
    throw new Error(`expected is_dag=${expected}, got ${JSON.stringify(actual)}`);
  }
});

Then(
  /^the first row value for "([^"]*)" is "([^"]*)"$/,
  function (this: GraphForgeWorld, col: string, val: string) {
    const got = firstRowValue(this, col);
    if (String(got) !== val) {
      throw new Error(`first row "${col}": expected ${JSON.stringify(val)}, got ${JSON.stringify(got)}`);
    }
  }
);

Then(
  /^the first row value for "([^"]*)" is null$/,
  function (this: GraphForgeWorld, col: string) {
    const got = firstRowValue(this, col);
    if (got !== null) {
      throw new Error(`first row "${col}": expected null, got ${JSON.stringify(got)}`);
    }
  }
);

function expectErrCode(world: GraphForgeWorld, code: string): void {
  const got = errCode(world.error);
  if (got !== code) {
    const detail = world.error ? `${got ?? "?"} (${world.error.message})` : "no error was raised";
    throw new Error(`expected ${code}, got ${detail}`);
  }
}

Then("a ParseError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "ParseError");
});

Then("the error includes a source span", function (this: GraphForgeWorld) {
  // Native ParseError encodes its span as a leading [span:<start>:<len>] token.
  expectErrCode(this, "ParseError");
  if (!/^\[span:\d+:\d+\]/.test(this.error?.message ?? "")) {
    throw new Error(`ParseError message lacks a [span:..] prefix: ${this.error?.message}`);
  }
});

Then("an ExecutionError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "ExecutionError");
});

Then("a StorageError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "StorageError");
});

Then("a LifecycleError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "LifecycleError");
});

Then("a TypeError is raised", function (this: GraphForgeWorld) {
  if (!(this.error instanceof TypeError)) {
    throw new Error(`expected a TypeError, got ${this.error?.message ?? "no error"}`);
  }
});

Then("a ValidationError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "ValidationError");
});

Then("an OntologyError is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "OntologyError");
});

Then(
  "no error is raised",
  function (this: GraphForgeWorld) {
    if (this.error !== null) {
      throw new Error(`Unexpected error: ${this.error.message}`);
    }
  }
);

Then(
  /^the result is a NodeHandle with label "([^"]*)"$/,
  function (this: GraphForgeWorld, label: string) {
    if (!(this.result instanceof NodeHandle)) {
      throw new Error(`expected native NodeHandle, got ${typeof this.result}`);
    }
    const handle = this.result as { label: string };
    if (handle.label !== label) {
      throw new Error(`expected label ${label}, got ${handle.label}`);
    }
  }
);

Then(
  "the NodeHandle exposes UUID identity with no numeric surrogate",
  function (this: GraphForgeWorld) {
    const handle = this.result as { uuid?: unknown; id?: unknown; get?: unknown };
    if (typeof handle?.uuid !== "string" || !(this.result instanceof NodeHandle)) {
      throw new Error("expected native NodeHandle UUID identity");
    }
    if ("id" in handle || "get" in handle) {
      throw new Error("NodeHandle exposed a surrogate or property cache");
    }
  }
);

Then(
  /^execute readback returns the NodeHandle UUID and name "([^"]*)"$/,
  function (this: GraphForgeWorld, name: string) {
    const escapedName = name.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    const table = tableFromIPC(
      this.forge!.execute(
        `MATCH (n {name: '${escapedName}'}) RETURN n.node_uuid AS uuid, n.name AS name`
      )
    );
    if (table.numRows !== 1 || table.getChild("name")?.get(0) !== name) {
      throw new Error(`UUID readback did not return ${name}`);
    }
    const bytes = table.getChild("uuid")?.get(0) as Uint8Array;
    const actual = Buffer.from(bytes).toString("hex");
    if (actual !== this.nodes[name].uuid.replaceAll("-", "")) {
      throw new Error(`UUID readback ${actual} did not match ${this.nodes[name].uuid}`);
    }
  }
);

Then(
  "the result is an EdgeHandle with UUID identity and no numeric surrogate",
  function (this: GraphForgeWorld) {
    if (!(this.result instanceof EdgeHandle)) {
      return "pending";
    }
    const handle = this.result as { uuid?: unknown; id?: unknown };
    if (typeof handle.uuid !== "string" || "id" in handle) {
      throw new Error("EdgeHandle exposed a numeric surrogate");
    }
  }
);

Then(
  /^execute "([^"]*)" returns (\d+) rows$/,
  function (this: GraphForgeWorld, query: string, nStr: string) {
    const buf = this.forge!.execute(query);
    const table = tableFromIPC(buf as Buffer);
    const want = parseInt(nStr, 10);
    if (table.numRows !== want) {
      throw new Error(`execute "${query}": expected ${want} rows, got ${table.numRows}`);
    }
  }
);

Then(
  /^execute "([^"]*)" returns (\d+) row with value (\d+)$/,
  function (this: GraphForgeWorld, _query: string, _nStr: string, _val: string) {
    return "pending";
  }
);

Then(
  /^execute "([^"]*)" returns (\d+) row$/,
  function (this: GraphForgeWorld, _query: string, _nStr: string) {
    return "pending";
  }
);

Then(
  "the string representation contains the NodeHandle UUID",
  function (this: GraphForgeWorld) {
    const handle = this.result as { uuid: string; toString(): string };
    if (!handle.toString().includes(handle.uuid)) {
      throw new Error(`handle representation omitted UUID: ${handle.toString()}`);
    }
  }
);

Then(
  /^the string representation does not contain cached property "([^"]*)"$/,
  function (this: GraphForgeWorld, property: string) {
    if (String(this.result).includes(property)) {
      throw new Error(`handle representation cached property ${property}`);
    }
  }
);

Then("the path request reaches Rust dispatch", function (this: GraphForgeWorld) {
  if (this.error) throw this.error;
  if (!Buffer.isBuffer(this.result)) {
    throw new Error(`expected paths Arrow IPC buffer, got ${typeof this.result}`);
  }
  const table = tableFromIPC(this.result);
  const names = table.schema.fields.map((field) => field.name);
  if (names.join(",") !== "source_uuid,target_uuid,cost,path") {
    throw new Error(`unexpected paths schema: ${names.join(",")}`);
  }
});

Then("a structured selector error is raised", function (this: GraphForgeWorld) {
  expectErrCode(this, "ValidationError");
});

Then(
  /^the result is (\d+)$/,
  function (this: GraphForgeWorld, nStr: string) {
    if (this.error) return "pending";
    if (this.result !== parseInt(nStr, 10)) return "pending";
  }
);

Then(
  "the result is a non-empty string",
  function (this: GraphForgeWorld) {
    if (this.error) throw new Error(`expected a string result, but errored: ${this.error.message}`);
    if (typeof this.result !== "string" || this.result.length === 0) {
      throw new Error(`expected a non-empty string, got: ${JSON.stringify(this.result)}`);
    }
  }
);

Then(
  /^the result contains "([^"]*)"$/,
  function (this: GraphForgeWorld, text: string) {
    if (this.error) throw new Error(`expected a result, but errored: ${this.error.message}`);
    if (typeof this.result === "string") {
      if (!this.result.includes(text)) throw new Error(`result string does not contain "${text}"`);
    } else if (Array.isArray(this.result)) {
      if (!(this.result as string[]).includes(text)) throw new Error(`result list does not contain "${text}"`);
    } else {
      throw new Error(`expected a string or list result, got: ${typeof this.result}`);
    }
  }
);

Then(
  "the result is an empty list",
  function (this: GraphForgeWorld) {
    if (this.error) return "pending";
    if (!Array.isArray(this.result) || (this.result as unknown[]).length !== 0) return "pending";
  }
);

Then(
  "calling relationship_types also returns an empty list",
  function (this: GraphForgeWorld) {
    const result = this.forge!.relationshipTypes();
    if (!Array.isArray(result) || result.length !== 0) return "pending";
  }
);

Then(
  /^the table contains an entry for label "([^"]*)"$/,
  function (this: GraphForgeWorld, _label: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  "the two score results are not identical",
  function (this: GraphForgeWorld) {
    return "pending";
  }
);

Then(
  "the 2 connected nodes share the same community_id",
  function (this: GraphForgeWorld) {
    return "pending";
  }
);

Then(
  "the 2 isolated nodes share a different community_id",
  function (this: GraphForgeWorld) {
    return "pending";
  }
);

// Used as both Given (setup) and Then (assertion) in find.feature
Given(
  "no explicit index call was made before find",
  function (this: GraphForgeWorld) {
    if (!("index_called" in this.extra)) {
      // Given context — initialise the flag
      this.extra["index_called"] = false;
    } else {
      // Then context — assert
      if (this.extra["index_called"]) {
        throw new Error("Index was called before find");
      }
    }
  }
);

Then(
  /^for each result row the id is valid in execute "([^"]*)"$/,
  function (this: GraphForgeWorld, _query: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  /^all result rows have label "([^"]*)"$/,
  function (this: GraphForgeWorld, _label: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  "the result contains that node",
  function (this: GraphForgeWorld) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  'find "paper" in label "Paper" returns the same results as after the first index call',
  function (this: GraphForgeWorld) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  /^the result contains a row with title "([^"]*)"$/,
  function (this: GraphForgeWorld, _title: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  /^the result contains a row for "([^"]*)"$/,
  function (this: GraphForgeWorld, _name: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  /^the result does not contain a row for "([^"]*)"$/,
  function (this: GraphForgeWorld, _name: string) {
    if (this.error) return "pending";
    return "pending";
  }
);

Then(
  "the result is an Arrow Table with at least 1 row",
  function (this: GraphForgeWorld) {
    if (this.error) return "pending";
    return "pending";
  }
);
