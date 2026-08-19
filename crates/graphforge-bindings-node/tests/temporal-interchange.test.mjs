// Real Arrow IPC acceptance for the Rust-owned temporal contract (#809).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../tests/contracts/temporal-interchange-v1.json",
      import.meta.url,
    ),
    "utf8",
  ),
);

test("temporal values remain typed Arrow with exact zone and calendar components", () => {
  const forge = new GraphForge();
  forge.addNode(
    "Temporal",
    Object.fromEntries(fixture.cases.map((entry) => [entry.name, entry.value])),
  );
  forge.addNode("Temporal", {});
  const projection = fixture.cases
    .map((entry) => `n.${entry.name} AS ${entry.name}`)
    .join(", ");
  const table = tableFromIPC(
    forge.execute(`MATCH (n:Temporal) RETURN ${projection}`),
  );
  assert.equal(table.numRows, 2);
  for (const entry of fixture.cases) {
    assert.notEqual(table.getChild(entry.name).get(0), null);
    assert.equal(table.getChild(entry.name).get(1), null);
  }
  assert.throws(() =>
    forge.addNode("Temporal", {
      bad: { type: "local_time", nanos: 86_400_000_000_000 },
    }),
  );
});
