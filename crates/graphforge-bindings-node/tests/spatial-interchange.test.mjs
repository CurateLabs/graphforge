// Real Arrow IPC acceptance for the Rust-owned GeoArrow contract (#801).

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../tests/contracts/geoarrow-interchange-v1.json",
      import.meta.url,
    ),
    "utf8",
  ),
);

const spatialValue = (entry) => ({
  spatial_type: { geometry: entry.geometry, crs: entry.crs },
  coordinates: entry.coordinates,
});

test("GeoArrow metadata values nulls and errors remain Rust-owned", () => {
  const forge = new GraphForge();
  forge.addNode(
    "Geometry",
    Object.fromEntries(
      fixture.cases.map((entry) => [entry.name, spatialValue(entry)]),
    ),
  );
  forge.addNode("Geometry", {});
  const projection = fixture.cases
    .map((entry) => `n.${entry.name} AS ${entry.name}`)
    .join(", ");
  const table = tableFromIPC(
    forge.execute(`MATCH (n:Geometry) RETURN ${projection}`),
  );
  assert.equal(table.numRows, 2);
  for (const entry of fixture.cases) {
    const field = table.schema.fields.find(({ name }) => name === entry.name);
    assert.equal(
      field.metadata.get("ARROW:extension:name"),
      entry.extensionName,
    );
    assert.equal(
      field.metadata.get("ARROW:extension:metadata"),
      entry.extensionMetadata,
    );
    assert.notEqual(table.getChild(entry.name).get(0), null);
    assert.equal(table.getChild(entry.name).get(1), null);
  }
  assert.throws(
    () =>
      forge.addNode("Geometry", {
        bad: {
          spatial_type: { geometry: "point", crs: "EPSG:9999" },
          coordinates: { Point: [1, 2] },
        },
      }),
    (error) =>
      error.code === "ValidationError" && !/coordinate/i.test(error.message),
  );
});
