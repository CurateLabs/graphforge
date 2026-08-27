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
  ...(entry.preservedOnly
    ? {
        extension_name: entry.extensionName,
        extension_metadata: entry.extensionMetadata,
      }
    : {}),
});

const flattenCoordinates = (value) => {
  if (typeof value === "number") return [value];
  if (value && typeof value === "object" && "x" in value && "y" in value) {
    return [value.x, value.y];
  }
  if (value && typeof value[Symbol.iterator] === "function") {
    return [...value].flatMap(flattenCoordinates);
  }
  if (value && typeof value === "object") {
    return Object.values(value).flatMap(flattenCoordinates);
  }
  throw new TypeError(`unexpected Arrow coordinate value ${typeof value}`);
};

test("GeoArrow metadata values nulls and errors remain Rust-owned", () => {
  const forge = new GraphForge();
  forge.addNode("Geometry", {
    ...Object.fromEntries(
      fixture.cases.map((entry) => [entry.name, spatialValue(entry)]),
    ),
    fixture_ordinal: 0,
  });
  forge.addNode("Geometry", { fixture_ordinal: 1 });
  const projection = fixture.cases
    .map((entry) => `n.${entry.name} AS ${entry.name}`)
    .join(", ");
  const table = tableFromIPC(
    forge.execute(
      `MATCH (n:Geometry) RETURN ${projection} ORDER BY n.fixture_ordinal`,
    ),
  );
  assert.equal(table.numRows, 2);
  assert.deepEqual(
    table.batches.map(({ numRows }) => numRows),
    fixture.rows.batchSizes,
  );
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
    assert.deepEqual(
      flattenCoordinates(
        table.getChild(entry.name).get(fixture.rows.populated),
      ),
      entry.flat,
    );
    assert.equal(table.getChild(entry.name).get(fixture.rows.null), null);
  }
  assert.throws(
    () =>
      forge.addNode("Geometry", {
        bad: fixture.malformed.value,
      }),
    (error) =>
      error.code === fixture.malformed.code &&
      error.message === fixture.malformed.message,
  );
});
