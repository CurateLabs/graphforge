// Direct coverage for first-party JS helpers under lib/.

import assert from "node:assert/strict";
import { test } from "node:test";
import { pathHex, uuidHex } from "../lib/helpers.mjs";

test("uuidHex encodes binary UUID values as hex", () => {
  const bytes = Buffer.from("0123456789abcdef0123456789abcdef", "hex");
  assert.equal(uuidHex(bytes), "0123456789abcdef0123456789abcdef");
});

test("pathHex maps an Arrow path column row to hex UUID strings", () => {
  const a = Buffer.from("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "hex");
  const b = Buffer.from("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "hex");
  const table = {
    getChild(name) {
      assert.equal(name, "path");
      return {
        get(row) {
          assert.equal(row, 0);
          return [a, b];
        },
      };
    },
  };
  assert.deepEqual(pathHex(table, 0), [
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  ]);
});
