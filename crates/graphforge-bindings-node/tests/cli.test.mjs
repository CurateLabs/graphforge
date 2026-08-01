import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { runCli } from "../index.js";

const fixtures = JSON.parse(
  readFileSync(
    new URL(
      "../../../tests/contracts/repository-cli-parity.json",
      import.meta.url,
    ),
    "utf8",
  ),
);

test("runCli matches the shared Python and Node fixtures exactly", () => {
  for (const fixture of fixtures.cases) {
    const result = runCli(fixture.args);
    assert.equal(result.exitCode, fixture.exitCode, fixture.name);
    assert.equal(result.stdout.toString("utf8"), fixture.stdout, fixture.name);
    assert.equal(result.stderr.toString("utf8"), fixture.stderr, fixture.name);
  }
});
