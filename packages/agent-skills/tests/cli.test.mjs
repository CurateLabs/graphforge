import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

const executable = fileURLToPath(
  new URL("../bin/graphforge-agent-skills.js", import.meta.url),
);
const packageMetadata = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const compatibility = JSON.parse(
  readFileSync(new URL("../compatibility.json", import.meta.url), "utf8"),
);

test("reports package version", () => {
  const output = execFileSync(process.execPath, [executable, "--version"], {
    encoding: "utf8",
  });
  assert.equal(output.trim(), packageMetadata.version);
});

test("reports the machine-readable GraphForge compatibility contract", () => {
  const output = execFileSync(
    process.execPath,
    [executable, "compatibility", "--json"],
    { encoding: "utf8" },
  );
  assert.deepEqual(JSON.parse(output), compatibility);
  assert.equal(
    compatibility.schema_version,
    packageMetadata.graphforgeCompatibility.schemaVersion,
  );
  assert.equal(compatibility.package, packageMetadata.name);
  assert.equal(compatibility.package_version, packageMetadata.version);
  assert.equal(compatibility.graphforge_release, packageMetadata.version);
  assert.equal(
    compatibility.graphforge_release,
    packageMetadata.graphforgeCompatibility.release,
  );
  assert.equal(
    compatibility.graphforge_version_range,
    packageMetadata.graphforgeCompatibility.range,
  );
});

test("prints help for empty and --help invocations", () => {
  for (const args of [[], ["--help"]]) {
    const result = spawnSync(process.execPath, [executable, ...args], {
      encoding: "utf8",
    });
    assert.equal(result.status, 0);
    assert.match(result.stdout, /Usage: graphforge-agent-skills/);
    assert.equal(result.stderr, "");
  }
});

test("fails closed for commands reserved for later slices", () => {
  const result = spawnSync(process.execPath, [executable, "bootstrap"], {
    encoding: "utf8",
  });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /later tracked slices/);
  assert.equal(result.stdout, "");
});
