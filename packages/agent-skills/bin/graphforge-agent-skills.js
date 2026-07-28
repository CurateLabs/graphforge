#!/usr/bin/env node

import { readFileSync } from "node:fs";

const packageRoot = new URL("../", import.meta.url);
const packageMetadata = JSON.parse(readFileSync(new URL("package.json", packageRoot), "utf8"));
const compatibility = JSON.parse(readFileSync(new URL("compatibility.json", packageRoot), "utf8"));

const usage = `Usage: graphforge-agent-skills <command>

Commands:
  compatibility --json  Print the machine-readable GraphForge compatibility contract
  --version             Print the package version
  --help                Print this help

Workflow skills and runtime adapters land in later tracked slices.`;

const args = process.argv.slice(2);

if (args.length === 1 && args[0] === "--version") {
  process.stdout.write(`${packageMetadata.version}\n`);
} else if (args.length === 2 && args[0] === "compatibility" && args[1] === "--json") {
  process.stdout.write(`${JSON.stringify(compatibility)}\n`);
} else if (args.length === 0 || (args.length === 1 && args[0] === "--help")) {
  process.stdout.write(`${usage}\n`);
} else {
  process.stderr.write(`${usage}\n`);
  process.exitCode = 2;
}
