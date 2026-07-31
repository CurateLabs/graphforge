#!/usr/bin/env node

import { run } from "../lib/run.mjs";

process.exitCode = await run(process.argv.slice(2));
