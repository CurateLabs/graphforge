#!/usr/bin/env node

import { runProcess } from "../lib/run.mjs";

process.exitCode = await runProcess(process.argv.slice(2));
